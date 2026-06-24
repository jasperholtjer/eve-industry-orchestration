"""Availability-driven sensor for the market-history Silver tier.

EVE Ref publishes one file per calendar day with a variable, ~1-day lag, so a
fixed cron would structurally miss "yesterday" (ROADMAP §Decisions). This sensor
polls ``corpus everef missing-partitions`` instead, which diffs upstream
availability against the SQLite ``partitions`` table, and requests a Silver
materialisation for each newly available date.

Status is keyed on the corpus run-state, never on globbing the NAS tree: the
``missing`` set already excludes locally ingested dates, and Dagster ``run_key``
dedup prevents re-queuing a date that is already requested or in flight.

Gold has its own availability sensor (``market_history_gold_sensor``): ``deps=``
only expresses lineage, it does not trigger downstream materialisations, so Gold
is driven by polling ``corpus gold ready-dates`` rather than by the Silver run.
"""

import dagster as dg

from eve_industry_orchestration.defs import market_orders as mo
from eve_industry_orchestration.defs import sde
from eve_industry_orchestration.defs import system_jumps as sj
from eve_industry_orchestration.defs.corpus_resource import CorpusResource
from eve_industry_orchestration.defs.market_history import (
    DATASET,
    gold_partitions,
    market_history_gold,
    market_history_silver,
    silver_partitions,
)

# Concurrency is governed by deploy/dagster.yaml (global max_concurrent_runs:4 plus
# the `everef_download` / `gold_heavy` pools on the assets). Independently, cap how
# many partitions enter the queue per tick (oldest first) so a cold start does not
# enqueue the whole backlog at once; later ticks drain the remainder.
MAX_PARTITIONS_PER_TICK = 10


@dg.sensor(
    target=market_history_silver,
    minimum_interval_seconds=3600,
    default_status=dg.DefaultSensorStatus.STOPPED,
)
def market_history_availability_sensor(
    context: dg.SensorEvaluationContext, corpus: CorpusResource
) -> dg.SensorResult:
    """Requests Silver runs for market-history dates newly available upstream."""
    report = corpus.everef_missing_partitions(DATASET)
    missing = report.get("missing", [])

    valid = set(silver_partitions.get_partition_keys())
    eligible = sorted(date for date in missing if date in valid)
    selected = eligible[:MAX_PARTITIONS_PER_TICK]

    deferred = len(eligible) - len(selected)
    if deferred > 0:
        context.log.info(
            "availability: %d eligible, requesting %d this tick, %d deferred",
            len(eligible),
            len(selected),
            deferred,
        )

    # No download tag here: the `everef_download` pool on the asset
    # (market_history.py) throttles fetches across every launch path.
    run_requests = [
        dg.RunRequest(run_key=f"{DATASET}-silver-{date}", partition_key=date)
        for date in selected
    ]
    return dg.SensorResult(run_requests=run_requests)


@dg.sensor(
    target=market_history_gold,
    minimum_interval_seconds=3600,
    default_status=dg.DefaultSensorStatus.STOPPED,
)
def market_history_gold_sensor(
    context: dg.SensorEvaluationContext, corpus: CorpusResource
) -> dg.SensorResult:
    """Requests Gold runs for market-history dates whose Silver window is complete.

    ``deps=`` only expresses lineage, so Gold needs its own availability trigger.
    The readiness decision (target-day Silver present, rolling window at
    ``coverage_min_ratio``, Gold not yet built) lives in ``corpus gold
    ready-dates`` — never recomputed in Python — so this sensor stays a thin
    cap-and-dedup loop, mirroring the Silver sensor.
    """
    report = corpus.gold_ready_dates(DATASET)
    ready = report.get("ready", [])

    valid = set(gold_partitions.get_partition_keys())
    eligible = sorted(date for date in ready if date in valid)
    selected = eligible[:MAX_PARTITIONS_PER_TICK]

    deferred = len(eligible) - len(selected)
    if deferred > 0:
        context.log.info(
            "gold-readiness: %d eligible, requesting %d this tick, %d deferred",
            len(eligible),
            len(selected),
            deferred,
        )

    # No memory tag here: the `gold_heavy` pool on the asset (market_history.py)
    # throttles every launch path — sensor, backfill, manual — so this stays a
    # thin cap-and-dedup loop.
    run_requests = [
        dg.RunRequest(run_key=f"{DATASET}-gold-{date}", partition_key=date)
        for date in selected
    ]
    return dg.SensorResult(run_requests=run_requests)


@dg.sensor(
    target=sj.system_jumps_silver,
    minimum_interval_seconds=3600,
    default_status=dg.DefaultSensorStatus.STOPPED,
)
def system_jumps_availability_sensor(
    context: dg.SensorEvaluationContext, corpus: CorpusResource
) -> dg.SensorResult:
    """Requests Silver runs for system-jumps dates newly available upstream."""
    report = corpus.everef_missing_partitions(sj.DATASET)
    missing = report.get("missing", [])

    valid = set(sj.silver_partitions.get_partition_keys())
    eligible = sorted(date for date in missing if date in valid)
    selected = eligible[:MAX_PARTITIONS_PER_TICK]

    deferred = len(eligible) - len(selected)
    if deferred > 0:
        context.log.info(
            "availability: %d eligible, requesting %d this tick, %d deferred",
            len(eligible),
            len(selected),
            deferred,
        )

    run_requests = [
        dg.RunRequest(run_key=f"{sj.DATASET}-silver-{date}", partition_key=date)
        for date in selected
    ]
    return dg.SensorResult(run_requests=run_requests)


@dg.sensor(
    target=sj.system_jumps_history_gold,
    minimum_interval_seconds=3600,
    default_status=dg.DefaultSensorStatus.STOPPED,
)
def system_jumps_history_gold_sensor(
    context: dg.SensorEvaluationContext, corpus: CorpusResource
) -> dg.SensorResult:
    """Requests history-Gold runs for system-jumps dates whose window is complete.

    Polls ``corpus gold ready-dates --derivative system-traffic-history`` (the
    binary owns the coverage decision) and stays a thin cap-and-dedup loop,
    mirroring the market-history Gold sensor. The ``system-traffic-recent``
    derivative has no sensor — a schedule drives its non-partitioned asset.
    """
    report = corpus.gold_ready_dates(sj.DATASET, derivative=sj.HISTORY_DERIVATIVE)
    ready = report.get("ready", [])

    valid = set(sj.history_gold_partitions.get_partition_keys())
    eligible = sorted(date for date in ready if date in valid)
    selected = eligible[:MAX_PARTITIONS_PER_TICK]

    deferred = len(eligible) - len(selected)
    if deferred > 0:
        context.log.info(
            "gold-readiness: %d eligible, requesting %d this tick, %d deferred",
            len(eligible),
            len(selected),
            deferred,
        )

    run_requests = [
        dg.RunRequest(
            run_key=f"{sj.HISTORY_DERIVATIVE}-gold-{date}", partition_key=date
        )
        for date in selected
    ]
    return dg.SensorResult(run_requests=run_requests)


# --- market-orders (orderbook-aggregate, ADR-0033) ------------------------


@dg.sensor(
    target=mo.market_orders_silver,
    minimum_interval_seconds=3600,
    default_status=dg.DefaultSensorStatus.STOPPED,
)
def market_orders_availability_sensor(
    context: dg.SensorEvaluationContext, corpus: CorpusResource
) -> dg.SensorResult:
    """Requests Silver runs for market-orders dates newly available upstream."""
    report = corpus.everef_missing_partitions(mo.DATASET)
    missing = report.get("missing", [])

    valid = set(mo.silver_partitions.get_partition_keys())
    eligible = sorted(date for date in missing if date in valid)
    selected = eligible[:MAX_PARTITIONS_PER_TICK]

    deferred = len(eligible) - len(selected)
    if deferred > 0:
        context.log.info(
            "availability: %d eligible, requesting %d this tick, %d deferred",
            len(eligible),
            len(selected),
            deferred,
        )

    run_requests = [
        dg.RunRequest(run_key=f"{mo.DATASET}-silver-{date}", partition_key=date)
        for date in selected
    ]
    return dg.SensorResult(run_requests=run_requests)


@dg.sensor(
    target=mo.market_orders_gold,
    minimum_interval_seconds=3600,
    default_status=dg.DefaultSensorStatus.STOPPED,
)
def market_orders_gold_sensor(
    context: dg.SensorEvaluationContext, corpus: CorpusResource
) -> dg.SensorResult:
    """Requests orderbook-sweep Gold runs for dates whose Silver is present.

    Polls ``corpus gold ready-dates --derivative orderbook-sweep`` (the binary
    owns the readiness decision: target-day Silver present, prior-day look-back
    available, Gold not yet built) and stays a thin cap-and-dedup loop. The
    ``run_key`` is keyed on the derivative, like its Gold tree.
    """
    report = corpus.gold_ready_dates(mo.DATASET, derivative=mo.GOLD_DERIVATIVE)
    ready = report.get("ready", [])

    valid = set(mo.gold_partitions.get_partition_keys())
    eligible = sorted(date for date in ready if date in valid)
    selected = eligible[:MAX_PARTITIONS_PER_TICK]

    deferred = len(eligible) - len(selected)
    if deferred > 0:
        context.log.info(
            "gold-readiness: %d eligible, requesting %d this tick, %d deferred",
            len(eligible),
            len(selected),
            deferred,
        )

    run_requests = [
        dg.RunRequest(run_key=f"{mo.GOLD_DERIVATIVE}-gold-{date}", partition_key=date)
        for date in selected
    ]
    return dg.SensorResult(run_requests=run_requests)


# --- sde (build-versioned, ADR-0031) --------------------------------------


def _parse_build_key(partition_key: str) -> int | None:
    """Parses ``build=<n>`` (the SDE run-state partition key) to ``n``."""
    suffix = partition_key.removeprefix("build=")
    if suffix == partition_key:
        return None
    try:
        return int(suffix)
    except ValueError:
        return None


@dg.sensor(
    target=sde.sde_silver,
    minimum_interval_seconds=3600,
    default_status=dg.DefaultSensorStatus.STOPPED,
)
def sde_build_discovery_sensor(
    context: dg.SensorEvaluationContext, corpus: CorpusResource
) -> dg.SensorResult:
    """Discovers SDE builds, registers each as a dynamic partition, ingests it.

    SDE is build-versioned: ``corpus everef list`` lists builds (not days), so the
    partition matrix is dynamic and grows here. New build numbers are added to
    ``sde_builds`` and a Silver run requested per build, oldest first, capped per
    tick. ``run_key`` dedup keeps an already-requested build from re-queuing.
    """
    builds = corpus.everef_list_builds(sde.DATASET)
    discovered = sorted({int(b["build"]) for b in builds})

    existing = set(
        sde.build_partitions.get_partition_keys(
            dynamic_partitions_store=context.instance
        )
    )
    new_builds = [b for b in discovered if str(b) not in existing]
    selected = new_builds[:MAX_PARTITIONS_PER_TICK]

    deferred = len(new_builds) - len(selected)
    if deferred > 0:
        context.log.info(
            "sde-discovery: %d new builds, requesting %d this tick, %d deferred",
            len(new_builds),
            len(selected),
            deferred,
        )

    keys = [str(b) for b in selected]
    run_requests = [
        dg.RunRequest(run_key=f"sde-silver-{b}", partition_key=b) for b in keys
    ]
    return dg.SensorResult(
        run_requests=run_requests,
        dynamic_partitions_requests=[sde.build_partitions.build_add_request(keys)],
    )


@dg.sensor(
    target=sde.sde_changelog_gold,
    minimum_interval_seconds=3600,
    default_status=dg.DefaultSensorStatus.STOPPED,
)
def sde_gold_sensor(
    context: dg.SensorEvaluationContext, corpus: CorpusResource
) -> dg.SensorResult:
    """Requests the unified changelog for builds whose Silver is committed.

    There is no ``ready-dates`` for SDE (no coverage window); readiness is keyed
    on corpus run-state (ADR-0032) — a build whose unified Silver partition
    (``dataset = sde``) is committed. The binary owns the predecessor lookup and
    the baseline skip, so this stays a thin cap-and-dedup loop: one changelog run
    per build, ``run_key``-deduped. A baseline build's run is requested once and
    writes nothing (the binary skips it). The snapshot is not driven here — it is
    a non-partitioned, latest-only asset on :data:`sde_snapshot_schedule`.
    """
    rows = corpus.state_query(
        "SELECT DISTINCT partition_key FROM partitions "
        "WHERE tier = 'silver' AND dataset = 'sde'"
    )
    committed = sorted(
        {
            build
            for row in rows
            if (build := _parse_build_key(row["partition_key"])) is not None
        }
    )

    valid = set(
        sde.build_partitions.get_partition_keys(
            dynamic_partitions_store=context.instance
        )
    )
    eligible = [b for b in committed if str(b) in valid]
    selected = eligible[:MAX_PARTITIONS_PER_TICK]

    deferred = len(eligible) - len(selected)
    if deferred > 0:
        context.log.info(
            "sde-gold: %d builds with committed Silver, requesting %d, %d deferred",
            len(eligible),
            len(selected),
            deferred,
        )

    run_requests = [
        dg.RunRequest(
            run_key=f"{sde.CHANGELOG_DERIVATIVE}-{build}",
            partition_key=str(build),
        )
        for build in selected
    ]
    return dg.SensorResult(run_requests=run_requests)


# Hourly navigate-now refresh of the EWMA "recent" heat. A schedule, not a
# sensor: there is no per-date matrix to diff, only "rebuild the latest". The
# asset omits the `gold_heavy` pool, so this cadence cannot starve the windowed
# history backfills under `max_concurrent_runs`.
system_jumps_recent_schedule = dg.ScheduleDefinition(
    name="system_jumps_recent_schedule",
    target=sj.system_jumps_recent_gold,
    cron_schedule="0 * * * *",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)


# Daily rebuild of the latest-only SDE snapshot (ADR-0032). A schedule, not a
# sensor: the snapshot is non-partitioned ("rebuild the latest"), like the
# system-jumps recent asset. SDE only changes on a game patch (every few days),
# so a daily cadence keeps the served catalogue fresh without churn; the
# build-discovery + gold sensors already pick up new builds for Silver and the
# changelog within the hour. The asset self-skips when no Silver is committed.
sde_snapshot_schedule = dg.ScheduleDefinition(
    name="sde_snapshot_schedule",
    target=sde.sde_snapshot_gold,
    cron_schedule="0 2 * * *",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)
