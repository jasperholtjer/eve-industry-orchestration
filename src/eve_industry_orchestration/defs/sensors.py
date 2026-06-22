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
