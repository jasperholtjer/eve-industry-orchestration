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

from eve_industry_orchestration.defs import industry_cost_indices as ici
from eve_industry_orchestration.defs import industry_cost_indices_live as icil
from eve_industry_orchestration.defs import killmails as km
from eve_industry_orchestration.defs import market_orders as mo
from eve_industry_orchestration.defs import market_orders_live as mol
from eve_industry_orchestration.defs import market_prices_live as mpl
from eve_industry_orchestration.defs import mer, sde
from eve_industry_orchestration.defs import structures as st
from eve_industry_orchestration.defs import system_jumps as sj
from eve_industry_orchestration.defs import system_kills as sk
from eve_industry_orchestration.defs.corpus_resource import CorpusResource
from eve_industry_orchestration.defs.market_history import (
    DATASET,
    gold_partitions,
    market_history_gold,
    market_history_silver,
    silver_partitions,
)
from eve_industry_orchestration.defs.sensor_util import (
    MAX_PARTITIONS_PER_TICK,
    request_partitions,
)


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
    # No download tag here: the `everef_download` pool on the asset
    # (market_history.py) throttles fetches across every launch path.
    return request_partitions(
        context,
        reported=report.get("missing", []),
        valid=set(silver_partitions.get_partition_keys()),
        run_key_prefix=f"{DATASET}-silver",
        asset_key=market_history_silver.key,
        label="availability",
    )


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
    # No memory tag here: the `heavy` pool on the asset (market_history.py)
    # throttles every launch path — sensor, backfill, manual — so this stays a
    # thin cap-and-dedup loop.
    return request_partitions(
        context,
        reported=report.get("ready", []),
        valid=set(gold_partitions.get_partition_keys()),
        run_key_prefix=f"{DATASET}-gold",
        asset_key=market_history_gold.key,
        label="gold-readiness",
    )


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
    return request_partitions(
        context,
        reported=report.get("missing", []),
        valid=set(sj.silver_partitions.get_partition_keys()),
        run_key_prefix=f"{sj.DATASET}-silver",
        asset_key=sj.system_jumps_silver.key,
        label="availability",
    )


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
    return request_partitions(
        context,
        reported=report.get("ready", []),
        valid=set(sj.history_gold_partitions.get_partition_keys()),
        run_key_prefix=f"{sj.HISTORY_DERIVATIVE}-gold",
        asset_key=sj.system_jumps_history_gold.key,
        label="gold-readiness",
    )


# --- industry-cost-indices (cost-index-history, ADR-0043) -----------------


@dg.sensor(
    target=ici.industry_cost_indices_silver,
    minimum_interval_seconds=3600,
    default_status=dg.DefaultSensorStatus.STOPPED,
)
def industry_cost_indices_availability_sensor(
    context: dg.SensorEvaluationContext, corpus: CorpusResource
) -> dg.SensorResult:
    """Requests Silver runs for cost-index dates newly available upstream."""
    report = corpus.everef_missing_partitions(ici.DATASET)
    return request_partitions(
        context,
        reported=report.get("missing", []),
        valid=set(ici.silver_partitions.get_partition_keys()),
        run_key_prefix=f"{ici.DATASET}-silver",
        asset_key=ici.industry_cost_indices_silver.key,
        label="availability",
    )


@dg.sensor(
    target=ici.industry_cost_indices_history_gold,
    minimum_interval_seconds=3600,
    default_status=dg.DefaultSensorStatus.STOPPED,
)
def industry_cost_indices_history_gold_sensor(
    context: dg.SensorEvaluationContext, corpus: CorpusResource
) -> dg.SensorResult:
    """Requests history-Gold runs for cost-index dates whose window is complete.

    Polls ``corpus gold ready-dates --derivative industry-cost-indices-history``
    (the binary owns the coverage decision) and stays a thin cap-and-dedup loop,
    mirroring the system-jumps history Gold sensor.
    """
    report = corpus.gold_ready_dates(ici.DATASET, derivative=ici.HISTORY_DERIVATIVE)
    return request_partitions(
        context,
        reported=report.get("ready", []),
        valid=set(ici.history_gold_partitions.get_partition_keys()),
        run_key_prefix=f"{ici.HISTORY_DERIVATIVE}-gold",
        asset_key=ici.industry_cost_indices_history_gold.key,
        label="gold-readiness",
    )


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
    return request_partitions(
        context,
        reported=report.get("missing", []),
        valid=set(mo.silver_partitions.get_partition_keys()),
        run_key_prefix=f"{mo.DATASET}-silver",
        asset_key=mo.market_orders_silver.key,
        label="availability",
    )


def _build_orderbook_gold_sensor(
    derivative: str, asset: dg.AssetsDefinition
) -> dg.SensorDefinition:
    """Builds a Gold readiness sensor for one market-orders derivative.

    Polls ``corpus gold ready-dates --derivative <derivative>`` (the binary owns
    the readiness decision: target-day Silver present, prior-day look-back
    available, Gold not yet built) and stays a thin cap-and-dedup loop. The
    ``run_key`` is keyed on the derivative, like its Gold tree.
    """

    @dg.sensor(
        name=f"{derivative.replace('-', '_')}_gold_sensor",
        target=asset,
        minimum_interval_seconds=3600,
        default_status=dg.DefaultSensorStatus.STOPPED,
    )
    def _sensor(
        context: dg.SensorEvaluationContext, corpus: CorpusResource
    ) -> dg.SensorResult:
        report = corpus.gold_ready_dates(mo.DATASET, derivative=derivative)
        return request_partitions(
            context,
            reported=report.get("ready", []),
            valid=set(mo.gold_partitions.get_partition_keys()),
            run_key_prefix=f"{derivative}-gold",
            asset_key=asset.key,
            label="gold-readiness",
        )

    return _sensor


market_orders_snapshot_gold_sensor = _build_orderbook_gold_sensor(
    mo.SNAPSHOT_DERIVATIVE, mo.market_orders_snapshot_gold
)
market_orders_changes_gold_sensor = _build_orderbook_gold_sensor(
    mo.CHANGES_DERIVATIVE, mo.market_orders_changes_gold
)
market_orders_events_gold_sensor = _build_orderbook_gold_sensor(
    mo.EVENTS_DERIVATIVE, mo.market_orders_events_gold
)


# --- system-kills (per-measure Gold, ADR-0037) ----------------------------


@dg.sensor(
    target=sk.system_kills_silver,
    minimum_interval_seconds=3600,
    default_status=dg.DefaultSensorStatus.STOPPED,
)
def system_kills_availability_sensor(
    context: dg.SensorEvaluationContext, corpus: CorpusResource
) -> dg.SensorResult:
    """Requests Silver runs for system-kills dates newly available upstream."""
    report = corpus.everef_missing_partitions(sk.DATASET)
    return request_partitions(
        context,
        reported=report.get("missing", []),
        valid=set(sk.silver_partitions.get_partition_keys()),
        run_key_prefix=f"{sk.DATASET}-silver",
        asset_key=sk.system_kills_silver.key,
        label="availability",
    )


def _build_kills_history_gold_sensor(
    derivative: str, asset: dg.AssetsDefinition
) -> dg.SensorDefinition:
    """Builds a history-Gold readiness sensor for one system-kills measure.

    Polls ``corpus gold ready-dates --derivative <derivative>`` (the binary owns
    the coverage decision) and stays a thin cap-and-dedup loop, mirroring the
    system-jumps history Gold sensor. The ``-recent`` derivatives have no
    sensor — schedules drive their non-partitioned assets.
    """

    @dg.sensor(
        name=f"{derivative.replace('-', '_')}_gold_sensor",
        target=asset,
        minimum_interval_seconds=3600,
        default_status=dg.DefaultSensorStatus.STOPPED,
    )
    def _sensor(
        context: dg.SensorEvaluationContext, corpus: CorpusResource
    ) -> dg.SensorResult:
        report = corpus.gold_ready_dates(sk.DATASET, derivative=derivative)
        return request_partitions(
            context,
            reported=report.get("ready", []),
            valid=set(sk.history_gold_partitions.get_partition_keys()),
            run_key_prefix=f"{derivative}-gold",
            asset_key=asset.key,
            label="gold-readiness",
        )

    return _sensor


system_kills_ship_history_gold_sensor = _build_kills_history_gold_sensor(
    sk.HISTORY_DERIVATIVES[0], sk.system_kills_ship_history_gold
)
system_kills_npc_history_gold_sensor = _build_kills_history_gold_sensor(
    sk.HISTORY_DERIVATIVES[1], sk.system_kills_npc_history_gold
)
system_kills_pod_history_gold_sensor = _build_kills_history_gold_sensor(
    sk.HISTORY_DERIVATIVES[2], sk.system_kills_pod_history_gold
)


# --- structures (dimension + population covariate, corpus ADR-0057) -------


@dg.sensor(
    target=st.structures_silver,
    minimum_interval_seconds=3600,
    default_status=dg.DefaultSensorStatus.STOPPED,
)
def structures_availability_sensor(
    context: dg.SensorEvaluationContext, corpus: CorpusResource
) -> dg.SensorResult:
    """Requests Silver runs for structures dates newly available upstream."""
    report = corpus.everef_missing_partitions(st.DATASET)
    return request_partitions(
        context,
        reported=report.get("missing", []),
        valid=set(st.silver_partitions.get_partition_keys()),
        run_key_prefix=f"{st.DATASET}-silver",
        asset_key=st.structures_silver.key,
        label="availability",
    )


def _build_structures_gold_sensor(
    derivative: str,
    asset: dg.AssetsDefinition,
    partitions: dg.DailyPartitionsDefinition,
) -> dg.SensorDefinition:
    """Builds a Gold readiness sensor for one structures derivative.

    Polls ``corpus gold ready-dates --derivative <derivative>`` — the binary owns
    the readiness decision (target-day Silver present, the 30-day window at
    ``coverage_min_ratio`` for the covariate, Gold not yet built) — and stays a
    thin cap-and-dedup loop. The two derivatives have **different** Gold starts
    (the dimension serves a month earlier than the covariate), so each sensor
    validates against its own partition matrix.

    Neither sensor checks the SDE snapshot Gold: the build reads it and fails
    loud when it is absent, and a stale-but-present snapshot is a fingerprint
    recorded in ``_INDEX.json``, never a run this sensor triggers.
    """

    @dg.sensor(
        name=f"{derivative.replace('-', '_')}_gold_sensor",
        target=asset,
        minimum_interval_seconds=3600,
        default_status=dg.DefaultSensorStatus.STOPPED,
    )
    def _sensor(
        context: dg.SensorEvaluationContext, corpus: CorpusResource
    ) -> dg.SensorResult:
        report = corpus.gold_ready_dates(st.DATASET, derivative=derivative)
        return request_partitions(
            context,
            reported=report.get("ready", []),
            valid=set(partitions.get_partition_keys()),
            run_key_prefix=f"{derivative}-gold",
            asset_key=asset.key,
            label="gold-readiness",
        )

    return _sensor


structures_snapshot_gold_sensor = _build_structures_gold_sensor(
    st.SNAPSHOT_DERIVATIVE,
    st.structures_snapshot_gold,
    st.snapshot_gold_partitions,
)
structure_population_history_gold_sensor = _build_structures_gold_sensor(
    st.POPULATION_DERIVATIVE,
    st.structure_population_history_gold,
    st.population_gold_partitions,
)


# --- killmails (consumption demand history, corpus ADR-0059/0060/0061) ----
#
# Four sensors, not two. The usual pair (availability + Gold readiness) covers the
# normal forward path; the other two exist because killmail partitions are the
# only **mutable** ones in the corpus. A day keeps growing upstream long after
# first archival, and neither of the normal signals can see that: `everef
# missing-partitions` reports only days with no partition at all, and `gold
# ready-dates` only days whose Gold does not yet exist. Left alone, a day would be
# frozen at its first-ingest count forever.


@dg.sensor(
    target=km.killmails_silver,
    minimum_interval_seconds=3600,
    default_status=dg.DefaultSensorStatus.STOPPED,
)
def killmails_availability_sensor(
    context: dg.SensorEvaluationContext, corpus: CorpusResource
) -> dg.SensorResult:
    """Requests Silver runs for killmail dates newly available upstream."""
    report = corpus.everef_missing_partitions(km.DATASET)
    return request_partitions(
        context,
        reported=report.get("missing", []),
        valid=set(km.silver_partitions.get_partition_keys()),
        run_key_prefix=f"{km.DATASET}-silver",
        asset_key=km.killmails_silver.key,
        label="availability",
    )


@dg.sensor(
    target=km.killmails_consumption_gold,
    minimum_interval_seconds=3600,
    default_status=dg.DefaultSensorStatus.STOPPED,
)
def killmails_consumption_gold_sensor(
    context: dg.SensorEvaluationContext, corpus: CorpusResource
) -> dg.SensorResult:
    """Requests Gold runs for killmail dates whose Silver window is complete.

    Polls ``corpus gold ready-dates --derivative killmails-consumption`` — the
    binary owns the readiness decision (target-day Silver present, the 365-day
    window at ``coverage_min_ratio``, Gold not yet built) — and stays a thin
    cap-and-dedup loop.

    It does not check the SDE snapshot or market-history Gold the build joins
    against: the build reads both and fails loud when either is absent, and a
    stale-but-present upstream is a fingerprint recorded in ``_INDEX.json``, never
    a run this sensor triggers.
    """
    report = corpus.gold_ready_dates(km.DATASET, derivative=km.CONSUMPTION_DERIVATIVE)
    return request_partitions(
        context,
        reported=report.get("ready", []),
        valid=set(km.gold_partitions.get_partition_keys()),
        run_key_prefix=f"{km.CONSUMPTION_DERIVATIVE}-gold",
        asset_key=km.killmails_consumption_gold.key,
        label="gold-readiness",
    )


@dg.sensor(
    target=km.killmails_silver,
    # Daily, not hourly. Drift is slow-moving — zKillboard surfaces old kills over
    # days to years — and each tick costs an upstream `totals.json` fetch plus, on
    # a hit, a re-ingest of the corpus's heaviest Silver. Hourly would buy nothing
    # and re-parse a lot.
    minimum_interval_seconds=86400,
    default_status=dg.DefaultSensorStatus.STOPPED,
)
def killmails_freshness_sensor(
    context: dg.SensorEvaluationContext, corpus: CorpusResource
) -> dg.SensorResult:
    """Re-proposes killmail days whose upstream kill count has changed (ADR-0060).

    ``corpus killmails freshness`` diffs upstream's root ``totals.json`` against
    the count each partition recorded at ingest. A day it reports has genuinely
    grown upstream, so re-ingesting it is not a retry — it is the repair, and the
    only path by which late-discovered destruction enters the corpus.

    The binary is read-only; the *decision* to re-propose lives here, which is why
    this is a sensor and not a corpus side effect. Rotating ``run_key`` tokens make
    the re-request legal even though the date already materialised once, and the
    shared in-flight guard keeps this and the availability sensor from putting two
    writers on one partition. Self-limiting: once the repair ingest updates the
    token the day stops being reported.
    """
    drifted = corpus.killmails_freshness(km.DATASET)
    dates = [str(row["date"]) for row in drifted if row.get("date")]
    if dates:
        context.log.info(
            "killmails-freshness: %d day(s) drifted upstream, re-proposing for "
            "re-ingest (oldest first)",
            len(dates),
        )
    return request_partitions(
        context,
        reported=dates,
        valid=set(km.silver_partitions.get_partition_keys()),
        run_key_prefix=f"{km.DATASET}-freshness",
        asset_key=km.killmails_silver.key,
        label="freshness",
    )


@dg.sensor(
    target=km.killmails_consumption_gold,
    minimum_interval_seconds=86400,
    default_status=dg.DefaultSensorStatus.STOPPED,
)
def killmails_consumption_gold_repair_sensor(
    context: dg.SensorEvaluationContext, corpus: CorpusResource
) -> dg.SensorResult:
    """Rebuilds Gold for killmail days whose Silver was re-ingested underneath it.

    The other half of the drift repair. ``gold ready-dates`` reports only days with
    no Gold yet, so a day repaired by
    :func:`killmails_freshness_sensor` would keep serving Gold built from the
    superseded Silver — sealed, sha-consistent, and wrong. This sensor asks
    run-state which Gold partitions predate their own Silver
    (``silver.last_seen_at > gold.last_seen_at``) and rebuilds exactly those.

    Ordering is implicit and needs no cross-sensor bookkeeping: ``last_seen_at``
    only moves once the repair ingest **commits**, so a day becomes eligible here
    on the tick after its Silver settles, never before.

    Scope is deliberately the repaired day itself, not its forward window. A
    changed day D also feeds the window features of D+1..D+365, so those stay
    marginally stale until independently rebuilt; rebuilding 366 partitions per
    drifted day would put the corpus's heaviest build in a multi-day queue for a
    handful of late-discovered kills. The day's own base columns
    (``qty_destroyed``, ``isk_value_destroyed``) — the ones consumers read
    directly — are made correct.
    """
    stale = corpus.stale_gold_dates(km.DATASET, km.CONSUMPTION_DERIVATIVE)
    if stale:
        context.log.info(
            "killmails-gold-repair: %d Gold partition(s) predate their Silver, "
            "rebuilding (oldest first)",
            len(stale),
        )
    return request_partitions(
        context,
        reported=stale,
        valid=set(km.gold_partitions.get_partition_keys()),
        run_key_prefix=f"{km.CONSUMPTION_DERIVATIVE}-repair",
        asset_key=km.killmails_consumption_gold.key,
        label="gold-repair",
    )


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
# asset omits the `heavy` pool, so this cadence cannot starve the windowed
# history backfills under `max_concurrent_runs`.
system_jumps_recent_schedule = dg.ScheduleDefinition(
    name="system_jumps_recent_schedule",
    target=sj.system_jumps_recent_gold,
    cron_schedule="0 * * * *",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)


# Hourly navigate-now refresh of each per-measure EWMA "danger-now" heat
# (ship/npc/pod), mirroring system_jumps_recent_schedule. Schedules, not sensors:
# there is no per-date matrix to diff, only "rebuild the latest". The assets omit
# the `heavy` pool, so this cadence cannot starve the windowed history
# backfills under `max_concurrent_runs`.
def _build_kills_recent_schedule(
    derivative: str, asset: dg.AssetsDefinition
) -> dg.ScheduleDefinition:
    return dg.ScheduleDefinition(
        name=f"{derivative.replace('-', '_')}_schedule",
        target=asset,
        cron_schedule="0 * * * *",
        default_status=dg.DefaultScheduleStatus.STOPPED,
    )


system_kills_ship_recent_schedule = _build_kills_recent_schedule(
    sk.RECENT_DERIVATIVES[0], sk.system_kills_ship_recent_gold
)
system_kills_npc_recent_schedule = _build_kills_recent_schedule(
    sk.RECENT_DERIVATIVES[1], sk.system_kills_npc_recent_gold
)
system_kills_pod_recent_schedule = _build_kills_recent_schedule(
    sk.RECENT_DERIVATIVES[2], sk.system_kills_pod_recent_gold
)


# --- mer (monthly-archive, corpus ADR-0058) -------------------------------


@dg.sensor(
    target=[mer.mer_silver, mer.mer_killdump_silver],
    minimum_interval_seconds=3600,
    default_status=dg.DefaultSensorStatus.STOPPED,
)
def mer_report_discovery_sensor(
    context: dg.SensorEvaluationContext, corpus: CorpusResource
) -> dg.SensorResult:
    """Discovers MER report-months, registers each partition, ingests both streams.

    MER is monthly-archive: ``corpus everef list`` lists report-months (not days),
    so the partition matrix is dynamic and grows here. New report-months
    (``YYYY-MM-01``) are added to ``mer_report_months`` and one run per month is
    requested — materialising **both** ``mer_silver`` and ``mer_killdump_silver``
    from the same ZIP — oldest first, capped per tick. ``run_key`` dedup keeps an
    already-requested month from re-queuing.

    A late *revision* of an already-ingested month is not re-requested here (the
    month key is already registered); revision re-latching is a manual operator
    action until a corpus completeness/`Last-Modified` gate lands (ADR-0058
    §Cadence), mirroring the SDE monotonic-discovery model.
    """
    reports = corpus.everef_list_reports(mer.DATASET)
    discovered = sorted({r["report_month"] for r in reports})

    existing = set(
        mer.report_partitions.get_partition_keys(
            dynamic_partitions_store=context.instance
        )
    )
    new_months = [m for m in discovered if m not in existing]
    selected = new_months[:MAX_PARTITIONS_PER_TICK]

    deferred = len(new_months) - len(selected)
    if deferred > 0:
        context.log.info(
            "mer-discovery: %d new report-months, requesting %d this tick, %d deferred",
            len(new_months),
            len(selected),
            deferred,
        )

    run_requests = [
        dg.RunRequest(run_key=f"mer-silver-{m}", partition_key=m) for m in selected
    ]
    return dg.SensorResult(
        run_requests=run_requests,
        dynamic_partitions_requests=[mer.report_partitions.build_add_request(selected)],
    )


# Daily rebuild of the five latest-only MER kern-series Gold trees (corpus
# ADR-0058 §5). A schedule, not a sensor: each is a full cross-month point-in-time
# merge over all committed Silver ("rebuild the merge"), with no per-date matrix
# to diff. MER publishes monthly, so a daily cadence keeps the served history
# fresh (picking up a new report-month within a day of the discovery sensor
# ingesting it) without churn; each asset self-skips when no Silver is committed.
mer_history_schedule = dg.ScheduleDefinition(
    name="mer_history_schedule",
    target=list(mer.HISTORY_GOLD_ASSETS),
    cron_schedule="0 3 * * *",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)


# Daily rebuild of the latest-only SDE Gold catalogues (ADR-0032/0044/0056). A
# schedule, not a sensor: all are non-partitioned ("rebuild the latest"), like
# the system-jumps recent asset. SDE only changes on a game patch (every few
# days), so a daily cadence keeps the served catalogue + product universe +
# industry-facility/hub maps fresh without churn; the build-discovery + gold
# sensors already pick up new builds for Silver and the changelog within the
# hour. Each asset self-skips when no Silver is committed.
sde_snapshot_schedule = dg.ScheduleDefinition(
    name="sde_snapshot_schedule",
    target=[
        sde.sde_snapshot_gold,
        sde.sde_industry_products_gold,
        sde.sde_industry_facilities_gold,
        sde.sde_industry_hubs_gold,
    ],
    cron_schedule="0 2 * * *",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)


# Half-hourly refresh of the live orderbook aggregate (corpus ADR-0039). A
# schedule, not a sensor, and a deliberate departure from "sensor over cron":
# there is no per-date matrix to diff, only "overwrite current/ with the newest
# snapshot". The cadence matches EVE Ref's ~30-min snapshot rhythm. The asset
# joins the `everef_download` pool (one fetch per run), not `heavy`, so it cannot
# starve the windowed backfills under max_concurrent_runs.
market_orders_live_schedule = dg.ScheduleDefinition(
    name="market_orders_live_schedule",
    target=mol.market_orders_live_gold,
    cron_schedule="*/30 * * * *",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)


# Hourly refresh of the live CCP prices (corpus ADR-0040). Same schedule-not-
# sensor rationale as `market_orders_live_schedule`: there is no per-date matrix
# to diff, only "overwrite current/ with the latest ESI snapshot". CCP recomputes
# the prices ~once per day and the exact update time is unknown, while ESI caches
# for ~40 min — so hourly is comfortably fresh at negligible cost. The asset hits
# ESI (not EVE Ref), so it joins no `everef_download` pool and obeys only the
# global concurrency cap.
market_prices_live_schedule = dg.ScheduleDefinition(
    name="market_prices_live_schedule",
    target=mpl.market_prices_live_gold,
    cron_schedule="0 * * * *",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)


# Hourly refresh of the live cost-index level (corpus ADR-0043). Same schedule-
# not-sensor rationale as `market_orders_live_schedule`: there is no per-date
# matrix to diff, only "overwrite current/ with the newest snapshot". The hourly
# cadence matches EVE Ref's cost-index publish rhythm. The source is EVE Ref, so
# the asset joins the `everef_download` pool (one fetch per run), not `heavy`, and
# cannot starve the windowed backfills under max_concurrent_runs.
industry_cost_indices_live_schedule = dg.ScheduleDefinition(
    name="industry_cost_indices_live_schedule",
    target=icil.industry_cost_indices_live_gold,
    cron_schedule="0 * * * *",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)


# --- context datasets (Bronze-only archival, corpus ADR-0045/0046/0048) ----
#
# Daily archival fetch of the CCP news feed and the YouTube transcripts. Schedules,
# not sensors, and a deliberate departure from "sensor over cron": the fetch is
# keyed on the fetch date (one dense Bronze partition per day) and the binary dedups
# already-archived documents via its seen-ledger, so there is no per-date upstream
# matrix to diff. Late UTC evening, after CCP's publishing day, staggered 30 min
# apart. STOPPED by default. Neither asset joins a pool (it hits neither EVE Ref nor
# ESI), so both obey only the global concurrency cap; the historical sweeps run via
# the manually-triggered `news_backfill_job` / `transcripts_backfill_job`, not here.
# news is the one context dataset with a Silver/Gold chain (ADR-0050/0052), so its
# schedule targets the whole `news` group — fetch → ingest → the four Gold trees —
# in one run, in dependency order. Every tier is keyed on the same fetch date and
# each Gold partition is a pure function of that day's Silver, so there is nothing
# to diff per-date and no sensor to write. The `news-entity-mentions` build reads
# the `sde-*` Gold snapshots (cross-dataset input) but does not rebuild them: the
# group selection stops at the news assets, so a stale SDE snapshot is a
# fingerprint recorded in `_INDEX.json`, never a run this schedule triggers.
news_daily_schedule = dg.ScheduleDefinition(
    name="news_daily_schedule",
    target=dg.AssetSelection.groups("news"),
    cron_schedule="0 22 * * *",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)


# transcripts now carries a full Silver/Gold chain (ADR-0055), so its schedule
# targets the whole `transcripts` group — fetch -> ingest -> videos/sections/
# entity-mentions Gold (+ embeddings) — in one run, in dependency order, exactly
# like `news_daily_schedule`. The embed step shares the `news_embed` limit-1 pool
# with news-embeddings, so no two embeds overlap even though both schedules fire in
# the same late-evening window (staggered 30 min apart). Annotations are NOT in this
# group's scheduled chain: `transcripts-annotations` is a manual operator run via the
# `annotate-transcripts` skill (contract `t2`), never a Dagster asset.
transcripts_daily_schedule = dg.ScheduleDefinition(
    name="transcripts_daily_schedule",
    target=dg.AssetSelection.groups("transcripts"),
    cron_schedule="30 22 * * *",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)
