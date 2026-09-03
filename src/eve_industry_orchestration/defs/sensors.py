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

from typing import Any

import dagster as dg

from eve_industry_orchestration.defs import industry_cost_indices as ici
from eve_industry_orchestration.defs import industry_cost_indices_live as icil
from eve_industry_orchestration.defs import killmails as km
from eve_industry_orchestration.defs import lp_store_offers_live as lpsol
from eve_industry_orchestration.defs import market_orders as mo
from eve_industry_orchestration.defs import market_orders_live as mol
from eve_industry_orchestration.defs import market_prices_live as mpl
from eve_industry_orchestration.defs import mer, sde, sensor_util
from eve_industry_orchestration.defs import public_contracts as pc
from eve_industry_orchestration.defs import public_contracts_live as pcl
from eve_industry_orchestration.defs import sovereignty_campaigns as sc
from eve_industry_orchestration.defs import sovereignty_map as sm
from eve_industry_orchestration.defs import sovereignty_structures as ss
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
    window at ``coverage_min_ratio``, the same-day ``market-history`` Gold built,
    Gold not yet built) — and stays a thin cap-and-dedup loop.

    The market-history gate is the binary's, not this sensor's: EVE Ref settles a
    day's market-history file hours after that day's killmail tar, so without it
    every fresh date would be proposed into a build that fails on the unsealed
    price input. The SDE snapshot is deliberately *not* gated — it is a
    date-independent tree whose absence is a configuration error the build should
    surface. A stale-but-present upstream is likewise never a run this sensor
    triggers; it is a fingerprint recorded in ``_INDEX.json``.
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


# --- sovereignty (map / structures / campaigns, corpus ADR-0066) ----------
#
# Three availability sensors and five Gold readiness sensors. All three datasets
# are hourly-folder-tar datasets whose days settle with the usual EVE Ref lag, so
# availability is the same thin cap-and-dedup loop as every other Silver sensor —
# the missing set comes from corpus run-state, never from listing the NAS tree,
# and the `everef_download` pool on the assets throttles the fetches across every
# launch path, so no sensor tag is set here.


@dg.sensor(
    target=sm.sovereignty_map_silver,
    minimum_interval_seconds=3600,
    default_status=dg.DefaultSensorStatus.STOPPED,
)
def sovereignty_map_availability_sensor(
    context: dg.SensorEvaluationContext, corpus: CorpusResource
) -> dg.SensorResult:
    """Requests Silver runs for sovereignty-map dates newly available upstream."""
    report = corpus.everef_missing_partitions(sm.DATASET)
    return request_partitions(
        context,
        reported=report.get("missing", []),
        valid=set(sm.silver_partitions.get_partition_keys()),
        run_key_prefix=f"{sm.DATASET}-silver",
        asset_key=sm.sovereignty_map_silver.key,
        label="availability",
    )


@dg.sensor(
    target=ss.sovereignty_structures_silver,
    minimum_interval_seconds=3600,
    default_status=dg.DefaultSensorStatus.STOPPED,
)
def sovereignty_structures_availability_sensor(
    context: dg.SensorEvaluationContext, corpus: CorpusResource
) -> dg.SensorResult:
    """Requests Silver runs for sovereignty-structures dates newly available."""
    report = corpus.everef_missing_partitions(ss.DATASET)
    return request_partitions(
        context,
        reported=report.get("missing", []),
        valid=set(ss.silver_partitions.get_partition_keys()),
        run_key_prefix=f"{ss.DATASET}-silver",
        asset_key=ss.sovereignty_structures_silver.key,
        label="availability",
    )


@dg.sensor(
    target=sc.sovereignty_campaigns_silver,
    minimum_interval_seconds=3600,
    default_status=dg.DefaultSensorStatus.STOPPED,
)
def sovereignty_campaigns_availability_sensor(
    context: dg.SensorEvaluationContext, corpus: CorpusResource
) -> dg.SensorResult:
    """Requests Silver runs for sovereignty-campaigns dates newly available."""
    report = corpus.everef_missing_partitions(sc.DATASET)
    return request_partitions(
        context,
        reported=report.get("missing", []),
        valid=set(sc.silver_partitions.get_partition_keys()),
        run_key_prefix=f"{sc.DATASET}-silver",
        asset_key=sc.sovereignty_campaigns_silver.key,
        label="availability",
    )


def _blocked_skip_reason(blocked: list[dict[str, Any]]) -> dg.SkipReason | None:
    """Reports why the binary held every candidate date back, or ``None``.

    ADR-0066 §8's window gate turned a lagging ``sovereignty-changes`` tree from
    a panel day sealed with NULL flip counts into a panel day that is simply
    never proposed — better data, but an operator would see a sensor requesting
    nothing and nothing anywhere saying why. ``gold ready-dates`` already
    answers that in ``blocked[]``; this records what it said. No readiness
    decision moves: the binary still decides, and the sensor still reads only
    ``ready``.
    """
    if not blocked:
        return None
    first = min(blocked, key=lambda entry: str(entry.get("date", "")))
    return dg.SkipReason(
        f"no date ready; {len(blocked)} blocked, earliest "
        f"{first.get('date')} on {first.get('block')}"
    )


def _build_sovereignty_gold_sensor(
    dataset: str,
    derivative: str,
    asset: dg.AssetsDefinition,
    partitions: dg.DailyPartitionsDefinition,
) -> dg.SensorDefinition:
    """Builds a Gold readiness sensor for one sovereignty derivative.

    Parameterised on the **dataset** as well as the derivative, unlike the
    market-orders and structures factories: this family's five Gold trees span
    three source datasets (``sovereignty-map`` owns ownership / changes / panel,
    ``sovereignty-structures`` owns adm, ``sovereignty-campaigns`` owns
    contests), so the poll target cannot be a module constant.

    Polls ``corpus gold ready-dates --derivative <derivative>`` and stays a thin
    cap-and-dedup loop. The binary owns every readiness decision — the Silver
    window at ``coverage_min_ratio`` for the four per-dataset trees, and for the
    panel the same day's three sibling Gold partitions: ownership, adm and
    contests (ADR-0052 sibling read, ADR-0066 decision 8). A sibling that skipped
    its day simply never becomes ready here, so the panel's build order needs no
    cross-sensor bookkeeping on top of the asset-graph edge.

    ``sovereignty-changes`` gates it too, as a *window* prerequisite rather than
    a same-day one: the panel is ready only once every day of the trailing
    ``[D-30, D)`` flip window is built in that tree or is a recorded gap on
    ``sovereignty-map`` Silver. A panel day is therefore never sealed with NULL
    flip counts because its changes tree lagged — it waits instead. Running four
    of these five sensors stalls the panel rather than degrading it; run all
    five, and a tick that requests nothing says which gate held the earliest
    candidate back.

    Each derivative validates against **its own** partition matrix: the panel
    serves one flip window later than the tenure pair, so a date that is ready
    for a sibling can be outside the panel's own range.

    No sensor gates the SDE snapshot the panel reads: it is a date-independent
    tree whose absence is a configuration error the build surfaces, and a
    stale-but-present one is a fingerprint recorded in ``_INDEX.json``, never a
    run this sensor triggers.
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
        report = corpus.gold_ready_dates(dataset, derivative=derivative)
        result = request_partitions(
            context,
            reported=report.get("ready", []),
            valid=set(partitions.get_partition_keys()),
            run_key_prefix=f"{derivative}-gold",
            asset_key=asset.key,
            label="gold-readiness",
        )
        # Gated on the report, not on `run_requests`: a tick whose ready dates
        # were all held back by the in-flight guard has no date blocked, and
        # saying so would send the operator after a tree that is fine.
        if report.get("ready"):
            return result
        reason = _blocked_skip_reason(report.get("blocked", []))
        if reason is None:
            return result
        # `skip_reason` is only valid on an empty request list, which is why it
        # is set here and not inside the shared `request_partitions` tail.
        return dg.SensorResult(
            run_requests=[], skip_reason=reason, cursor=result.cursor
        )

    return _sensor


sovereignty_ownership_gold_sensor = _build_sovereignty_gold_sensor(
    sm.DATASET,
    sm.OWNERSHIP_DERIVATIVE,
    sm.sovereignty_ownership_gold,
    sm.ownership_gold_partitions,
)
sovereignty_changes_gold_sensor = _build_sovereignty_gold_sensor(
    sm.DATASET,
    sm.CHANGES_DERIVATIVE,
    sm.sovereignty_changes_gold,
    sm.changes_gold_partitions,
)
sovereignty_adm_gold_sensor = _build_sovereignty_gold_sensor(
    ss.DATASET,
    ss.ADM_DERIVATIVE,
    ss.sovereignty_adm_gold,
    ss.adm_gold_partitions,
)
sovereignty_contests_gold_sensor = _build_sovereignty_gold_sensor(
    sc.DATASET,
    sc.CONTESTS_DERIVATIVE,
    sc.sovereignty_contests_gold,
    sc.contests_gold_partitions,
)
sovereignty_panel_gold_sensor = _build_sovereignty_gold_sensor(
    sm.DATASET,
    sm.PANEL_DERIVATIVE,
    sm.sovereignty_panel_gold,
    sm.panel_gold_partitions,
)


# --- public-contracts (history tier, corpus ADR-0068) ---------------------
#
# One availability sensor for Silver and one readiness sensor per Gold
# derivative: the dataset now declares four of them (corpus's
# `public-contracts-gold` row, ADR-0068), each its own tree, its own `_DONE`
# and its own run-state row, so each is polled with its own `--derivative`.
# Availability is the same thin cap-and-dedup loop as every other Silver
# sensor — the missing set comes from
# corpus run-state, never from listing the NAS tree, and the `everef_download`
# pool on the asset throttles the fetches across every launch path, so no
# sensor tag is set here.
#
# It covers the trailing edge only. The 1 892-day history behind it is an
# operator backfill (ADR-0068 consequences: ~8.2 h at the politeness limit),
# not this sensor's job: widening the per-tick cap to make it one would put an
# 8-hour queue behind a 10-partition tick budget shared with every other
# dataset. The live twin has no availability to diff and is driven by
# `public_contracts_live_schedule` instead.


@dg.sensor(
    target=pc.public_contracts_silver,
    minimum_interval_seconds=3600,
    default_status=dg.DefaultSensorStatus.STOPPED,
)
def public_contracts_availability_sensor(
    context: dg.SensorEvaluationContext, corpus: CorpusResource
) -> dg.SensorResult:
    """Requests Silver runs for public-contracts dates newly available upstream."""
    report = corpus.everef_missing_partitions(pc.DATASET)
    return request_partitions(
        context,
        reported=report.get("missing", []),
        valid=set(pc.silver_partitions.get_partition_keys()),
        run_key_prefix=f"{pc.DATASET}-silver",
        asset_key=pc.public_contracts_silver.key,
        label="availability",
    )


def _build_public_contracts_gold_sensor(
    derivative: str,
    asset: dg.AssetsDefinition,
    partitions: dg.DailyPartitionsDefinition,
) -> dg.SensorDefinition:
    """Builds a Gold readiness sensor for one public-contracts derivative.

    One dataset, four derivatives, so the factory takes only the derivative;
    unlike the sovereignty family the poll target is the module constant.

    Polls ``corpus gold ready-dates --derivative <derivative>``, which answers
    from the run-state ``partitions`` table — the day's Silver recorded built
    and that derivative's Gold not yet built — never from listing the tree.
    Each of the four folds the target day's Silver alone and holds no cross-day
    state (ADR-0068 decision 5), so readiness is that one day's Silver and
    nothing behind it; no window coverage is evaluated here or asked for.

    Each derivative validates against **its own** partition matrix. The four
    share a served start today, but each resolves the one its own configuration
    declares, so a date reported ready for a sibling that moved its start is
    still not proposed for a derivative that does not have that key.
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
        report = corpus.gold_ready_dates(pc.DATASET, derivative=derivative)
        return request_partitions(
            context,
            reported=report.get("ready", []),
            valid=set(partitions.get_partition_keys()),
            run_key_prefix=f"{derivative}-gold",
            asset_key=asset.key,
            label="gold-readiness",
        )

    return _sensor


contracts_facts_gold_sensor = _build_public_contracts_gold_sensor(
    pc.CONTRACTS_FACTS_DERIVATIVE,
    pc.contracts_facts_gold,
    pc.contracts_facts_gold_partitions,
)
contracts_item_facts_gold_sensor = _build_public_contracts_gold_sensor(
    pc.CONTRACTS_ITEM_FACTS_DERIVATIVE,
    pc.contracts_item_facts_gold,
    pc.contracts_item_facts_gold_partitions,
)
contracts_item_prices_gold_sensor = _build_public_contracts_gold_sensor(
    pc.CONTRACTS_ITEM_PRICES_DERIVATIVE,
    pc.contracts_item_prices_gold,
    pc.contracts_item_prices_gold_partitions,
)
contracts_courier_rates_gold_sensor = _build_public_contracts_gold_sensor(
    pc.CONTRACTS_COURIER_RATES_DERIVATIVE,
    pc.contracts_courier_rates_gold,
    pc.contracts_courier_rates_gold_partitions,
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


# Committed SDE partitions, by tier. Both trees key run-state on ``build=<n>``
# (corpus `sde.rs`: Silver commits `dataset = 'sde'`, the changelog commits
# `dataset = 'sde-changelog'`), so one parser serves both.
_SDE_SILVER_BUILDS_SQL = (
    "SELECT DISTINCT partition_key FROM partitions "
    "WHERE tier = 'silver' AND dataset = 'sde'"
)
_SDE_CHANGELOG_BUILDS_SQL = (
    "SELECT DISTINCT partition_key FROM partitions "
    "WHERE tier = 'gold' AND dataset = 'sde-changelog'"
)


def _committed_builds(corpus: CorpusResource, sql: str) -> set[int]:
    """Build numbers whose run-state partition ``sql`` reports committed."""
    return {
        build
        for row in corpus.state_query(sql)
        if (build := _parse_build_key(row["partition_key"])) is not None
    }


def _registered_builds(context: dg.SensorEvaluationContext) -> set[str]:
    """Registered ``sde_builds`` partition keys, skipping any non-numeric one.

    Every path that orders or compares builds keys on ``int(key)``, and the
    dynamic-partitions store is hand-editable, so one junk key would otherwise
    raise and take the whole sensor tick down. Tolerate it the way
    :func:`_parse_build_key` and :func:`_in_flight_builds` do: skip it, log it,
    and keep serving the valid builds.
    """
    keys = set(
        sde.build_partitions.get_partition_keys(
            dynamic_partitions_store=context.instance
        )
    )
    numeric = {key for key in keys if key.lstrip("-").isdigit()}
    if skipped := keys - numeric:
        context.log.warning(
            "sde: ignoring non-numeric build partition key(s): %s",
            ", ".join(sorted(skipped)),
        )
    return numeric


def _in_flight_builds(context: dg.SensorEvaluationContext) -> set[int]:
    """SDE build numbers with a queued or running ``sde_silver`` run.

    Best-effort, like the guard it reuses: a context without run storage reports
    nothing in flight, and the stale term then covers what the deferral would
    have prevented.
    """
    in_flight = sensor_util._in_flight_partitions(context, sde.sde_silver.key)  # noqa: SLF001
    return {int(key) for key in in_flight if key.isdigit()}


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
    partition matrix is dynamic and grows here.

    Readiness is the corpus run-state, not "not yet registered". A build is
    **eligible** when it is registered *or* discovered on this tick and its ``sde``
    Silver partition (``dataset = 'sde'``) is not committed. Registering a key is
    not evidence that its ingest succeeded: keyed on first sighting, a failed or
    green-no-op ingest was never asked for again, and the resulting hole in the
    build sequence was permanent — the changelog binary diffs against the largest
    *committed* Silver below its target, so every later build then diffs across
    the hole. Including already-registered builds is what makes a failed ingest
    retried and the hole heal on a later tick; a build whose Silver is committed
    drops out on its own, so the retries are self-limiting.

    The eligible set goes through :func:`request_partitions` for the rotating,
    retry-safe ``run_key`` and the in-flight guard, with ``sort_key=int`` because
    build keys are numbers and the helper's default sort is lexical, where ``"99"``
    lands after ``"100"``. The helper sets no ``dynamic_partitions_requests`` and
    this sensor is the only writer of ``sde.build_partitions``, so its result is
    reassembled here with an add-request for the newly discovered keys. Dagster
    evaluates run requests with those additions applied, so a build discovered on
    this tick can also be requested on it.

    ``release_date`` is a label, never a key: it is logged with the build and
    attached to the run request as the ``sde.RELEASE_DATE_TAG`` tag so a build that
    keeps failing is visible rather than silent. It comes from the ``everef list``
    payload this sensor already fetches, never from ``done_path``. Only the builds
    actually **requested** on the tick are logged, not the whole eligible set: a
    run-state reset or a wide backfill leaves hundreds eligible, and the per-tick
    cap already reports how many were held back.
    """
    builds = corpus.everef_list_builds(sde.DATASET)
    discovered = {str(int(row["build"])) for row in builds}
    release_dates = {
        str(int(row["build"])): date
        for row in builds
        if (date := row.get("release_date"))
    }

    registered = _registered_builds(context)
    committed = {
        str(build) for build in _committed_builds(corpus, _SDE_SILVER_BUILDS_SQL)
    }
    known = registered | discovered
    eligible = known - committed
    new_keys = sorted(discovered - registered, key=int)

    result = request_partitions(
        context,
        reported=eligible,
        valid=known,
        run_key_prefix=f"{sde.DATASET}-silver",
        asset_key=sde.sde_silver.key,
        label="sde-discovery",
        sort_key=int,
    )
    run_requests = []
    for request in result.run_requests:
        key = request.partition_key
        date = release_dates.get(key)
        if date is None:
            context.log.info("sde-discovery: requesting ingest for build %s", key)
        else:
            context.log.info(
                "sde-discovery: requesting ingest for build %s (released %s)",
                key,
                date,
            )
        run_requests.append(
            dg.RunRequest(
                run_key=request.run_key,
                partition_key=key,
                tags={sde.RELEASE_DATE_TAG: date} if date is not None else None,
            )
        )
    return dg.SensorResult(
        run_requests=run_requests,
        cursor=result.cursor,
        dynamic_partitions_requests=(
            [sde.build_partitions.build_add_request(new_keys)] if new_keys else []
        ),
    )


@dg.sensor(
    target=sde.sde_changelog_gold,
    minimum_interval_seconds=3600,
    default_status=dg.DefaultSensorStatus.STOPPED,
)
def sde_gold_sensor(
    context: dg.SensorEvaluationContext, corpus: CorpusResource
) -> dg.SensorResult:
    """Requests the unified changelog for the builds that still owe one.

    There is no ``ready-dates`` for SDE (no coverage window); readiness is keyed
    on corpus run-state (ADR-0032). A build is **outstanding** when its unified
    Silver partition (``dataset = 'sde'``) is committed and its changelog Gold
    partition (``dataset = 'sde-changelog'``) is not. Subtracting the Gold before
    the per-tick cap is the whole point: cap the committed set instead and the
    oldest builds hold every slot for good, so no build past the cap is ever
    requested.

    The **baseline** build is left out. The binary's predecessor rule is "largest
    committed Silver build < target", so the lowest committed build has none: it
    is skipped, writes nothing, and can therefore never leave the outstanding set
    on its own. The binary still owns that decision — the sensor only declines to
    queue a run it knows is a no-op, the pre-check the Gold gate already does.

    The remainder goes through :func:`request_partitions` for the rotating,
    retry-safe ``run_key`` and the in-flight guard, so a changelog run that failed
    or finished as a green no-op (``output_required=False``) is asked for again on
    the next tick. ``sort_key=int`` because build keys are numbers: the helper's
    default sort is lexical, where ``"99"`` lands after ``"100"``.

    A committed changelog is **stale** when it was diffed across a hole: the
    nearest lower committed Silver was committed *after* the changelog was built,
    so the binary would pick a different predecessor now.
    :meth:`CorpusResource.stale_changelog_builds` reports those from run-state and
    they are folded back into the outstanding set, so the changelog is rebuilt in
    place on the next tick — one sensor, one union, because both terms target
    ``sde_changelog_gold``. The full rule is therefore
    ``(committed Silver − committed Gold − baseline) ∪ stale``.

    An outstanding build ``B`` is **deferred** only while a queued or in-flight
    ``sde_silver`` run ``S`` satisfies ``pred(B) < S <= B``, where ``pred(B)`` is
    ``B``'s current predecessor — the largest committed Silver build below ``B``.
    Such an ``S`` is exactly a run that would change what ``corpus gold build``
    reads underneath ``B``: strictly between, it would become the predecessor; at
    ``B`` itself, a re-ingest is rewriting the very Silver partition the changelog
    diffs *from*, which the per-asset in-flight guard does not catch because it
    guards ``sde_changelog_gold`` only. A run at or below that predecessor cannot
    become the predecessor and does not touch ``B``, so it must not defer. The
    broad form ("defer every outstanding build above the lowest in-flight run")
    would let a single permanently-failing ingest, re-requested every discovery
    tick and often sitting QUEUED behind the ``everef_download`` pool, silence the
    whole changelog stream. Deferral is applied before the per-tick cap so a
    deferred build does not consume a slot, and the build stays outstanding, so the
    wait is bounded by a run's lifetime. Readiness deliberately does not depend on
    the registered build sequence — a build that never ingests would then stall
    every changelog above it for good (see design.md).

    The snapshot is not driven here — it is a non-partitioned, latest-only asset
    on :data:`sde_snapshot_schedule`.
    """
    committed = _committed_builds(corpus, _SDE_SILVER_BUILDS_SQL)
    built = _committed_builds(corpus, _SDE_CHANGELOG_BUILDS_SQL)
    baseline = {min(committed)} if committed else set()
    stale = set(corpus.stale_changelog_builds())
    if stale:
        context.log.info(
            "sde-gold: %d changelog(s) diffed across a hole, rebuilding: %s",
            len(stale),
            ", ".join(str(build) for build in sorted(stale)),
        )
    outstanding = (committed - built - baseline) | stale

    silver_in_flight = _in_flight_builds(context)
    if silver_in_flight and outstanding:
        deferred: set[int] = set()
        blocking: set[int] = set()
        for build in outstanding:
            predecessor = max((c for c in committed if c < build), default=None)
            blockers = {
                run
                for run in silver_in_flight
                if run <= build and (predecessor is None or run > predecessor)
            }
            if blockers:
                deferred.add(build)
                blocking |= blockers
        if deferred:
            context.log.info(
                "sde-gold: deferring %s while Silver build(s) %s are in flight",
                ", ".join(str(build) for build in sorted(deferred)),
                ", ".join(str(build) for build in sorted(blocking)),
            )
        outstanding = outstanding - deferred

    return request_partitions(
        context,
        reported=[str(build) for build in outstanding],
        valid=_registered_builds(context),
        run_key_prefix=sde.CHANGELOG_DERIVATIVE,
        asset_key=sde.sde_changelog_gold.key,
        label="sde-gold",
        sort_key=int,
    )


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


# Half-hourly refresh of the live open-contract snapshot (corpus ADR-0068). Same
# schedule-not-sensor rationale as `market_orders_live_schedule`: the tree is a
# current-overwrite `current/` partition, so there is no per-date availability to
# diff — only "grab whatever EVE Ref published last". The cadence matches the
# upstream ~30-min publish rhythm (~47 snapshots a day). The source is EVE Ref, so
# the asset joins the `everef_download` pool (one fetch per run), not `heavy`, and
# cannot starve the windowed backfills under max_concurrent_runs. Offset to
# :15/:45 rather than :00/:30: only the half-hourly cadence matters, since the
# tree is last-write-wins with no retention to protect, and upstream publishes
# on no fixed offset anyway — so the offset is free to spend avoiding the
# minute-:00/:30 pile-up with `market_orders_live_schedule` (`*/30`) and, at the
# hour boundary, `market_prices_live_schedule` and
# `industry_cost_indices_live_schedule` (both `0 * * * *`).
public_contracts_live_schedule = dg.ScheduleDefinition(
    name="public_contracts_live_schedule",
    target=pcl.public_contracts_live_gold,
    cron_schedule="15,45 * * * *",
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


# Daily refresh of the live LP store offers (corpus ADR-0070). Same schedule-not-
# sensor rationale as the live siblings — no per-date matrix to diff, only
# "overwrite current/ with what ESI serves now" — but daily rather than hourly,
# and that is measured, not inherited: on 2026-09-02 all 283 stores returned
# `Expires: 11:05:00 UTC` the following day, so the caches roll together once a
# day at 11:05. 11:30 is comfortably past the roll and fetches one clean
# generation. Hourly would repeat the 284-request fan-out against a payload that
# only moves on deployments. One schedule for one asset, which writes both Gold
# trees; the source is ESI, so no `everef_download` pool and no memory pool —
# the global cap alone.
lp_store_offers_live_schedule = dg.ScheduleDefinition(
    name="lp_store_offers_live_schedule",
    target=lpsol.lp_store_offers_live_gold,
    cron_schedule="30 11 * * *",
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
# matrix to diff. Late UTC evening, after CCP's publishing day, staggered an hour
# apart. STOPPED by default. Neither fetch asset joins a pool (it hits neither
# EVE Ref nor ESI), so the fetch step obeys only the global concurrency cap; the
# embed step further down each group's chain joins `heavy` instead. The
# historical sweeps run via the manually-triggered `news_backfill_job` /
# `transcripts_backfill_job`, not here.
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
    cron_schedule="10 22 * * *",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)


# transcripts now carries a full Silver/Gold chain (ADR-0055), so its schedule
# targets the whole `transcripts` group — fetch -> ingest -> videos/sections/
# entity-mentions Gold (+ embeddings) — in one run, in dependency order, exactly
# like `news_daily_schedule`. The embed step shares the `heavy` limit-1 pool with
# news-embeddings and with every windowed Gold build, so the two schedules fire a
# full hour apart rather than 30 min. With `granularity: run`, the whole group
# run — fetch, ingest, the Gold builds and the embed step — holds `heavy` for
# its duration, not just the embed step, so the hour is not guaranteed to clear
# it; the stagger only makes it likely. If it doesn't, the other schedule's run
# queues rather than failing, which is an acceptable cost, not a cadence bug.
# Annotations are NOT in this
# group's scheduled chain: `transcripts-annotations` is a manual operator run via the
# `annotate-transcripts` skill (contract `t2`), never a Dagster asset.
transcripts_daily_schedule = dg.ScheduleDefinition(
    name="transcripts_daily_schedule",
    target=dg.AssetSelection.groups("transcripts"),
    cron_schedule="10 23 * * *",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)
