"""system-kills: one hourly Silver source, six per-measure Gold derivatives (ADR-0037).

Packaging is identical to system-jumps (per-year JSON tars + per-date folders from
2022-12-16); a partition is one DAY of merged hourly snapshots. The payload
carries three measures — ``ship_kills`` (PvP danger), ``npc_kills`` (PvE activity),
``pod_kills`` (capsule losses) — kept verbatim in source-faithful Silver
(ADR-0007).

Gold fans the three measures into six single-measure trees (ADR-0037), each its
own Hive tree (``gold/<derivative>/...``):

- ``system-kills-{ship,npc,pod}-history`` (``kills-flat`` shape) — a backfillable
  historical matrix per measure: a daily-partitioned Gold asset driven by a
  ``ready-dates`` sensor, exactly like system-jumps's history Gold but passing
  ``--derivative``.
- ``system-kills-{ship,npc,pod}-recent`` (``kills-recent`` EWMA shape) — a
  point-in-time "danger-now" signal per measure: a single non-partitioned asset a
  schedule rematerialises against the latest buildable date. Backfilling a past
  heat value is misleading, so there is no partition matrix and no sensor.

Each asset is a thin shim over the ``corpus`` binary; the binary owns the compute,
the coverage gate, the EWMA decay, and the ``parquet + _INDEX.json + _DONE``
contract. Partition starts come from the corpus dataset config (see
:mod:`config`), never hardcoded.

**Gold verify keys on the derivative name, not the dataset.** ``corpus gold
build`` writes under ``gold/<derivative>/...`` and ``corpus verify --tier gold``
resolves ``gold/<--dataset>/...``, so Gold verify passes the *derivative* name as
``--dataset``. Silver verify still uses the dataset name.
"""

from collections.abc import Iterator

import dagster as dg

from eve_industry_orchestration.defs.config import resolve_partition_starts
from eve_industry_orchestration.defs.corpus_resource import CorpusResource

DATASET = "system-kills"

# One history + one recent derivative per measure (ADR-0037). The tuples drive the
# asset/sensor/schedule factories below so the six trees stay 1:1 with the YAML.
MEASURES = ("ship", "npc", "pod")
HISTORY_DERIVATIVES = tuple(f"system-kills-{m}-history" for m in MEASURES)
RECENT_DERIVATIVES = tuple(f"system-kills-{m}-recent" for m in MEASURES)

# Silver is shared by all six derivatives. Its derived start is the earliest
# preload (each history derivative's 365d window before served_start 2022-01-01 →
# 2021-01-01), but EVE Ref's dense hourly era begins 2021-07-01 (2020/early-2021
# has no archive), so the dataset YAML's silver.served_start (ADR-0027) clamps
# Silver up to 2021-07-01. Gold (history) starts at 2022-01-01. The recent
# derivatives have no served_start and are non-partitioned, so they need no start.
_history_starts = resolve_partition_starts(DATASET, HISTORY_DERIVATIVES[0])
if _history_starts.gold is None:  # history derivatives declare served_start; narrow
    raise ValueError(
        f"{DATASET}/{HISTORY_DERIVATIVES[0]} resolved no Gold served_start; "
        "a kills-flat derivative must declare one"
    )
silver_partitions = dg.DailyPartitionsDefinition(start_date=_history_starts.silver)
history_gold_partitions = dg.DailyPartitionsDefinition(start_date=_history_starts.gold)

_SILVER_POOL = "everef_download"
_GOLD_POOL = "heavy"


def _derivative_to_asset_name(derivative: str) -> str:
    """``system-kills-ship-history`` → ``system_kills_ship_history_gold``."""
    return f"{derivative.replace('-', '_')}_gold"


@dg.asset(
    partitions_def=silver_partitions,
    group_name="system_kills",
    kinds={"corpus"},
    pool=_SILVER_POOL,
    # An interior upstream-gap day (EVE Ref published nothing, ADR-0028) skips:
    # corpus exits 0 with status "skipped" and writes no partition, so the asset
    # must complete without materialising — the partition stays Missing.
    output_required=False,
)
def system_kills_silver(
    context: dg.AssetExecutionContext, corpus: CorpusResource
) -> Iterator[dg.MaterializeResult | dg.AssetObservation]:
    """Silver partition: ingest one day's hourly snapshots, then verify.

    A genuinely-absent upstream day (corpus reports ``status: skipped``) is left
    Missing: the verify (which would 404 on the absent partition) is skipped and
    an ``AssetObservation`` records why, instead of a misleading materialisation.
    """
    date = context.partition_key
    status = corpus.run(
        context,
        "ingest",
        "--dataset",
        DATASET,
        "--date",
        date,
        "--sink-path",
        corpus.sink_path,
    )
    if status is not None and status.get("status") == "skipped":
        context.log.info(
            "system-kills %s: upstream absent, leaving partition missing", date
        )
        yield dg.AssetObservation(
            asset_key=context.asset_key,
            partition=date,
            metadata={
                "skip_reason": "upstream_absent",
                "detail": str(status.get("reason", "")),
            },
        )
        return
    corpus.run(
        context,
        "verify",
        "--dataset",
        DATASET,
        "--date",
        date,
        "--tier",
        "silver",
        "--sink-path",
        corpus.sink_path,
    )
    yield dg.MaterializeResult(
        metadata={"dataset": DATASET, "tier": "silver", "partition": date}
    )


def _build_history_asset(derivative: str) -> dg.AssetsDefinition:
    """Builds a daily-partitioned kills-flat history Gold asset for one measure.

    ``deps=`` is lineage only; the readiness sensor drives this. ``corpus gold
    build`` reads the full ``[date - 365d, date]`` Silver window and enforces
    ``coverage_min_ratio`` itself. Verify keys on the derivative name (its own
    ``gold/<derivative>/...`` tree), not the dataset.
    """

    @dg.asset(
        name=_derivative_to_asset_name(derivative),
        partitions_def=history_gold_partitions,
        deps=[system_kills_silver],
        group_name="system_kills",
        kinds={"corpus"},
        pool=_GOLD_POOL,
        # A target day whose Silver is an upstream gap can never build a Gold row
        # (ADR-0029); corpus reports "skipped", so the asset completes without
        # materialising — the partition stays Missing rather than failing.
        output_required=False,
    )
    def _gold(
        context: dg.AssetExecutionContext, corpus: CorpusResource
    ) -> Iterator[dg.MaterializeResult | dg.AssetObservation]:
        date = context.partition_key
        status = corpus.run(
            context,
            "gold",
            "build",
            "--dataset",
            DATASET,
            "--derivative",
            derivative,
            "--date",
            date,
            "--sink-path",
            corpus.sink_path,
        )
        if status is not None and status.get("status") == "skipped":
            context.log.info(
                "%s %s: target silver is an upstream gap, leaving partition missing",
                derivative,
                date,
            )
            yield dg.AssetObservation(
                asset_key=context.asset_key,
                partition=date,
                metadata={
                    "skip_reason": "upstream_gap",
                    "detail": str(status.get("reason", "")),
                },
            )
            return
        corpus.run(
            context,
            "verify",
            "--dataset",
            derivative,
            "--date",
            date,
            "--tier",
            "gold",
            "--sink-path",
            corpus.sink_path,
        )
        yield dg.MaterializeResult(
            metadata={
                "dataset": DATASET,
                "derivative": derivative,
                "tier": "gold",
                "partition": date,
            }
        )

    return _gold


def _build_recent_asset(derivative: str) -> dg.AssetsDefinition:
    """Builds a non-partitioned kills-recent EWMA "danger-now" Gold asset.

    Resolves the latest date itself (``corpus gold ready-dates`` → ``max(ready)``)
    and builds only that date. ``deps=`` carries lineage only (the EWMA reads the
    shared Silver tree); a non-partitioned asset cannot chain partitions, so the
    schedule — not Silver — drives it. No ``heavy`` pool: the EWMA build spans
    only the short warmup, so it is lightweight, and keeping it out of the heavy
    pool stops the hourly schedules from starving the 365d history backfills.
    """

    @dg.asset(
        name=_derivative_to_asset_name(derivative),
        deps=[system_kills_silver],
        group_name="system_kills",
        kinds={"corpus"},
    )
    def _gold(
        context: dg.AssetExecutionContext, corpus: CorpusResource
    ) -> dg.MaterializeResult:
        report = corpus.gold_ready_dates(DATASET, derivative=derivative)
        ready = report.get("ready", [])
        if not ready:
            context.log.info(
                "%s: no buildable date (ready empty); skipping", derivative
            )
            return dg.MaterializeResult(
                metadata={
                    "dataset": DATASET,
                    "derivative": derivative,
                    "tier": "gold",
                    "built": False,
                }
            )

        latest = max(ready)
        corpus.run(
            context,
            "gold",
            "build",
            "--dataset",
            DATASET,
            "--derivative",
            derivative,
            "--date",
            latest,
            "--sink-path",
            corpus.sink_path,
        )
        corpus.run(
            context,
            "verify",
            "--dataset",
            derivative,
            "--date",
            latest,
            "--tier",
            "gold",
            "--sink-path",
            corpus.sink_path,
        )
        return dg.MaterializeResult(
            metadata={
                "dataset": DATASET,
                "derivative": derivative,
                "tier": "gold",
                "built": True,
                "partition": latest,
            }
        )

    return _gold


# One module-level asset per derivative so auto-discovery registers all six.
system_kills_ship_history_gold = _build_history_asset(HISTORY_DERIVATIVES[0])
system_kills_npc_history_gold = _build_history_asset(HISTORY_DERIVATIVES[1])
system_kills_pod_history_gold = _build_history_asset(HISTORY_DERIVATIVES[2])
system_kills_ship_recent_gold = _build_recent_asset(RECENT_DERIVATIVES[0])
system_kills_npc_recent_gold = _build_recent_asset(RECENT_DERIVATIVES[1])
system_kills_pod_recent_gold = _build_recent_asset(RECENT_DERIVATIVES[2])

HISTORY_GOLD_ASSETS = (
    system_kills_ship_history_gold,
    system_kills_npc_history_gold,
    system_kills_pod_history_gold,
)
RECENT_GOLD_ASSETS = (
    system_kills_ship_recent_gold,
    system_kills_npc_recent_gold,
    system_kills_pod_recent_gold,
)
