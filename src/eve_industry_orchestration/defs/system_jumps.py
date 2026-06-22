"""system-jumps: the first multi-derivative dataset (ADR-0025).

One hourly, day-partitioned Silver source feeds two Gold derivatives, each its
own Hive tree (``gold/<derivative>/...``):

- ``system-traffic-history`` (``flat-multi-horizon``) — a backfillable historical
  matrix: a daily-partitioned Gold asset driven by a ``ready-dates`` sensor,
  exactly like market-history's Gold but passing ``--derivative``.
- ``system-traffic-recent`` (``recency-weighted`` EWMA) — a point-in-time
  "navigate-now" signal: a single non-partitioned asset a schedule rematerialises
  against the latest buildable date. Backfilling a past heat value is misleading,
  so there is no partition matrix and no sensor.

Each asset is a thin shim over the ``corpus`` binary; the binary owns the compute,
the coverage gate, and the ``parquet + _INDEX.json + _DONE`` contract. Partition
starts come from the corpus dataset config (see :mod:`config`), never hardcoded.

**Gold verify keys on the derivative name, not the dataset.** ``corpus gold
build`` writes under ``gold/<derivative>/...`` and ``corpus verify --tier gold``
resolves ``gold/<--dataset>/...``, so Gold verify passes the *derivative* name as
``--dataset``. Silver verify still uses the dataset name.
"""

import dagster as dg

from eve_industry_orchestration.defs.config import resolve_partition_starts
from eve_industry_orchestration.defs.corpus_resource import CorpusResource

DATASET = "system-jumps"
HISTORY_DERIVATIVE = "system-traffic-history"
RECENT_DERIVATIVE = "system-traffic-recent"

# Silver is shared by both derivatives; its start is the earliest preload
# (system-traffic-history's 365d window before 2021-01-01 → 2020-01-02). Gold
# starts at the history derivative's served_start (2021-01-01). The recent
# derivative has no served_start and is non-partitioned, so it needs no start.
_history_starts = resolve_partition_starts(DATASET, HISTORY_DERIVATIVE)
silver_partitions = dg.DailyPartitionsDefinition(start_date=_history_starts.silver)
traffic_history_partitions = dg.DailyPartitionsDefinition(
    start_date=_history_starts.gold
)

_SILVER_POOL = "everef_download"
_GOLD_POOL = "gold_heavy"


@dg.asset(
    partitions_def=silver_partitions,
    group_name="system_jumps",
    kinds={"corpus"},
    pool=_SILVER_POOL,
)
def system_jumps_silver(
    context: dg.AssetExecutionContext, corpus: CorpusResource
) -> dg.MaterializeResult:
    """Silver partition: ingest one day's hourly snapshots, then verify."""
    date = context.partition_key
    corpus.run(
        context,
        "ingest",
        "--dataset",
        DATASET,
        "--date",
        date,
        "--sink-path",
        corpus.sink_path,
    )
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
    return dg.MaterializeResult(
        metadata={"dataset": DATASET, "tier": "silver", "partition": date}
    )


@dg.asset(
    partitions_def=traffic_history_partitions,
    deps=[system_jumps_silver],
    group_name="system_jumps",
    kinds={"corpus"},
    pool=_GOLD_POOL,
)
def system_jumps_traffic_history_gold(
    context: dg.AssetExecutionContext, corpus: CorpusResource
) -> dg.MaterializeResult:
    """Gold partition for the flat-multi-horizon derivative, then verify.

    ``deps=`` is lineage only; the readiness sensor drives this. ``corpus gold
    build`` reads the full ``[date - 365d, date]`` Silver window and enforces
    ``coverage_min_ratio`` itself — an incomplete window exits non-zero. Verify
    keys on the derivative name (its Gold tree), not the dataset.
    """
    date = context.partition_key
    corpus.run(
        context,
        "gold",
        "build",
        "--dataset",
        DATASET,
        "--derivative",
        HISTORY_DERIVATIVE,
        "--date",
        date,
        "--sink-path",
        corpus.sink_path,
    )
    corpus.run(
        context,
        "verify",
        "--dataset",
        HISTORY_DERIVATIVE,
        "--date",
        date,
        "--tier",
        "gold",
        "--sink-path",
        corpus.sink_path,
    )
    return dg.MaterializeResult(
        metadata={
            "dataset": DATASET,
            "derivative": HISTORY_DERIVATIVE,
            "tier": "gold",
            "partition": date,
        }
    )


@dg.asset(
    group_name="system_jumps",
    kinds={"corpus"},
)
def system_jumps_traffic_recent(
    context: dg.AssetExecutionContext, corpus: CorpusResource
) -> dg.MaterializeResult:
    """Non-partitioned EWMA "navigate-now" heat for the latest buildable date.

    Resolves the latest date itself (``corpus gold ready-dates`` → ``max(ready)``)
    and builds only that date. No ``deps=`` partition chain and no ``gold_heavy``
    pool: the EWMA build spans only the short warmup, so it is lightweight, and
    keeping it out of the heavy pool stops the hourly schedule from starving the
    365d ``system-traffic-history`` backfills.
    """
    report = corpus.gold_ready_dates(DATASET, derivative=RECENT_DERIVATIVE)
    ready = report.get("ready", [])
    if not ready:
        context.log.info(
            "system-traffic-recent: no buildable date (ready empty); skipping"
        )
        return dg.MaterializeResult(
            metadata={
                "dataset": DATASET,
                "derivative": RECENT_DERIVATIVE,
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
        RECENT_DERIVATIVE,
        "--date",
        latest,
        "--sink-path",
        corpus.sink_path,
    )
    corpus.run(
        context,
        "verify",
        "--dataset",
        RECENT_DERIVATIVE,
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
            "derivative": RECENT_DERIVATIVE,
            "tier": "gold",
            "built": True,
            "partition": latest,
        }
    )
