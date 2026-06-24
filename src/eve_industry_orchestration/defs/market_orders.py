"""market-orders: full-orderbook Silver and the orderbook-aggregate Gold (ADR-0033).

A day-partitioned Silver source — the day's ~30-min ``*.v3.csv.bz2`` snapshots of
the full k-space orderbook, merged into one Silver stream — feeds a single Gold
derivative, ``orderbook-sweep`` (``orderbook-aggregate`` shape): one row per
``(snapshot_at, region_id, type_id)`` aggregate plus the cross-snapshot activity
delta (filled/cancelled/expired/new/partial-filled/modified) against the
immediately preceding snapshot.

Each asset is a thin shim over the ``corpus`` binary; the binary owns the
compute, the k-space filter, the delta classifier, and the ``parquet +
_INDEX.json + _DONE`` contract. Partition starts come from the corpus dataset
config (see :mod:`config`), never hardcoded.

**Gold verify keys on the derivative name, not the dataset.** ``corpus gold
build`` writes under ``gold/orderbook-sweep/...`` and ``corpus verify --tier
gold`` resolves ``gold/<--dataset>/...``, so Gold verify passes the *derivative*
name as ``--dataset``. Silver verify still uses the dataset name. The derivative
name differs from the dataset name (like system-jumps), so every Gold call passes
``--derivative`` explicitly.
"""

from collections.abc import Iterator

import dagster as dg

from eve_industry_orchestration.defs.config import resolve_partition_starts
from eve_industry_orchestration.defs.corpus_resource import CorpusResource

DATASET = "market-orders"
GOLD_DERIVATIVE = "orderbook-sweep"

# Silver and Gold share a served floor (2021-07-09, the first full-cadence day);
# the orderbook delta engine looks back exactly one snapshot, so the Silver
# preload is one day before the Gold start but clamps back up to the
# silver.served_start floor (ADR-0027/0033) — both tiers land on 2021-07-09.
_starts = resolve_partition_starts(DATASET, GOLD_DERIVATIVE)
if _starts.gold is None:  # orderbook-sweep declares a served_start; narrow for typing
    raise ValueError(
        f"{DATASET}/{GOLD_DERIVATIVE} resolved no Gold served_start; "
        "the orderbook-aggregate derivative must declare one"
    )
silver_partitions = dg.DailyPartitionsDefinition(start_date=_starts.silver)
gold_partitions = dg.DailyPartitionsDefinition(start_date=_starts.gold)

# A Silver day is the whole k-space orderbook (~835 MiB/day compressed input);
# the `everef_download` pool caps concurrent upstream fetches across every launch
# path. A Gold build reads a day plus one tail snapshot and is the heaviest build
# in the workspace, so it joins the shared `gold_heavy` memory pool. Both limits
# live in deploy/dagster.yaml.
_SILVER_POOL = "everef_download"
_GOLD_POOL = "gold_heavy"


@dg.asset(
    partitions_def=silver_partitions,
    group_name="market_orders",
    kinds={"corpus"},
    pool=_SILVER_POOL,
    # A day EVE Ref never published (ADR-0028) skips: corpus exits 0 with status
    # "skipped" and writes no partition, so the asset must complete without
    # materialising — the partition stays Missing rather than failing.
    output_required=False,
)
def market_orders_silver(
    context: dg.AssetExecutionContext, corpus: CorpusResource
) -> Iterator[dg.MaterializeResult | dg.AssetObservation]:
    """Silver partition: ingest one day's orderbook snapshots, then verify.

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
            "market-orders %s: upstream absent, leaving partition missing", date
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


@dg.asset(
    partitions_def=gold_partitions,
    deps=[market_orders_silver],
    group_name="market_orders",
    kinds={"corpus"},
    pool=_GOLD_POOL,
    # A target day whose Silver is an upstream gap can never build a Gold row
    # (ADR-0029); corpus reports "skipped", so the asset must complete without
    # materialising — the partition stays Missing rather than failing.
    output_required=False,
)
def market_orders_gold(
    context: dg.AssetExecutionContext, corpus: CorpusResource
) -> Iterator[dg.MaterializeResult | dg.AssetObservation]:
    """Gold partition for the orderbook-aggregate derivative, then verify.

    ``deps=`` is lineage only; the readiness sensor drives this. ``corpus gold
    build`` reads the target day plus the prior day's tail snapshot and emits the
    per-snapshot aggregate + activity delta. Verify keys on the derivative name
    (its own ``gold/orderbook-sweep/...`` tree), not the dataset.

    A target day that is a recorded upstream gap (``status: skipped``, ADR-0029)
    is left Missing: the verify (which would 404 on the absent Gold partition) is
    skipped and an ``AssetObservation`` records why, mirroring the Silver asset.
    """
    date = context.partition_key
    status = corpus.run(
        context,
        "gold",
        "build",
        "--dataset",
        DATASET,
        "--derivative",
        GOLD_DERIVATIVE,
        "--date",
        date,
        "--sink-path",
        corpus.sink_path,
    )
    if status is not None and status.get("status") == "skipped":
        context.log.info(
            "orderbook-sweep %s: target silver is an upstream gap, "
            "leaving partition missing",
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
        GOLD_DERIVATIVE,
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
            "derivative": GOLD_DERIVATIVE,
            "tier": "gold",
            "partition": date,
        }
    )
