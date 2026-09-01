"""market-orders: full-orderbook Silver and the three split Gold derivatives.

A day-partitioned Silver source — the day's ~30-min ``*.v3.csv.bz2`` snapshots of
the full k-space orderbook, merged into one Silver stream — feeds **three**
independent Gold derivatives, each its own Hive tree (``gold/<derivative>/...``),
split from the single ADR-0033 ``orderbook-sweep`` (ADR-0036, ADR-0042):

- ``market-orders-snapshot`` (``orderbook-aggregate`` shape) — the per-snapshot
  current-prices aggregate (top-of-book, VWAPs, depth, notionals). Pure
  per-snapshot, no activity columns.
- ``market-orders-changes`` (``orderbook-delta`` shape) — the cross-snapshot activity
  changelog (filled/cancelled/expired/new/partial/modified) against the
  immediately preceding snapshot.
- ``market-orders-events`` (``orderbook-events`` shape, ADR-0042) — the
  un-collapsed per-order event log feeding the changes derivative: one row per
  order state-change (create/fill/cancel/expire/partial/modify), keyed by
  ``order_id``. Same one-snapshot look-back and classifier as the changes
  derivative.

All three are daily-partitioned with a one-snapshot look-back (so the planner
loads one day of tail), driven by a ``ready-dates`` sensor, exactly like the prior
single derivative but one asset per tree.

Each asset is a thin shim over the ``corpus`` binary; the binary owns the compute,
the k-space filter, the delta classifier, and the ``parquet + _INDEX.json +
_DONE`` contract. Partition starts come from the corpus dataset config (see
:mod:`config`), never hardcoded.

**Gold verify keys on the derivative name, not the dataset.** ``corpus gold
build`` writes under ``gold/<derivative>/...`` and ``corpus verify --tier gold``
resolves ``gold/<--dataset>/...``, so Gold verify passes the *derivative* name as
``--dataset``. Silver verify still uses the dataset name. The derivative names
differ from the dataset name (like system-jumps), so every Gold call passes
``--derivative`` explicitly.
"""

from collections.abc import Iterator

import dagster as dg

from eve_industry_orchestration.defs.config import resolve_partition_starts
from eve_industry_orchestration.defs.corpus_resource import CorpusResource

DATASET = "market-orders"
SNAPSHOT_DERIVATIVE = "market-orders-snapshot"
CHANGES_DERIVATIVE = "market-orders-changes"
EVENTS_DERIVATIVE = "market-orders-events"
GOLD_DERIVATIVES = (SNAPSHOT_DERIVATIVE, CHANGES_DERIVATIVE, EVENTS_DERIVATIVE)

# All three derivatives share the served floor (2021-07-09, the first
# full-cadence day) and a one-snapshot look-back, so they resolve to the same
# Silver/Gold starts; Silver is shared. Resolve via the snapshot derivative.
_starts = resolve_partition_starts(DATASET, SNAPSHOT_DERIVATIVE)
if _starts.gold is None:  # both derivatives declare a served_start; narrow for typing
    raise ValueError(
        f"{DATASET}/{SNAPSHOT_DERIVATIVE} resolved no Gold served_start; "
        "an orderbook derivative must declare one"
    )
silver_partitions = dg.DailyPartitionsDefinition(start_date=_starts.silver)
gold_partitions = dg.DailyPartitionsDefinition(start_date=_starts.gold)

# market-orders is the heaviest dataset in the workspace: a Silver day is the
# whole k-space orderbook (~835 MiB/day compressed, ~75M rows) and Gold reads a
# day plus one tail snapshot. The corpus binary now streams Silver one row-group
# per snapshot, so its peak is one snapshot (~3-4 GB), comparable to a Gold build.
#
# Silver and Gold split across two pools for two different reasons:
#   - Gold joins the shared `heavy` MEMORY pool (limit 2 via `default_limit`),
#     sharing one budget with every other Gold build.
#   - Silver gets its OWN `market_orders` pool at limit 1 (set in deploy, see
#     deploy/dagster.yaml). market-orders Silver is the ONLY ingestor that parses
#     with rayon (ingestor-market-orders parses a window of snapshots via
#     `par_iter`), so a single run already saturates every core on the box. Two
#     concurrent runs would just oversubscribe the cores (rayon threads x runs)
#     with no throughput gain — observed as a run-queue backlog (loadavg `r` ~9 on
#     4 cores) during a backfill. limit 1 keeps one CPU-saturating run in flight;
#     other (single-threaded) datasets fill any remaining cores via their own
#     pools. limit 1 bounds this dataset only against itself; it says nothing
#     about overlap with `heavy` or `news_embed` — every memory-bearing pool
#     counts against one box budget, stated in deploy/dagster.yaml.
_SILVER_POOL = "market_orders"
_GOLD_POOL = "heavy"


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


def _build_gold_asset(derivative: str) -> dg.AssetsDefinition:
    """Builds a daily-partitioned Gold asset for one orderbook derivative.

    All derivatives share the look-back, gap-skip, and verify contract; only the
    ``--derivative`` and its own Hive tree differ. ``corpus gold build`` reads the
    target day plus the prior day's tail snapshot; verify keys on the derivative
    name (its own ``gold/<derivative>/...`` tree), not the dataset.
    """

    @dg.asset(
        name=f"{derivative.replace('-', '_')}_gold",
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


market_orders_snapshot_gold = _build_gold_asset(SNAPSHOT_DERIVATIVE)
market_orders_changes_gold = _build_gold_asset(CHANGES_DERIVATIVE)
market_orders_events_gold = _build_gold_asset(EVENTS_DERIVATIVE)
