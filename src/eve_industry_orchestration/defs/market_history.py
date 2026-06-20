"""Pilot dataset: market-history, one daily partition through Silver and Gold.

Each partition shells out to the ``corpus`` binary, which does the compute and
writes the ``parquet + _INDEX.json + _DONE`` contract to the NFS sink. Dagster
owns the partition matrix, the backfill UI, and the materialisation log only.

Silver and Gold have distinct start dates: Gold begins at ``gold.served_start``
(the earliest legal target), while Silver reaches back one rolling window before
it so the first Gold partition has its full window present. Both dates come from
the corpus dataset config (see :mod:`config`), never hardcoded here.
"""

import dagster as dg

from eve_industry_orchestration.defs.config import resolve_partition_starts
from eve_industry_orchestration.defs.corpus_resource import CorpusResource

DATASET = "market-history"

_starts = resolve_partition_starts(DATASET)
# Layout is year={year}/month={month:02d}/day={day:02d} (per the dataset YAML),
# matching one Dagster daily partition per contract directory.
silver_partitions = dg.DailyPartitionsDefinition(start_date=_starts.silver)
gold_partitions = dg.DailyPartitionsDefinition(start_date=_starts.gold)


@dg.asset(
    partitions_def=silver_partitions,
    group_name="market_history",
    kinds={"corpus"},
)
def market_history_silver(
    context: dg.AssetExecutionContext, corpus: CorpusResource
) -> dg.MaterializeResult:
    """Silver partition: ingest one date, then verify the written contract."""
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
    # TODO: enrich metadata from _INDEX.json / `corpus state query`.
    return dg.MaterializeResult(
        metadata={"dataset": DATASET, "tier": "silver", "partition": date}
    )


@dg.asset(
    partitions_def=gold_partitions,
    deps=[market_history_silver],
    group_name="market_history",
    kinds={"corpus"},
)
def market_history_gold(
    context: dg.AssetExecutionContext, corpus: CorpusResource
) -> dg.MaterializeResult:
    """Gold partition, built from the Silver contract on the NAS."""
    # Blocked upstream: `corpus gold` documents the rolling-window builder as
    # "deferred to plan 07 follow-up" (ROADMAP item 2). Keep failing loudly
    # until the corpus side completes; do not invoke a half-built builder.
    raise NotImplementedError("corpus gold builder deferred to plan 07 follow-up")
