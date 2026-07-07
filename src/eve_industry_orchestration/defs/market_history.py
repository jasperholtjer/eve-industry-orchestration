"""Pilot dataset: market-history, one daily partition through Silver and Gold.

Each partition shells out to the ``corpus`` binary, which does the compute and
writes the ``parquet + _INDEX.json + _DONE`` contract to the NFS sink. Dagster
owns the partition matrix, the backfill UI, and the materialisation log only.

Silver and Gold have distinct start dates: Gold begins at ``gold.served_start``
(the earliest legal target), while Silver reaches back one rolling window before
it so the first Gold partition has its full window present. Both dates come from
the corpus dataset config (see :mod:`config`), never hardcoded here.
"""

from collections.abc import Iterator

import dagster as dg

from eve_industry_orchestration.defs.config import resolve_partition_starts
from eve_industry_orchestration.defs.corpus_resource import CorpusResource

DATASET = "market-history"

_starts = resolve_partition_starts(DATASET)
# Layout is year={year}/month={month:02d}/day={day:02d} (per the dataset YAML),
# matching one Dagster daily partition per contract directory.
silver_partitions = dg.DailyPartitionsDefinition(start_date=_starts.silver)
gold_partitions = dg.DailyPartitionsDefinition(start_date=_starts.gold)


# Silver fetches one EVE Ref archive per run; the `everef_download` pool caps
# concurrent upstream fetches (politeness to data.everef.net) across every launch
# path — sensor, UI backfill, manual — mirroring the `heavy` memory pool. The
# limit lives in deploy/dagster.yaml.
_SILVER_POOL = "everef_download"


@dg.asset(
    partitions_def=silver_partitions,
    group_name="market_history",
    kinds={"corpus"},
    pool=_SILVER_POOL,
    # EVE Ref publishes the daily market-history file incrementally (ADR-0041):
    # a not-yet-settled day reports status "incomplete" (exit 0, no partition),
    # so the asset must complete without materialising — the partition stays
    # Missing and the sensor re-proposes it until the upstream file settles.
    output_required=False,
)
def market_history_silver(
    context: dg.AssetExecutionContext, corpus: CorpusResource
) -> Iterator[dg.MaterializeResult | dg.AssetObservation]:
    """Silver partition: ingest one date, then verify the written contract.

    A day whose upstream publication is still incomplete (corpus reports
    ``status: incomplete``, ADR-0041) is left Missing: the verify (which would
    fail on the absent partition) is skipped and an ``AssetObservation`` records
    why. Unlike a permanent absent-day skip, the partition is re-proposed on the
    next sensor tick — the availability sensor rotates its ``run_key`` per tick
    (see :mod:`sensor_util`) so Dagster dedup does not swallow the retry — so the
    full file is picked up once EVE Ref finishes it.
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
    if status is not None and status.get("status") == "incomplete":
        context.log.info(
            "market-history %s: upstream publication incomplete, leaving partition "
            "missing (retryable)",
            date,
        )
        yield dg.AssetObservation(
            asset_key=context.asset_key,
            partition=date,
            metadata={
                "skip_reason": "upstream_incomplete",
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
    # TODO: enrich metadata from _INDEX.json / `corpus state query`.
    yield dg.MaterializeResult(
        metadata={"dataset": DATASET, "tier": "silver", "partition": date}
    )


# Blueprint for heavyweight datasets: a Gold build reads the full
# [date - max_horizon, date] rolling window and peaks ~3-4 GB in the `corpus`
# subprocess. The `heavy` concurrency pool (limit set in deploy/dagster.yaml)
# caps how many such builds run at once across ALL launch paths — sensor, UI
# backfill, manual — and is shared by every heavy dataset so total memory stays
# bounded on the single box. Lightweight datasets omit `pool=` entirely.
_GOLD_POOL = "heavy"


@dg.asset(
    partitions_def=gold_partitions,
    deps=[market_history_silver],
    group_name="market_history",
    kinds={"corpus"},
    pool=_GOLD_POOL,
)
def market_history_gold(
    context: dg.AssetExecutionContext, corpus: CorpusResource
) -> dg.MaterializeResult:
    """Gold partition: build the rolling-window features, then verify the contract.

    ``corpus gold build`` reads the full ``[date - max_horizon, date]`` Silver
    window from the contract on the NAS and enforces ``coverage_min_ratio``
    itself: an incomplete window exits non-zero and fails the run rather than
    writing a degraded partition. The availability sensor only requests dates
    ``corpus gold ready-dates`` already reports as buildable, so this is a
    backstop rather than the primary gate.
    """
    date = context.partition_key
    corpus.run(
        context,
        "gold",
        "build",
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
        "gold",
        "--sink-path",
        corpus.sink_path,
    )
    # TODO: enrich metadata from _INDEX.json / `corpus state query`.
    return dg.MaterializeResult(
        metadata={"dataset": DATASET, "tier": "gold", "partition": date}
    )
