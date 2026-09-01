"""sovereignty-map: one hourly Silver fold feeding three Gold trees (ADR-0066).

This module wires the Silver tier only. The dataset declares three Gold
derivatives — ``sovereignty-ownership`` and ``sovereignty-changes`` (the tenure
pair, which set the Silver reach-back) and ``sovereignty-panel`` (Gold-over-Gold,
no Silver window) — and those builds land in a later row.

Because the dataset is multi-derivative, ``resolve_partition_starts`` requires a
selector: it is given ``sovereignty-ownership`` and only ``.silver`` is used.
``_silver_start`` runs over every derivative regardless of which one is named, so
the choice does not move the Silver start; the selector exists for the Gold half
of the return value, which this module does not use.

The asset is a thin shim over the ``corpus`` binary: the binary owns the compute,
the ``parquet + _INDEX.json + _DONE`` contract, and the on-disk layout — including
this dataset's folder/tar era boundary, which never reaches Python. Partition
starts come from the corpus dataset config (see :mod:`config`), never hardcoded.
"""

from collections.abc import Iterator

import dagster as dg

from eve_industry_orchestration.defs.config import resolve_partition_starts
from eve_industry_orchestration.defs.corpus_resource import CorpusResource, date_key

DATASET = "sovereignty-map"
OWNERSHIP_DERIVATIVE = "sovereignty-ownership"

# Silver is shared by all three derivatives. Its derived start is the earliest
# preload across them (the tenure pair's 180d look-back before served_start
# 2022-01-01 → 2021-07-05), which stays above the dataset YAML's
# silver.served_start floor of 2021-07-01 (ADR-0027), so the clamp does not bite.
_starts = resolve_partition_starts(DATASET, OWNERSHIP_DERIVATIVE)
silver_partitions = dg.DailyPartitionsDefinition(start_date=_starts.silver)

_SILVER_POOL = "everef_download"


@dg.asset(
    partitions_def=silver_partitions,
    group_name="sovereignty_map",
    kinds={"corpus"},
    pool=_SILVER_POOL,
    # An interior upstream-gap day (EVE Ref published nothing, ADR-0028) skips:
    # corpus exits 0 with status "skipped" and writes no partition, so the
    # asset must be allowed to complete without materialising — the partition
    # stays Missing rather than failing or materialising empty.
    output_required=False,
)
def sovereignty_map_silver(
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
            "sovereignty-map %s: upstream absent, leaving partition missing", date
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
    # The run-state facts corpus recorded for the partition it just wrote (rows,
    # retention_class, parquet_sha256) merge over the identifying fields; the read
    # is advisory and yields {} rather than failing a completed materialisation.
    yield dg.MaterializeResult(
        metadata={"dataset": DATASET, "tier": "silver", "partition": date}
        | corpus.partition_metadata(DATASET, "silver", date_key(date))
    )
