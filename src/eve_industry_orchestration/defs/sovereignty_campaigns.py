"""sovereignty-campaigns: hourly Silver feeding one no-reach-back Gold tree.

The dataset declares a single Gold derivative, ``sovereignty-contests``
(``shape: sov-contests``, no look-back and no coverage gate), so
``resolve_partition_starts`` needs no derivative selector for Silver; the Gold
asset names it anyway, because a derivative's served start is the derivative's
own and nothing here may assume a dataset has just one.

The asset is a thin shim over the ``corpus`` binary: the binary owns the compute,
the ``parquet + _INDEX.json + _DONE`` contract, and the on-disk layout — including
this dataset's folder/tar era boundary, which never reaches Python. Partition
starts come from the corpus dataset config (see :mod:`config`), never hardcoded.
"""

from collections.abc import Iterator

import dagster as dg

from eve_industry_orchestration.defs.config import resolve_partition_starts
from eve_industry_orchestration.defs.corpus_resource import CorpusResource, date_key

DATASET = "sovereignty-campaigns"
CONTESTS_DERIVATIVE = "sovereignty-contests"

# The single derivative declares no reach-back, so Silver starts at its
# served_start (2022-01-01) — already above the dataset YAML's
# silver.served_start floor of 2021-07-01 (ADR-0027), so the clamp does not bite.
_starts = resolve_partition_starts(DATASET)
silver_partitions = dg.DailyPartitionsDefinition(start_date=_starts.silver)

_SILVER_POOL = "everef_download"


@dg.asset(
    partitions_def=silver_partitions,
    group_name="sovereignty_campaigns",
    kinds={"corpus"},
    pool=_SILVER_POOL,
    # An interior upstream-gap day (EVE Ref published nothing, ADR-0028) skips:
    # corpus exits 0 with status "skipped" and writes no partition, so the
    # asset must be allowed to complete without materialising — the partition
    # stays Missing rather than failing or materialising empty.
    output_required=False,
)
def sovereignty_campaigns_silver(
    context: dg.AssetExecutionContext, corpus: CorpusResource
) -> Iterator[dg.MaterializeResult | dg.AssetObservation]:
    """Silver partition: ingest one day's hourly snapshots, then verify.

    A genuinely-absent upstream day (corpus reports ``status: skipped``) is left
    Missing: the verify (which would 404 on the absent partition) is skipped and
    an ``AssetObservation`` records why, instead of a misleading materialisation.

    ``incomplete`` gets no branch here: this dataset's ``hourly-folder-tar``
    layout carries no ``member_suffix`` field at all, so corpus's
    ``FolderEmptiedByDeclaredSuffix``
    → ``PublicationFrontier`` path never fires and every non-``skipped`` failure
    is either a clean ``UpstreamAbsent`` skip or a fatal error, which leaves an
    ``incomplete`` branch here unreachable rather than protective.
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
            "sovereignty-campaigns %s: upstream absent, leaving partition missing",
            date,
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


_gold_starts = resolve_partition_starts(DATASET, CONTESTS_DERIVATIVE)
if _gold_starts.gold is None:
    raise ValueError(
        f"{DATASET} resolved no Gold served_start for {CONTESTS_DERIVATIVE}; every "
        "sovereignty derivative declares one"
    )
# The derivative's own served_start, resolved by name. Two derivatives of one
# dataset need not share a start (the sovereignty panel does not), so the Gold
# matrix is never derived from the Silver one.
contests_gold_partitions = dg.DailyPartitionsDefinition(start_date=_gold_starts.gold)


@dg.asset(
    name="sovereignty_contests_gold",
    partitions_def=contests_gold_partitions,
    deps=[sovereignty_campaigns_silver],
    group_name="sovereignty_campaigns",
    kinds={"corpus"},
    # No `pool=`: membership of a memory-bearing pool is by measured peak and
    # this build has none yet (see deploy/dagster.yaml). The global cap applies.
    #
    # A day whose prerequisite can never arrive is reported by corpus as
    # "skipped" with exit 0 and no partition written (ADR-0065), so the asset
    # must be allowed to complete without materialising.
    output_required=False,
)
def sovereignty_contests_gold(
    context: dg.AssetExecutionContext, corpus: CorpusResource
) -> Iterator[dg.MaterializeResult | dg.AssetObservation]:
    """Gold partition: the contested-system view for one day.

    ``deps=`` is lineage only; the readiness sensor drives this. The build reads
    the target day alone — no window and no coverage gate — but the skipped-day
    branch is carried regardless: which days a build can produce is the binary's
    answer, not a shape this module reasons about.
    """
    date = context.partition_key
    status = corpus.run(
        context,
        "gold",
        "build",
        "--dataset",
        DATASET,
        "--derivative",
        CONTESTS_DERIVATIVE,
        "--date",
        date,
        "--sink-path",
        corpus.sink_path,
    )
    if status is not None and status.get("status") == "skipped":
        context.log.info(
            "%s %s: prerequisite permanently absent, leaving partition missing",
            CONTESTS_DERIVATIVE,
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
        CONTESTS_DERIVATIVE,
        "--date",
        date,
        "--tier",
        "gold",
        "--sink-path",
        corpus.sink_path,
    )
    # `corpus gold build` writes both the partition tree and the run-state row
    # under the *derivative* name, not the dataset, so Gold verify and the
    # run-state read both key on CONTESTS_DERIVATIVE.
    yield dg.MaterializeResult(
        metadata={
            "dataset": DATASET,
            "derivative": CONTESTS_DERIVATIVE,
            "tier": "gold",
            "partition": date,
        }
        | corpus.partition_metadata(CONTESTS_DERIVATIVE, "gold", date_key(date))
    )
