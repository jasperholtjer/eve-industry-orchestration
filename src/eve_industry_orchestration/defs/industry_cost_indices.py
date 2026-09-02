"""industry-cost-indices: the rolling cost-index feature history (ADR-0043).

One hourly, day-partitioned Silver source feeds a single Gold derivative,
``industry-cost-indices-history`` (shape ``cost-index-history`` — a flat rolling
matrix over horizons ``[7, 30, 90, 365]`` with a ``0.5`` coverage gate). It is a
backfillable historical matrix, so it gets a daily-partitioned Gold asset driven
by a ``ready-dates`` sensor — exactly like system-jumps' ``system-traffic-history``
derivative, passing ``--derivative``.

The point-in-time "now" cost-index level is **not** here: it is the separate
current-overwrite dataset ``industry-cost-indices-live`` (see
:mod:`industry_cost_indices_live`).

Each asset is a thin shim over the ``corpus`` binary; the binary owns the compute,
the coverage gate, and the ``parquet + _INDEX.json + _DONE`` contract. Partition
starts come from the corpus dataset config (see :mod:`config`), never hardcoded.

**Gold verify keys on the derivative name, not the dataset.** ``corpus gold
build`` writes under ``gold/<derivative>/...`` and ``corpus verify --tier gold``
resolves ``gold/<--dataset>/...``, so Gold verify passes the *derivative* name
(``industry-cost-indices-history``) as ``--dataset``. Silver verify still uses the
dataset name.
"""

from collections.abc import Iterator

import dagster as dg

from eve_industry_orchestration.defs.config import resolve_partition_starts
from eve_industry_orchestration.defs.corpus_resource import CorpusResource, date_key

DATASET = "industry-cost-indices"
HISTORY_DERIVATIVE = "industry-cost-indices-history"

# Silver is shared by the history derivative. Its derived start is the 365d
# preload before the derivative's served_start (2022-01-01 → 2021-01-01), but the
# dataset YAML's silver.served_start (ADR-0027) clamps Silver up to 2021-07-01,
# the start of EVE Ref's dense hourly era. Gold starts at the history
# derivative's served_start (2022-01-01).
_history_starts = resolve_partition_starts(DATASET, HISTORY_DERIVATIVE)
silver_partitions = dg.DailyPartitionsDefinition(start_date=_history_starts.silver)
history_gold_partitions = dg.DailyPartitionsDefinition(start_date=_history_starts.gold)

_SILVER_POOL = "everef_download"


@dg.asset(
    partitions_def=silver_partitions,
    group_name="industry_cost_indices",
    kinds={"corpus"},
    pool=_SILVER_POOL,
    # An interior upstream-gap day (EVE Ref published nothing, ADR-0028) skips:
    # corpus exits 0 with status "skipped" and writes no partition, so the asset
    # must complete without materialising — the partition stays Missing rather
    # than failing or materialising empty.
    output_required=False,
)
def industry_cost_indices_silver(
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
            "industry-cost-indices %s: upstream absent, leaving partition missing",
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


@dg.asset(
    partitions_def=history_gold_partitions,
    deps=[industry_cost_indices_silver],
    group_name="industry_cost_indices",
    kinds={"corpus"},
    # No `heavy` pool: the 365d window is ~360 narrow daily files (~5k systems ×
    # ~4 activities, last-of-day) merged k-way over small row-groups — well under
    # the ~4 GiB `heavy` budget. Bounded by the global cap alone so it never
    # occupies a scarce heavy slot meant for the big windowed backfills.
    # A target day whose Silver is an upstream gap can never build a Gold row
    # (ADR-0029); corpus reports "skipped", so the asset must complete without
    # materialising — the partition stays Missing rather than failing.
    output_required=False,
)
def industry_cost_indices_history_gold(
    context: dg.AssetExecutionContext, corpus: CorpusResource
) -> Iterator[dg.MaterializeResult | dg.AssetObservation]:
    """Gold partition for the cost-index-history derivative, then verify.

    ``deps=`` is lineage only; the readiness sensor drives this. ``corpus gold
    build`` reads the full ``[date - 365d, date]`` Silver window and enforces
    ``coverage_min_ratio`` itself — an incomplete window exits non-zero. Verify
    keys on the derivative name (its Gold tree), not the dataset.

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
        HISTORY_DERIVATIVE,
        "--date",
        date,
        "--sink-path",
        corpus.sink_path,
    )
    if status is not None and status.get("status") == "skipped":
        context.log.info(
            "industry-cost-indices-history %s: target silver is an upstream gap, "
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
        HISTORY_DERIVATIVE,
        "--date",
        date,
        "--tier",
        "gold",
        "--sink-path",
        corpus.sink_path,
    )
    # A multi-derivative Gold row is keyed in run-state on the derivative (its own
    # gold/<derivative>/ tree), not on the dataset name.
    yield dg.MaterializeResult(
        metadata={
            "dataset": DATASET,
            "derivative": HISTORY_DERIVATIVE,
            "tier": "gold",
            "partition": date,
        }
        | corpus.partition_metadata(HISTORY_DERIVATIVE, "gold", date_key(date))
    )
