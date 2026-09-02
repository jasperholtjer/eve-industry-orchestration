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

from collections.abc import Iterator

import dagster as dg

from eve_industry_orchestration.defs.config import resolve_partition_starts
from eve_industry_orchestration.defs.corpus_resource import CorpusResource, date_key

DATASET = "system-jumps"
HISTORY_DERIVATIVE = "system-traffic-history"
RECENT_DERIVATIVE = "system-traffic-recent"

# Silver is shared by both derivatives. Its derived start is the earliest preload
# (system-traffic-history's 365d window before its served_start 2022-01-01 →
# 2021-01-01), but EVE Ref's dense hourly era begins 2021-07-01 (2020/early-2021
# has no archive), so the dataset YAML's silver.served_start (ADR-0027) clamps
# Silver up to 2021-07-01. Gold starts at
# the history derivative's served_start (2022-01-01). The recent derivative has
# no served_start and is non-partitioned, so it needs no start.
_history_starts = resolve_partition_starts(DATASET, HISTORY_DERIVATIVE)
silver_partitions = dg.DailyPartitionsDefinition(start_date=_history_starts.silver)
history_gold_partitions = dg.DailyPartitionsDefinition(start_date=_history_starts.gold)

_SILVER_POOL = "everef_download"


@dg.asset(
    partitions_def=silver_partitions,
    group_name="system_jumps",
    kinds={"corpus"},
    pool=_SILVER_POOL,
    # An interior upstream-gap day (EVE Ref published nothing, ADR-0028) skips:
    # corpus exits 0 with status "skipped" and writes no partition, so the
    # asset must be allowed to complete without materialising — the partition
    # stays Missing rather than failing or materialising empty.
    output_required=False,
)
def system_jumps_silver(
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
            "system-jumps %s: upstream absent, leaving partition missing", date
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
    deps=[system_jumps_silver],
    group_name="system_jumps",
    kinds={"corpus"},
    # No `heavy` pool: the 365d window is ~360 narrow daily files (~5k systems ×
    # 24h) and the k-way merge holds only small row-groups — measured peak RSS
    # ~97 MiB, ~40x under the ~4 GiB `heavy` budget. Bounded by the global cap
    # alone so it never occupies a scarce heavy slot meant for the big backfills.
    # A target day whose Silver is an upstream gap can never build a Gold row
    # (ADR-0029); corpus reports "skipped", so the asset must complete without
    # materialising — the partition stays Missing rather than failing.
    output_required=False,
)
def system_jumps_history_gold(
    context: dg.AssetExecutionContext, corpus: CorpusResource
) -> Iterator[dg.MaterializeResult | dg.AssetObservation]:
    """Gold partition for the flat-multi-horizon derivative, then verify.

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
            "system-traffic-history %s: target silver is an upstream gap, "
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


@dg.asset(
    deps=[system_jumps_silver],
    group_name="system_jumps",
    kinds={"corpus"},
)
def system_jumps_recent_gold(
    context: dg.AssetExecutionContext, corpus: CorpusResource
) -> dg.MaterializeResult:
    """Non-partitioned EWMA "navigate-now" heat for the latest buildable date.

    Resolves the latest date itself (``corpus gold ready-dates`` → ``max(ready)``)
    and builds only that date. ``deps=`` carries lineage only (the EWMA reads the
    shared Silver tree); a non-partitioned asset cannot chain partitions, so the
    schedule — not Silver — drives it. No ``heavy`` pool: the EWMA build spans
    only the short warmup, so it is lightweight and bounded by the global cap
    alone.
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
    # `latest` is the newest buildable *date*, not a latest-only tree: `corpus gold
    # build --date` registers the run-state row as `date=<target>` for every
    # derivative shape, EWMA included, so the key is a date key here too.
    return dg.MaterializeResult(
        metadata={
            "dataset": DATASET,
            "derivative": RECENT_DERIVATIVE,
            "tier": "gold",
            "built": True,
            "partition": latest,
        }
        | corpus.partition_metadata(RECENT_DERIVATIVE, "gold", date_key(latest))
    )
