"""sovereignty-map: one hourly Silver fold feeding three Gold trees (ADR-0066).

The dataset declares three Gold derivatives — ``sovereignty-ownership`` and
``sovereignty-changes`` (the tenure pair, which set the Silver reach-back) and
``sovereignty-panel`` (Gold-over-Gold, no Silver window) — and all three build
here, each under its own ``--derivative`` and its own partitions definition,
because they do not share a served start.

The panel also owns the family's assembly edge: it depends on the other four
sovereignty Gold assets and on the SDE snapshot, so ADR-0066's build order is a
real dependency rather than schedule ordering. This module therefore imports
``sovereignty_structures`` and ``sovereignty_campaigns``; neither imports back.

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

from eve_industry_orchestration.defs import (
    sde,
    sovereignty_campaigns,
    sovereignty_structures,
)
from eve_industry_orchestration.defs.config import resolve_partition_starts
from eve_industry_orchestration.defs.corpus_resource import CorpusResource, date_key

DATASET = "sovereignty-map"
OWNERSHIP_DERIVATIVE = "sovereignty-ownership"
CHANGES_DERIVATIVE = "sovereignty-changes"
PANEL_DERIVATIVE = "sovereignty-panel"

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


def _build_gold(
    context: dg.AssetExecutionContext, corpus: CorpusResource, derivative: str
) -> Iterator[dg.MaterializeResult | dg.AssetObservation]:
    """The body all three Gold assets share, differing only in ``--derivative``.

    Build, then — unless corpus reported a skipped day — Gold-tier verify and a
    ``MaterializeResult``. Nothing here inspects a window, a coverage ratio or a
    sibling tree: which days a build can produce is the binary's answer
    (ADR-0065/ADR-0066), and this asset only reports the status it was given.
    """
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
            "%s %s: prerequisite permanently absent, leaving partition missing",
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
    # `corpus gold build` writes both the partition tree and the run-state row
    # under the *derivative* name, not the dataset, so Gold verify and the
    # run-state read both key on the derivative. Two derivatives of one dataset
    # therefore record their own facts, never each other's.
    yield dg.MaterializeResult(
        metadata={
            "dataset": DATASET,
            "derivative": derivative,
            "tier": "gold",
            "partition": date,
        }
        | corpus.partition_metadata(derivative, "gold", date_key(date))
    )


def _gold_start(derivative: str) -> str:
    """The derivative's own configured ``served_start``.

    Resolved by name rather than derived from the Silver matrix: this dataset's
    three Gold trees do not share a start (the panel serves one flip window
    later than the tenure pair), so each asset resolves its own.
    """
    start = resolve_partition_starts(DATASET, derivative).gold
    if start is None:
        raise ValueError(
            f"{DATASET} resolved no Gold served_start for {derivative}; every "
            "sovereignty derivative declares one"
        )
    return start


ownership_gold_partitions = dg.DailyPartitionsDefinition(
    start_date=_gold_start(OWNERSHIP_DERIVATIVE)
)
changes_gold_partitions = dg.DailyPartitionsDefinition(
    start_date=_gold_start(CHANGES_DERIVATIVE)
)
panel_gold_partitions = dg.DailyPartitionsDefinition(
    start_date=_gold_start(PANEL_DERIVATIVE)
)


@dg.asset(
    name="sovereignty_ownership_gold",
    partitions_def=ownership_gold_partitions,
    deps=[sovereignty_map_silver],
    group_name="sovereignty_map",
    kinds={"corpus"},
    # No `pool=`: membership of a memory-bearing pool is by measured peak and
    # this build has none yet (see deploy/dagster.yaml). The global cap applies.
    #
    # A day whose prerequisite can never arrive is reported by corpus as
    # "skipped" with exit 0 and no partition written (ADR-0065), so the asset
    # must be allowed to complete without materialising.
    output_required=False,
)
def sovereignty_ownership_gold(
    context: dg.AssetExecutionContext, corpus: CorpusResource
) -> Iterator[dg.MaterializeResult | dg.AssetObservation]:
    """Gold partition: who holds which nullsec system that day, with tenure.

    ``deps=`` is lineage only; the readiness sensor drives this. The build reads
    the ``[date - 180d, date]`` Silver window and owns its own coverage gate — an
    incomplete window is the binary's decision to make, never a Python
    pre-check.
    """
    yield from _build_gold(context, corpus, OWNERSHIP_DERIVATIVE)


@dg.asset(
    name="sovereignty_changes_gold",
    partitions_def=changes_gold_partitions,
    deps=[sovereignty_map_silver],
    group_name="sovereignty_map",
    kinds={"corpus"},
    # No `pool=`: see sovereignty_ownership_gold.
    output_required=False,
)
def sovereignty_changes_gold(
    context: dg.AssetExecutionContext, corpus: CorpusResource
) -> Iterator[dg.MaterializeResult | dg.AssetObservation]:
    """Gold partition: the flip log for one day.

    Shares ``sovereignty_map_silver`` and its 180d tenure window with
    ``sovereignty_ownership_gold`` on purpose, but is a separate build under its
    own ``--derivative``: the two trees are written and registered separately,
    and neither run produces the other's partition.
    """
    yield from _build_gold(context, corpus, CHANGES_DERIVATIVE)


@dg.asset(
    name="sovereignty_panel_gold",
    partitions_def=panel_gold_partitions,
    deps=[
        sovereignty_ownership_gold,
        sovereignty_changes_gold,
        sovereignty_structures.sovereignty_adm_gold,
        sovereignty_campaigns.sovereignty_contests_gold,
        sde.sde_snapshot_gold,
    ],
    group_name="sovereignty_map",
    kinds={"corpus"},
    # No `pool=`: see sovereignty_ownership_gold.
    output_required=False,
)
def sovereignty_panel_gold(
    context: dg.AssetExecutionContext, corpus: CorpusResource
) -> Iterator[dg.MaterializeResult | dg.AssetObservation]:
    """Gold partition: the assembled per-system panel (ADR-0066 decision 8).

    Gold-over-Gold: the build reads the same day's ownership / adm / contests
    partitions, the trailing 30-day ``sovereignty-changes`` flip window and the
    ``sde-mapSolarSystems`` snapshot — never Silver — through the ADR-0052
    sibling read, which fingerprints those inputs into ``_INDEX.json``. The four
    sibling ``deps=`` make ADR-0066's build order a real edge in the asset graph
    rather than an accident of sensor timing; the non-partitioned SDE dep
    carries lineage only, as it cannot chain build partitions.

    A sibling tree that skipped its day can never be assembled, so corpus
    reports ``skipped`` and the panel day stays Missing (ADR-0065).
    """
    yield from _build_gold(context, corpus, PANEL_DERIVATIVE)
