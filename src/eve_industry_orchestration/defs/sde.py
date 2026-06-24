"""sde: the first build-versioned static-reference dataset (ADR-0030, ADR-0031).

Unlike the daily time-series datasets, an SDE partition is a game *build*,
published only on patches. Builds are discovered by ``corpus everef list``, not by
date enumeration, so the partition matrix is a ``DynamicPartitionsDefinition``
keyed on the build number, populated by the build-discovery sensor — there is no
``DailyPartitionsDefinition`` and no :func:`config.resolve_partition_starts`
window (SDE has no ``served_start`` / look-back).

A single ``corpus ingest --dataset sde --build <n>`` call fans out over the
configured ``silver.entities`` and commits each entity as its own Silver partition
(ADR-0031); a single ``corpus gold build --dataset sde --derivative <d> --build
<n>`` call fans the derivative's shape out over those same entities into its own
canonical Gold tree. To mirror that fan-out in the asset graph — one node per
entity (ADR-0031) — without re-running the whole build per entity, each tier is a
``@multi_asset``: ONE function, ONE CLI call, N entity outputs.

Two Gold derivatives (ADR-0030):

- ``sde-changelog`` (``entity-changelog``): a row-level diff of build N-1 -> N per
  entity. The baseline build (no committed predecessor Silver) writes no
  changelog partition (ADR-0031), so the changelog outputs are skippable and a
  baseline build leaves them Missing.
- ``sde-snapshot`` (``entity-snapshot``): a full-state passthrough per entity,
  always written.

Both Gold builds are blob passthroughs / row diffs over a single build's Silver,
not a windowed scan, so they are lightweight — no ``gold_heavy`` pool. Silver
fetches one ~94 MB archive per build, so it carries ``everef_download``.

**Verify keys on the on-disk tree, not the dataset.** Silver lives under
``silver/sde/<entity>/`` so verify passes ``sde/<entity>``; the Gold trees are
``gold/sde-<entity>/`` (snapshot) and ``gold/sde-<entity>-changelog/``
(changelog), so Gold verify passes those names. The build's ``release_date``
(from the ingest/build status JSON) is the physical Hive date verify addresses.
"""

from collections.abc import Iterator

import dagster as dg

from eve_industry_orchestration.defs.config import sde_entities
from eve_industry_orchestration.defs.corpus_resource import CorpusResource

DATASET = "sde"
CHANGELOG_DERIVATIVE = "sde-changelog"
SNAPSHOT_DERIVATIVE = "sde-snapshot"

# The entity fan-out is config-driven (ADR-0031), read once at import time like
# the daily datasets resolve their partition starts.
ENTITIES = sde_entities(DATASET)

# A build is the partition identity (ADR-0031). Builds are rare and discovered,
# not enumerable from a start date, so the matrix is dynamic and the
# build-discovery sensor registers each new build's key.
build_partitions = dg.DynamicPartitionsDefinition(name="sde_builds")

_SILVER_POOL = "everef_download"

_GROUP = "sde"


def _silver_key(entity: str) -> str:
    return f"sde_silver_{entity}"


def _changelog_key(entity: str) -> str:
    return f"sde_changelog_{entity}"


def _snapshot_key(entity: str) -> str:
    return f"sde_snapshot_{entity}"


changelog_asset_keys = [dg.AssetKey(_changelog_key(e)) for e in ENTITIES]
snapshot_asset_keys = [dg.AssetKey(_snapshot_key(e)) for e in ENTITIES]


@dg.multi_asset(
    specs=[
        dg.AssetSpec(key=_silver_key(e), group_name=_GROUP, kinds={"corpus"})
        for e in ENTITIES
    ],
    partitions_def=build_partitions,
    pool=_SILVER_POOL,
)
def sde_silver(
    context: dg.AssetExecutionContext, corpus: CorpusResource
) -> Iterator[dg.MaterializeResult]:
    """Silver build: ingest one build (all entities), then verify each tree.

    The single ``corpus ingest --dataset sde --build <n>`` call commits every
    configured entity (ADR-0031); the asset only shells out and records one
    materialisation per entity. Verify addresses each entity tree at the build's
    ``release_date`` (from the ingest status JSON).
    """
    build = context.partition_key
    status = corpus.run(
        context,
        "ingest",
        "--dataset",
        DATASET,
        "--build",
        build,
        "--sink-path",
        corpus.sink_path,
    )
    release_date = (status or {}).get("release_date")
    if release_date is None:
        raise dg.Failure(
            description=f"sde ingest build={build} returned no release_date"
        )

    for entity in ENTITIES:
        corpus.run(
            context,
            "verify",
            "--dataset",
            f"sde/{entity}",
            "--date",
            release_date,
            "--tier",
            "silver",
            "--sink-path",
            corpus.sink_path,
        )

    for entity in ENTITIES:
        yield dg.MaterializeResult(
            asset_key=_silver_key(entity),
            metadata={
                "dataset": DATASET,
                "tier": "silver",
                "entity": entity,
                "build": build,
                "release_date": release_date,
            },
        )


@dg.multi_asset(
    specs=[
        dg.AssetSpec(
            key=_changelog_key(e),
            deps=[_silver_key(e)],
            group_name=_GROUP,
            kinds={"corpus"},
            # Baseline build (no committed predecessor Silver) writes no
            # changelog partition (ADR-0031), so the output may be absent.
            skippable=True,
        )
        for e in ENTITIES
    ],
    partitions_def=build_partitions,
)
def sde_changelog_gold(
    context: dg.AssetExecutionContext, corpus: CorpusResource
) -> Iterator[dg.MaterializeResult]:
    """Changelog Gold: diff each entity build N-1 -> N. ``deps=`` is lineage only.

    A baseline build (the binary finds no committed predecessor Silver) writes no
    changelog partition and reports ``entities_written: 0`` — every entity output
    is left Missing rather than materialised empty (ADR-0031). Phase-1 builds
    ingest all entities together, so a non-baseline build writes all of them.
    """
    build = context.partition_key
    status = corpus.run(
        context,
        "gold",
        "build",
        "--dataset",
        DATASET,
        "--derivative",
        CHANGELOG_DERIVATIVE,
        "--build",
        build,
        "--sink-path",
        corpus.sink_path,
    )
    written = int((status or {}).get("entities_written", 0))
    release_date = (status or {}).get("release_date")
    if written == 0:
        context.log.info(
            "sde changelog build=%s is baseline (no predecessor); "
            "leaving changelog partitions missing",
            build,
        )
        return

    for entity in ENTITIES:
        corpus.run(
            context,
            "verify",
            "--dataset",
            f"sde-{entity}-changelog",
            "--date",
            release_date,
            "--tier",
            "gold",
            "--sink-path",
            corpus.sink_path,
        )

    for entity in ENTITIES:
        yield dg.MaterializeResult(
            asset_key=_changelog_key(entity),
            metadata={
                "dataset": DATASET,
                "derivative": CHANGELOG_DERIVATIVE,
                "tier": "gold",
                "entity": entity,
                "build": build,
                "release_date": release_date,
            },
        )


@dg.multi_asset(
    specs=[
        dg.AssetSpec(
            key=_snapshot_key(e),
            deps=[_silver_key(e)],
            group_name=_GROUP,
            kinds={"corpus"},
        )
        for e in ENTITIES
    ],
    partitions_def=build_partitions,
)
def sde_snapshot_gold(
    context: dg.AssetExecutionContext, corpus: CorpusResource
) -> Iterator[dg.MaterializeResult]:
    """Snapshot Gold: full-state passthrough per entity, always written.

    ``deps=`` is lineage only; the readiness sensor drives this. Every entity has
    target-build Silver, so all snapshot partitions are written (no baseline
    skip), then verified at the build's ``release_date``.
    """
    build = context.partition_key
    status = corpus.run(
        context,
        "gold",
        "build",
        "--dataset",
        DATASET,
        "--derivative",
        SNAPSHOT_DERIVATIVE,
        "--build",
        build,
        "--sink-path",
        corpus.sink_path,
    )
    release_date = (status or {}).get("release_date")
    if release_date is None:
        raise dg.Failure(
            description=f"sde snapshot build={build} returned no release_date"
        )

    for entity in ENTITIES:
        corpus.run(
            context,
            "verify",
            "--dataset",
            f"sde-{entity}",
            "--date",
            release_date,
            "--tier",
            "gold",
            "--sink-path",
            corpus.sink_path,
        )

    for entity in ENTITIES:
        yield dg.MaterializeResult(
            asset_key=_snapshot_key(entity),
            metadata={
                "dataset": DATASET,
                "derivative": SNAPSHOT_DERIVATIVE,
                "tier": "gold",
                "entity": entity,
                "build": build,
                "release_date": release_date,
            },
        )
