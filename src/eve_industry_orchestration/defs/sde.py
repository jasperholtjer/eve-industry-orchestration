"""sde: the build-versioned static-reference dataset (ADR-0032).

Unlike the daily time-series datasets, an SDE partition is a game *build*,
published only on patches. Builds are discovered by ``corpus everef list``, not
by date enumeration, so the build matrix is a ``DynamicPartitionsDefinition``
keyed on the build number, populated by the build-discovery sensor — there is no
``DailyPartitionsDefinition`` and no :func:`config.resolve_partition_starts`
window (SDE has no ``served_start`` / look-back).

ADR-0032 unified the layout (superseding the per-entity fan-out of
ADR-0030/0031), so the asset graph collapses to one node per tier per build:

- :func:`sde_silver` — one ``corpus ingest --dataset sde --build <n>`` call
  commits a single atomic unified Silver partition `(entity, _key, build_id,
  json)`. Build-partitioned. Carries ``everef_download`` (one ~94 MB archive).
- :func:`sde_changelog_gold` — one ``corpus gold build --derivative
  sde-changelog --build <n>`` writes one unified changelog partition keyed
  `(entity, _key)`. Build-partitioned. A baseline build (no committed
  predecessor Silver) reports ``status: skipped`` and is left Missing
  (``output_required=False``).
- :func:`sde_snapshot_gold` — the latest-only full-state catalogue. A
  **non-partitioned** asset a schedule rematerialises against ``--latest``
  (mirrors the system-jumps ``recency-weighted`` recent asset): it fans out over
  entities into flat ``gold/sde-<entity>/`` trees, overwritten per build. No
  historical snapshot trees — Silver is the build history.

Both Gold builds are row diffs / passthroughs over a single build's Silver, not
a windowed scan, so they are lightweight — no ``gold_heavy`` pool.

**Verify keys on the on-disk tree.** Silver and changelog use Hive date paths
(``silver/sde/`` and ``gold/sde-changelog/``) addressed at the build's
``release_date``. The snapshot tree is flat / non-partitioned, so it is not
date-verifiable; its success signal is the build status + the ``_DONE`` the
commit writes.
"""

from collections.abc import Iterator

import dagster as dg

from eve_industry_orchestration.defs.config import sde_entities
from eve_industry_orchestration.defs.corpus_resource import CorpusResource

DATASET = "sde"
CHANGELOG_DERIVATIVE = "sde-changelog"
SNAPSHOT_DERIVATIVE = "sde-snapshot"

# The entity list (ADR-0032 parse manifest) is config-driven, read once at
# import like the daily datasets resolve their partition starts. Used for
# materialisation metadata, not for an asset-per-entity fan-out.
ENTITIES = sde_entities(DATASET)

# A build is the partition identity (ADR-0031). Builds are rare and discovered,
# not enumerable from a start date, so the matrix is dynamic and the
# build-discovery sensor registers each new build's key.
build_partitions = dg.DynamicPartitionsDefinition(name="sde_builds")

_SILVER_POOL = "everef_download"
_GROUP = "sde"


@dg.asset(
    key="sde_silver",
    partitions_def=build_partitions,
    group_name=_GROUP,
    kinds={"corpus"},
    pool=_SILVER_POOL,
)
def sde_silver(
    context: dg.AssetExecutionContext, corpus: CorpusResource
) -> dg.MaterializeResult:
    """Silver build: ingest one build into the unified stream, then verify.

    A single ``corpus ingest --dataset sde --build <n>`` call commits the one
    atomic unified Silver partition (ADR-0032); the asset only shells out and
    records the materialisation. Verify addresses the ``sde`` tree at the
    build's ``release_date`` (from the ingest status JSON).
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

    corpus.run(
        context,
        "verify",
        "--dataset",
        DATASET,
        "--date",
        release_date,
        "--tier",
        "silver",
        "--sink-path",
        corpus.sink_path,
    )

    return dg.MaterializeResult(
        metadata={
            "dataset": DATASET,
            "tier": "silver",
            "build": build,
            "release_date": release_date,
            "rows": (status or {}).get("rows"),
        }
    )


@dg.asset(
    key="sde_changelog",
    partitions_def=build_partitions,
    deps=[sde_silver],
    group_name=_GROUP,
    kinds={"corpus"},
    # A baseline build (no committed predecessor Silver) writes no changelog
    # partition (ADR-0032): corpus reports status "skipped", so the asset must
    # complete without materialising — the partition stays Missing.
    output_required=False,
)
def sde_changelog_gold(
    context: dg.AssetExecutionContext, corpus: CorpusResource
) -> Iterator[dg.MaterializeResult | dg.AssetObservation]:
    """Unified changelog Gold: diff this build against its predecessor.

    ``deps=`` is lineage only; the readiness sensor drives this. A baseline
    build (no committed predecessor Silver) reports ``status: skipped`` and is
    left Missing rather than materialised empty (ADR-0032), mirroring the
    system-jumps upstream-gap skip.
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
    if status is not None and status.get("status") == "skipped":
        context.log.info(
            "sde changelog build=%s is baseline (no predecessor); "
            "leaving partition missing",
            build,
        )
        yield dg.AssetObservation(
            asset_key=context.asset_key,
            partition=build,
            metadata={"skip_reason": "baseline_build", "build": build},
        )
        return

    release_date = (status or {}).get("release_date")
    corpus.run(
        context,
        "verify",
        "--dataset",
        CHANGELOG_DERIVATIVE,
        "--date",
        release_date,
        "--tier",
        "gold",
        "--sink-path",
        corpus.sink_path,
    )
    yield dg.MaterializeResult(
        metadata={
            "dataset": DATASET,
            "derivative": CHANGELOG_DERIVATIVE,
            "tier": "gold",
            "build": build,
            "release_date": release_date,
        }
    )


@dg.asset(
    key="sde_snapshot",
    deps=[sde_silver],
    group_name=_GROUP,
    kinds={"corpus"},
)
def sde_snapshot_gold(
    context: dg.AssetExecutionContext, corpus: CorpusResource
) -> dg.MaterializeResult:
    """Latest-only full-state snapshot for the current build (ADR-0032).

    Non-partitioned: a schedule rematerialises it against ``--latest``, like the
    system-jumps recent EWMA. ``corpus gold build --derivative sde-snapshot
    --latest`` resolves the highest committed Silver build and fans out over
    entities into flat ``gold/sde-<entity>/`` trees, overwriting the previous
    snapshot. ``deps=`` carries lineage only (a non-partitioned asset cannot
    chain build partitions); the schedule drives it.

    Skips cleanly when no Silver build is committed yet (the schedule may fire on
    a cold corpus): the flat tree is not date-verifiable, so the build status is
    the success signal.
    """
    committed = corpus.state_query(
        "SELECT 1 FROM partitions WHERE tier = 'silver' AND dataset = 'sde' LIMIT 1"
    )
    if not committed:
        context.log.info("sde snapshot: no committed Silver build yet; skipping")
        return dg.MaterializeResult(
            metadata={"dataset": DATASET, "tier": "gold", "built": False}
        )

    status = corpus.run(
        context,
        "gold",
        "build",
        "--dataset",
        DATASET,
        "--derivative",
        SNAPSHOT_DERIVATIVE,
        "--latest",
        "--sink-path",
        corpus.sink_path,
    )
    return dg.MaterializeResult(
        metadata={
            "dataset": DATASET,
            "derivative": SNAPSHOT_DERIVATIVE,
            "tier": "gold",
            "built": True,
            "build": (status or {}).get("build_id"),
            "release_date": (status or {}).get("release_date"),
            "entities_written": (status or {}).get("entities_written"),
        }
    )
