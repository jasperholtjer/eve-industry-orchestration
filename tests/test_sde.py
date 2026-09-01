"""Tests for the build-versioned unified SDE assets and sensors (ADR-0032)."""

from __future__ import annotations

import dagster as dg
import pytest

from eve_industry_orchestration.defs import sde, sensor_util
from eve_industry_orchestration.defs.config import sde_entities, sde_gold_derivatives
from eve_industry_orchestration.defs.sde import (
    sde_changelog_gold,
    sde_industry_facilities_gold,
    sde_industry_hubs_gold,
    sde_industry_products_gold,
    sde_silver,
    sde_snapshot_gold,
)
from eve_industry_orchestration.defs.sensors import (
    sde_build_discovery_sensor,
    sde_gold_sensor,
)

DATASET = "sde"
BUILDS = "100:2025-09-18,200:2025-10-01"


def _ingest_build(corpus, build: int) -> None:
    corpus.run(
        dg.build_asset_context(),
        "ingest",
        "--dataset",
        DATASET,
        "--build",
        str(build),
        "--sink-path",
        corpus.sink_path,
    )


def _build_changelog(corpus, build: int) -> None:
    corpus.run(
        dg.build_asset_context(),
        "gold",
        "build",
        "--dataset",
        DATASET,
        "--derivative",
        sde.CHANGELOG_DERIVATIVE,
        "--build",
        str(build),
        "--sink-path",
        corpus.sink_path,
    )


def _instance_with_builds(*builds: int) -> dg.DagsterInstance:
    instance = dg.DagsterInstance.ephemeral()
    instance.add_dynamic_partitions(sde.build_partitions.name, [str(b) for b in builds])
    return instance


# --- config ----------------------------------------------------------------


def test_sde_entities_from_config() -> None:
    assert sde_entities(DATASET) == ["types", "groups", "mapRegions"]


def test_sde_gold_derivatives_from_config() -> None:
    derivatives = sde_gold_derivatives(DATASET)
    assert [(d.name, d.shape) for d in derivatives] == [
        ("sde-changelog", "entity-changelog"),
        ("sde-snapshot", "entity-snapshot"),
        ("sde-industry-products", "industry-products"),
        ("sde-industry-facilities", "industry-facilities"),
        ("sde-industry-hubs", "industry-hubs"),
    ]


# --- Silver asset (one atomic partition per build) -------------------------


def test_silver_materialises_one_partition_for_a_build(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FAKE_SDE_BUILDS", BUILDS)
    instance = _instance_with_builds(100)

    result = dg.materialize(
        [sde_silver],
        partition_key="100",
        instance=instance,
        resources={"corpus": corpus},
    )

    assert result.success
    materialised = {
        e.asset_key.to_user_string() for e in result.get_asset_materialization_events()
    }
    assert materialised == {"sde_silver"}


# --- snapshot Gold (latest-only, non-partitioned) --------------------------


def test_snapshot_materialises_against_latest(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FAKE_SDE_BUILDS", BUILDS)
    instance = _instance_with_builds(100)
    _ingest_build(corpus, 100)

    result = dg.materialize(
        [sde_snapshot_gold],
        instance=instance,
        resources={"corpus": corpus},
    )

    assert result.success
    events = result.get_asset_materialization_events()
    assert len(events) == 1
    assert events[0].asset_key.to_user_string() == "sde_snapshot"


def test_snapshot_skips_when_no_silver_committed(corpus) -> None:
    # The schedule may fire on a cold corpus: no committed Silver → built False,
    # but the asset still materialises (a clean skip, not a failure).
    instance = dg.DagsterInstance.ephemeral()

    result = dg.materialize(
        [sde_snapshot_gold],
        instance=instance,
        resources={"corpus": corpus},
    )

    assert result.success
    events = result.get_asset_materialization_events()
    assert len(events) == 1
    assert events[0].materialization.metadata["built"].value is False


# --- industry-products Gold (latest-only, non-partitioned; ADR-0044) --------


def test_industry_products_materialises_against_latest(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FAKE_SDE_BUILDS", BUILDS)
    instance = _instance_with_builds(100)
    _ingest_build(corpus, 100)

    result = dg.materialize(
        [sde_industry_products_gold],
        instance=instance,
        resources={"corpus": corpus},
    )

    assert result.success
    events = result.get_asset_materialization_events()
    assert len(events) == 1
    assert events[0].asset_key.to_user_string() == "sde_industry_products"


def test_industry_products_skips_when_no_silver_committed(corpus) -> None:
    instance = dg.DagsterInstance.ephemeral()

    result = dg.materialize(
        [sde_industry_products_gold],
        instance=instance,
        resources={"corpus": corpus},
    )

    assert result.success
    events = result.get_asset_materialization_events()
    assert len(events) == 1
    assert events[0].materialization.metadata["built"].value is False


# --- industry-facilities + industry-hubs Gold (latest-only; ADR-0056) -------


@pytest.mark.parametrize(
    ("asset", "asset_key"),
    [
        (sde_industry_facilities_gold, "sde_industry_facilities"),
        (sde_industry_hubs_gold, "sde_industry_hubs"),
    ],
)
def test_industry_facility_assets_materialise_against_latest(
    corpus,
    monkeypatch: pytest.MonkeyPatch,
    asset: dg.AssetsDefinition,
    asset_key: str,
) -> None:
    monkeypatch.setenv("FAKE_SDE_BUILDS", BUILDS)
    instance = _instance_with_builds(100)
    _ingest_build(corpus, 100)

    result = dg.materialize(
        [asset],
        instance=instance,
        resources={"corpus": corpus},
    )

    assert result.success
    events = result.get_asset_materialization_events()
    assert len(events) == 1
    assert events[0].asset_key.to_user_string() == asset_key
    assert events[0].materialization.metadata["row_count"].value == 1


@pytest.mark.parametrize(
    "asset",
    [sde_industry_facilities_gold, sde_industry_hubs_gold],
)
def test_industry_facility_assets_skip_when_no_silver_committed(
    corpus, asset: dg.AssetsDefinition
) -> None:
    instance = dg.DagsterInstance.ephemeral()

    result = dg.materialize(
        [asset],
        instance=instance,
        resources={"corpus": corpus},
    )

    assert result.success
    events = result.get_asset_materialization_events()
    assert len(events) == 1
    assert events[0].materialization.metadata["built"].value is False


# --- changelog Gold (baseline skip) ----------------------------------------


def test_changelog_baseline_build_writes_nothing(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The first build has no committed predecessor Silver → no changelog
    # partition; the output is left Missing (ADR-0032).
    monkeypatch.setenv("FAKE_SDE_BUILDS", BUILDS)
    instance = _instance_with_builds(100)
    _ingest_build(corpus, 100)

    result = dg.materialize(
        [sde_changelog_gold],
        partition_key="100",
        instance=instance,
        resources={"corpus": corpus},
    )

    assert result.success
    assert result.get_asset_materialization_events() == []


def test_changelog_with_predecessor_materialises_one_partition(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FAKE_SDE_BUILDS", BUILDS)
    instance = _instance_with_builds(100, 200)
    _ingest_build(corpus, 100)
    _ingest_build(corpus, 200)

    result = dg.materialize(
        [sde_changelog_gold],
        partition_key="200",
        instance=instance,
        resources={"corpus": corpus},
    )

    assert result.success
    events = result.get_asset_materialization_events()
    assert len(events) == 1
    assert events[0].asset_key.to_user_string() == "sde_changelog"


# --- build-discovery sensor ------------------------------------------------


def test_build_discovery_sensor_registers_and_requests(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FAKE_SDE_BUILDS", BUILDS)
    instance = dg.DagsterInstance.ephemeral()
    context = dg.build_sensor_context(resources={"corpus": corpus}, instance=instance)

    result = sde_build_discovery_sensor(context)

    by_partition = {rr.partition_key: rr for rr in result.run_requests}
    assert sorted(by_partition) == ["100", "200"]
    assert by_partition["100"].run_key == "sde-silver-100"
    # New build keys are registered as dynamic partitions in the same tick.
    added = {
        key for req in result.dynamic_partitions_requests for key in req.partition_keys
    }
    assert added == {"100", "200"}


def test_build_discovery_sensor_skips_known_builds(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FAKE_SDE_BUILDS", BUILDS)
    instance = _instance_with_builds(100)
    context = dg.build_sensor_context(resources={"corpus": corpus}, instance=instance)

    result = sde_build_discovery_sensor(context)

    assert [rr.partition_key for rr in result.run_requests] == ["200"]


# --- Gold readiness sensor -------------------------------------------------


def test_gold_sensor_requests_outstanding_builds_only(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The snapshot is non-partitioned + schedule-driven (ADR-0032), so the gold
    # sensor only requests the build-partitioned changelog — and not for build
    # 100, which is the baseline the binary skips.
    monkeypatch.setenv("FAKE_SDE_BUILDS", BUILDS)
    _ingest_build(corpus, 100)
    _ingest_build(corpus, 200)
    instance = _instance_with_builds(100, 200)
    context = dg.build_sensor_context(resources={"corpus": corpus}, instance=instance)

    result = sde_gold_sensor(context)

    assert [rr.partition_key for rr in result.run_requests] == ["200"]
    assert result.run_requests[0].run_key.startswith("sde-changelog-200-")


def test_gold_sensor_skips_builds_whose_changelog_is_committed(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Readiness is the run-state difference, not the sensor's memory: a build
    # whose changelog Gold is committed drops out of the outstanding set.
    monkeypatch.setenv("FAKE_SDE_BUILDS", BUILDS)
    _ingest_build(corpus, 100)
    _ingest_build(corpus, 200)
    _build_changelog(corpus, 200)
    instance = _instance_with_builds(100, 200)
    context = dg.build_sensor_context(resources={"corpus": corpus}, instance=instance)

    result = sde_gold_sensor(context)

    assert result.run_requests == []


def test_gold_sensor_no_silver_yields_no_requests(corpus) -> None:
    instance = dg.DagsterInstance.ephemeral()
    context = dg.build_sensor_context(resources={"corpus": corpus}, instance=instance)

    result = sde_gold_sensor(context)

    assert result.run_requests == []


def test_gold_sensor_single_build_is_baseline(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A cold corpus with one committed build has nothing to diff against, and the
    # binary would report `skipped`. Do not queue that run.
    monkeypatch.setenv("FAKE_SDE_BUILDS", BUILDS)
    _ingest_build(corpus, 100)
    instance = _instance_with_builds(100)
    context = dg.build_sensor_context(resources={"corpus": corpus}, instance=instance)

    result = sde_gold_sensor(context)

    assert result.run_requests == []


def test_gold_sensor_backfilled_build_unblocks_the_old_baseline(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Ingesting below the current baseline gives the old baseline a predecessor,
    # so it becomes outstanding — the rule tracks run-state, not first sighting.
    monkeypatch.setenv("FAKE_SDE_BUILDS", "50:2025-09-01," + BUILDS)
    _ingest_build(corpus, 200)
    instance = _instance_with_builds(50, 100, 200)
    context = dg.build_sensor_context(resources={"corpus": corpus}, instance=instance)
    assert sde_gold_sensor(context).run_requests == []

    _ingest_build(corpus, 50)
    context = dg.build_sensor_context(resources={"corpus": corpus}, instance=instance)

    result = sde_gold_sensor(context)

    assert [rr.partition_key for rr in result.run_requests] == ["200"]


def test_gold_sensor_orders_builds_numerically(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Build keys are numbers; a lexical sort would put "100" before "99" and the
    # per-tick cap would then take the wrong builds.
    monkeypatch.setenv("FAKE_SDE_BUILDS", "98:2025-09-01,99:2025-09-02,100:2025-09-03")
    monkeypatch.setattr(sensor_util, "MAX_PARTITIONS_PER_TICK", 1)
    for build in (98, 99, 100):
        _ingest_build(corpus, build)
    instance = _instance_with_builds(98, 99, 100)
    context = dg.build_sensor_context(resources={"corpus": corpus}, instance=instance)

    result = sde_gold_sensor(context)

    assert [rr.partition_key for rr in result.run_requests] == ["99"]


def test_gold_sensor_drains_a_backlog_larger_than_the_cap(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The regression this row exists for. The cap must bound the work *left*: a
    # sensor that caps every committed build and keys on a static run_key stalls
    # on the first cap-many builds and never reaches the rest.
    cap = sensor_util.MAX_PARTITIONS_PER_TICK
    builds = list(range(100, 100 + cap + 2))
    monkeypatch.setenv(
        "FAKE_SDE_BUILDS",
        ",".join(f"{b}:2025-09-{(i % 28) + 1:02d}" for i, b in enumerate(builds)),
    )
    for build in builds:
        _ingest_build(corpus, build)
    instance = _instance_with_builds(*builds)

    first = sde_gold_sensor(
        dg.build_sensor_context(resources={"corpus": corpus}, instance=instance)
    )
    # Every build but the baseline is outstanding; the cap takes the oldest.
    assert [rr.partition_key for rr in first.run_requests] == [
        str(b) for b in builds[1 : 1 + cap]
    ]

    for build in builds[1 : 1 + cap]:
        _build_changelog(corpus, build)

    second = sde_gold_sensor(
        dg.build_sensor_context(
            resources={"corpus": corpus}, instance=instance, cursor=first.cursor
        )
    )

    assert [rr.partition_key for rr in second.run_requests] == [str(builds[-1])]


def test_gold_sensor_retries_a_build_that_did_not_commit(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A changelog run can fail, or finish green without materialising
    # (output_required=False). The rotating run_key is what lets the next tick
    # ask again; a static one is deduped by Dagster for good.
    monkeypatch.setenv("FAKE_SDE_BUILDS", BUILDS)
    _ingest_build(corpus, 100)
    _ingest_build(corpus, 200)
    instance = _instance_with_builds(100, 200)

    first = sde_gold_sensor(
        dg.build_sensor_context(resources={"corpus": corpus}, instance=instance)
    )
    second = sde_gold_sensor(
        dg.build_sensor_context(
            resources={"corpus": corpus}, instance=instance, cursor=first.cursor
        )
    )

    (first_rr,) = first.run_requests
    (second_rr,) = second.run_requests
    assert first_rr.partition_key == second_rr.partition_key == "200"
    assert first_rr.run_key != second_rr.run_key


def test_gold_sensor_skips_a_build_already_in_flight(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The rotating run_key must not put a second corpus writer on a build whose
    # prior run has not settled.
    monkeypatch.setenv("FAKE_SDE_BUILDS", BUILDS)
    monkeypatch.setattr(
        sensor_util, "_in_flight_partitions", lambda context, asset_key: {"200"}
    )
    _ingest_build(corpus, 100)
    _ingest_build(corpus, 200)
    instance = _instance_with_builds(100, 200)
    context = dg.build_sensor_context(resources={"corpus": corpus}, instance=instance)

    result = sde_gold_sensor(context)

    assert result.run_requests == []


# --- multi-derivative ambiguity (matches the binary) -----------------------


def test_sde_gold_build_without_derivative_fails(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FAKE_SDE_BUILDS", BUILDS)
    _ingest_build(corpus, 100)
    with pytest.raises(dg.Failure):
        corpus.run(
            dg.build_asset_context(),
            "gold",
            "build",
            "--dataset",
            DATASET,
            "--build",
            "100",
            "--sink-path",
            corpus.sink_path,
        )
