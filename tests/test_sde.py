"""Tests for the build-versioned SDE assets and sensors (ADR-0030/0031)."""

from __future__ import annotations

import dagster as dg
import pytest

from eve_industry_orchestration.defs import sde
from eve_industry_orchestration.defs.config import sde_entities, sde_gold_derivatives
from eve_industry_orchestration.defs.sde import (
    ENTITIES,
    sde_changelog_gold,
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
    ]


# --- Silver multi_asset ----------------------------------------------------


def test_silver_materialises_every_entity_for_a_build(
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
    assert materialised == {f"sde_silver_{e}" for e in ENTITIES}


# --- snapshot Gold (always written) ----------------------------------------


def test_snapshot_materialises_every_entity(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FAKE_SDE_BUILDS", BUILDS)
    instance = _instance_with_builds(100)
    _ingest_build(corpus, 100)

    result = dg.materialize(
        [sde_snapshot_gold],
        partition_key="100",
        instance=instance,
        resources={"corpus": corpus},
    )

    assert result.success
    assert len(result.get_asset_materialization_events()) == len(ENTITIES)


# --- changelog Gold (baseline skip) ----------------------------------------


def test_changelog_baseline_build_writes_nothing(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The first build has no committed predecessor Silver → no changelog
    # partition; every entity output is left Missing (ADR-0031).
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


def test_changelog_with_predecessor_materialises_every_entity(
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
    assert len(result.get_asset_materialization_events()) == len(ENTITIES)


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


def test_gold_sensor_requests_both_derivatives_for_committed_builds(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FAKE_SDE_BUILDS", BUILDS)
    _ingest_build(corpus, 100)
    _ingest_build(corpus, 200)
    instance = _instance_with_builds(100, 200)
    context = dg.build_sensor_context(resources={"corpus": corpus}, instance=instance)

    result = sde_gold_sensor(context)

    run_keys = sorted(rr.run_key for rr in result.run_requests)
    assert run_keys == [
        "sde-changelog-100",
        "sde-changelog-200",
        "sde-snapshot-100",
        "sde-snapshot-200",
    ]


def test_gold_sensor_no_silver_yields_no_requests(corpus) -> None:
    instance = dg.DagsterInstance.ephemeral()
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
