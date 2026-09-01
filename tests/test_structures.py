"""Tests for the structures assets and sensors (corpus ADR-0057/0062)."""

from __future__ import annotations

import dagster as dg
import pytest

from eve_industry_orchestration.defs import sensors as s
from eve_industry_orchestration.defs import structures as st
from tests.conftest import _assert_enriched

DATASET = "structures"

# (Gold asset, its readiness sensor, derivative name) per derivative.
_GOLD_CASES = [
    (
        st.structures_snapshot_gold,
        s.structures_snapshot_gold_sensor,
        "structures-snapshot",
    ),
    (
        st.structure_population_history_gold,
        s.structure_population_history_gold_sensor,
        "structure-population-history",
    ),
]


def _ingest(corpus, date: str) -> None:
    corpus.run(
        dg.build_asset_context(),
        "ingest",
        "--dataset",
        DATASET,
        "--date",
        date,
        "--sink-path",
        corpus.sink_path,
    )


# --- Silver ingest skip on absent upstream day (ADR-0028) -----------------


def test_silver_skips_absent_upstream_day(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FAKE_EVEREF_DATES", "2024-05-15")

    result = dg.materialize(
        [st.structures_silver],
        partition_key="2024-05-20",
        resources={"corpus": corpus},
    )

    assert result.success
    assert result.get_asset_materialization_events() == []
    observations = result.get_asset_observation_events()
    assert len(observations) == 1
    metadata = observations[0].event_specific_data.asset_observation.metadata
    assert metadata["skip_reason"].value == "upstream_absent"


def test_silver_materialises_present_upstream_day(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FAKE_EVEREF_DATES", "2024-05-15")

    result = dg.materialize(
        [st.structures_silver],
        partition_key="2024-05-15",
        resources={"corpus": corpus},
    )

    assert result.success
    (materialisation,) = result.get_asset_materialization_events()
    metadata = materialisation.materialization.metadata
    assert metadata["dataset"].value == DATASET
    assert metadata["partition"].value == "2024-05-15"
    _assert_enriched(metadata)


# --- Silver availability sensor -------------------------------------------


def test_silver_sensor_requests_newly_available_dates(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FAKE_EVEREF_DATES", "2024-05-15,2024-05-16")
    context = dg.build_sensor_context(resources={"corpus": corpus})

    result = s.structures_availability_sensor(context)

    by_partition = {rr.partition_key: rr for rr in result.run_requests}
    assert sorted(by_partition) == ["2024-05-15", "2024-05-16"]
    assert by_partition["2024-05-15"].run_key.startswith(
        "structures-silver-2024-05-15-"
    )


# --- Gold readiness sensors (one per derivative) --------------------------


@pytest.mark.parametrize(("_asset", "sensor", "derivative"), _GOLD_CASES)
def test_gold_sensor_requests_ready_dates(corpus, _asset, sensor, derivative) -> None:
    _ingest(corpus, "2024-05-15")
    context = dg.build_sensor_context(resources={"corpus": corpus})

    result = sensor(context)

    by_partition = {rr.partition_key: rr for rr in result.run_requests}
    assert sorted(by_partition) == ["2024-05-15"]
    assert by_partition["2024-05-15"].run_key.startswith(
        f"{derivative}-gold-2024-05-15-"
    )


@pytest.mark.parametrize(("asset", "_sensor", "derivative"), _GOLD_CASES)
def test_gold_materialises_ready_day(corpus, asset, _sensor, derivative) -> None:
    _ingest(corpus, "2024-05-15")

    result = dg.materialize(
        [asset],
        partition_key="2024-05-15",
        selection=[asset],
        resources={"corpus": corpus},
    )

    assert result.success
    (materialisation,) = result.get_asset_materialization_events()
    metadata = materialisation.materialization.metadata
    assert metadata["derivative"].value == derivative
    # Gold run-state is keyed on the derivative tree, not the dataset.
    _assert_enriched(metadata)


@pytest.mark.parametrize(("asset", "_sensor", "_derivative"), _GOLD_CASES)
def test_gold_skips_upstream_gap_day(
    corpus, monkeypatch: pytest.MonkeyPatch, asset, _sensor, _derivative
) -> None:
    monkeypatch.setenv("FAKE_EVEREF_DATES", "2024-05-15")
    _ingest(corpus, "2024-05-20")  # records the upstream gap

    result = dg.materialize(
        [asset],
        partition_key="2024-05-20",
        selection=[asset],
        resources={"corpus": corpus},
    )

    assert result.success
    assert result.get_asset_materialization_events() == []
    observations = result.get_asset_observation_events()
    assert len(observations) == 1
    metadata = observations[0].event_specific_data.asset_observation.metadata
    assert metadata["skip_reason"].value == "upstream_gap"


# --- the two Gold matrices deliberately differ ----------------------------


def test_gold_partition_matrices_have_distinct_starts() -> None:
    # The dimension serves a month earlier than the covariate (which needs its
    # 30-day reference day), so each sensor validates its own matrix.
    assert st.snapshot_gold_partitions.start.strftime("%Y-%m-%d") == "2024-03-31"
    assert st.population_gold_partitions.start.strftime("%Y-%m-%d") == "2024-04-30"


# --- the SDE class map is a real upstream, not lineage decoration ---------


def test_gold_assets_depend_on_the_sde_snapshot() -> None:
    from eve_industry_orchestration.defs import sde

    for asset, _sensor, _derivative in _GOLD_CASES:
        dep_keys = {dep.asset_key for dep in asset.op.ins.values()} | {
            key for key in asset.asset_deps[asset.key]
        }
        assert sde.sde_snapshot_gold.key in dep_keys
        assert st.structures_silver.key in dep_keys


# --- multi-derivative ambiguity (matches the binary's error) --------------


def test_gold_build_without_derivative_fails(corpus) -> None:
    _ingest(corpus, "2024-05-15")
    context = dg.build_asset_context()
    with pytest.raises(dg.Failure):
        corpus.run(
            context,
            "gold",
            "build",
            "--dataset",
            DATASET,
            "--date",
            "2024-05-15",
            "--sink-path",
            corpus.sink_path,
        )
