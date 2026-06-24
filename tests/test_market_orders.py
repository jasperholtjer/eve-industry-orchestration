"""Tests for the market-orders split Gold assets and sensors (ADR-0036)."""

from __future__ import annotations

import dagster as dg
import pytest

from eve_industry_orchestration.defs.market_orders import (
    market_orders_changes_gold,
    market_orders_silver,
    market_orders_snapshot_gold,
)
from eve_industry_orchestration.defs.sensors import (
    market_orders_availability_sensor,
    market_orders_changes_gold_sensor,
    market_orders_snapshot_gold_sensor,
)

DATASET = "market-orders"

# (gold asset, gold sensor, derivative name) per split derivative.
_GOLD_CASES = [
    (
        market_orders_snapshot_gold,
        market_orders_snapshot_gold_sensor,
        "orderbook-snapshot",
    ),
    (
        market_orders_changes_gold,
        market_orders_changes_gold_sensor,
        "orderbook-changes",
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
    monkeypatch.setenv("FAKE_EVEREF_DATES", "2021-07-09")

    result = dg.materialize(
        [market_orders_silver],
        partition_key="2021-07-12",
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
    monkeypatch.setenv("FAKE_EVEREF_DATES", "2021-07-09")

    result = dg.materialize(
        [market_orders_silver],
        partition_key="2021-07-09",
        resources={"corpus": corpus},
    )

    assert result.success
    assert len(result.get_asset_materialization_events()) == 1


# --- Silver availability sensor -------------------------------------------


def test_silver_sensor_requests_newly_available_dates(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FAKE_EVEREF_DATES", "2021-07-09,2021-07-10")
    context = dg.build_sensor_context(resources={"corpus": corpus})

    result = market_orders_availability_sensor(context)

    by_partition = {rr.partition_key: rr for rr in result.run_requests}
    assert sorted(by_partition) == ["2021-07-09", "2021-07-10"]
    assert by_partition["2021-07-09"].run_key == "market-orders-silver-2021-07-09"


# --- Gold readiness sensors (one per derivative) --------------------------


@pytest.mark.parametrize(("_asset", "sensor", "derivative"), _GOLD_CASES)
def test_gold_sensor_requests_ready_dates(corpus, _asset, sensor, derivative) -> None:
    _ingest(corpus, "2021-07-09")
    context = dg.build_sensor_context(resources={"corpus": corpus})

    result = sensor(context)

    by_partition = {rr.partition_key: rr for rr in result.run_requests}
    assert sorted(by_partition) == ["2021-07-09"]
    # run_key is keyed on the derivative (its own Gold tree), not the dataset.
    assert by_partition["2021-07-09"].run_key == f"{derivative}-gold-2021-07-09"


@pytest.mark.parametrize(("_asset", "sensor", "_derivative"), _GOLD_CASES)
def test_gold_sensor_no_silver_yields_no_requests(
    corpus, _asset, sensor, _derivative
) -> None:
    context = dg.build_sensor_context(resources={"corpus": corpus})

    result = sensor(context)

    assert result.run_requests == []


@pytest.mark.parametrize(("asset", "_sensor", "_derivative"), _GOLD_CASES)
def test_gold_builds_and_verifies_on_derivative_tree(
    corpus, asset, _sensor, _derivative
) -> None:
    _ingest(corpus, "2021-07-09")

    result = dg.materialize(
        [asset],
        partition_key="2021-07-09",
        resources={"corpus": corpus},
    )

    assert result.success
    assert len(result.get_asset_materialization_events()) == 1


@pytest.mark.parametrize(("asset", "_sensor", "_derivative"), _GOLD_CASES)
def test_gold_skips_upstream_gap_day(
    corpus, monkeypatch: pytest.MonkeyPatch, asset, _sensor, _derivative
) -> None:
    monkeypatch.setenv("FAKE_EVEREF_DATES", "2021-07-09")
    _ingest(corpus, "2021-07-12")  # records the upstream gap

    result = dg.materialize(
        [asset],
        partition_key="2021-07-12",
        resources={"corpus": corpus},
    )

    assert result.success
    assert result.get_asset_materialization_events() == []
    observations = result.get_asset_observation_events()
    assert len(observations) == 1
    metadata = observations[0].event_specific_data.asset_observation.metadata
    assert metadata["skip_reason"].value == "upstream_gap"
