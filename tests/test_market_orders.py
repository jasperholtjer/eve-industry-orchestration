"""Tests for the market-orders orderbook-aggregate assets and sensors (ADR-0033)."""

from __future__ import annotations

import dagster as dg
import pytest

from eve_industry_orchestration.defs.market_orders import (
    market_orders_gold,
    market_orders_silver,
)
from eve_industry_orchestration.defs.sensors import (
    market_orders_availability_sensor,
    market_orders_gold_sensor,
)

DATASET = "market-orders"
DERIVATIVE = "orderbook-sweep"


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


# --- Gold readiness sensor ------------------------------------------------


def test_gold_sensor_requests_ready_dates(corpus) -> None:
    _ingest(corpus, "2021-07-09")
    context = dg.build_sensor_context(resources={"corpus": corpus})

    result = market_orders_gold_sensor(context)

    by_partition = {rr.partition_key: rr for rr in result.run_requests}
    assert sorted(by_partition) == ["2021-07-09"]
    # run_key is keyed on the derivative (its own Gold tree), not the dataset.
    assert by_partition["2021-07-09"].run_key == "orderbook-sweep-gold-2021-07-09"


def test_gold_sensor_no_silver_yields_no_requests(corpus) -> None:
    context = dg.build_sensor_context(resources={"corpus": corpus})

    result = market_orders_gold_sensor(context)

    assert result.run_requests == []


def test_gold_builds_and_verifies_on_derivative_tree(corpus) -> None:
    _ingest(corpus, "2021-07-09")

    result = dg.materialize(
        [market_orders_gold],
        partition_key="2021-07-09",
        resources={"corpus": corpus},
    )

    assert result.success
    materialisations = result.get_asset_materialization_events()
    assert len(materialisations) == 1


def test_gold_skips_upstream_gap_day(corpus, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FAKE_EVEREF_DATES", "2021-07-09")
    _ingest(corpus, "2021-07-12")  # records the upstream gap

    result = dg.materialize(
        [market_orders_gold],
        partition_key="2021-07-12",
        resources={"corpus": corpus},
    )

    assert result.success
    assert result.get_asset_materialization_events() == []
    observations = result.get_asset_observation_events()
    assert len(observations) == 1
    metadata = observations[0].event_specific_data.asset_observation.metadata
    assert metadata["skip_reason"].value == "upstream_gap"
