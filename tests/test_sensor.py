"""Tests for the market-history availability sensor."""

from __future__ import annotations

import dagster as dg
import pytest

from eve_industry_orchestration.defs.sensors import (
    market_history_availability_sensor,
)


def test_requests_runs_for_newly_available_dates(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FAKE_EVEREF_DATES", "2024-01-15,2024-01-16")
    context = dg.build_sensor_context(resources={"corpus": corpus})

    result = market_history_availability_sensor(context)

    by_partition = {rr.partition_key: rr for rr in result.run_requests}
    assert sorted(by_partition) == ["2024-01-15", "2024-01-16"]
    assert all(rr.run_key for rr in result.run_requests)
    assert by_partition["2024-01-15"].run_key == "market-history-silver-2024-01-15"


def test_excludes_already_materialised_dates(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Status is keyed on corpus run-state: ingesting one date records it in the
    # state file, so missing-partitions (and thus the sensor) drops it.
    monkeypatch.setenv("FAKE_EVEREF_DATES", "2024-01-15,2024-01-16")
    asset_context = dg.build_asset_context()
    corpus.run(
        asset_context,
        "ingest",
        "--dataset",
        "market-history",
        "--date",
        "2024-01-15",
        "--sink-path",
        corpus.sink_path,
    )

    context = dg.build_sensor_context(resources={"corpus": corpus})
    result = market_history_availability_sensor(context)

    assert [rr.partition_key for rr in result.run_requests] == ["2024-01-16"]


def test_no_missing_dates_yields_no_requests(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FAKE_EVEREF_DATES", "")
    context = dg.build_sensor_context(resources={"corpus": corpus})

    result = market_history_availability_sensor(context)

    assert result.run_requests == []
