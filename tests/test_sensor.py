"""Tests for the market-history availability sensor."""

from __future__ import annotations

import dagster as dg
import pytest

from eve_industry_orchestration.defs.sensors import (
    market_history_availability_sensor,
    market_history_gold_sensor,
)


def _ingest(corpus, date: str) -> None:
    corpus.run(
        dg.build_asset_context(),
        "ingest",
        "--dataset",
        "market-history",
        "--date",
        date,
        "--sink-path",
        corpus.sink_path,
    )


def _build_gold(corpus, date: str) -> None:
    corpus.run(
        dg.build_asset_context(),
        "gold",
        "build",
        "--dataset",
        "market-history",
        "--date",
        date,
        "--sink-path",
        corpus.sink_path,
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


def test_gold_sensor_requests_ready_dates(corpus) -> None:
    # A Silver partition whose window is complete (per the binary) shows up as
    # ready until its Gold partition is built.
    _ingest(corpus, "2024-01-15")
    context = dg.build_sensor_context(resources={"corpus": corpus})

    result = market_history_gold_sensor(context)

    by_partition = {rr.partition_key: rr for rr in result.run_requests}
    assert sorted(by_partition) == ["2024-01-15"]
    assert by_partition["2024-01-15"].run_key == "market-history-gold-2024-01-15"


def test_gold_sensor_excludes_already_built_dates(corpus) -> None:
    _ingest(corpus, "2024-01-15")
    _build_gold(corpus, "2024-01-15")
    context = dg.build_sensor_context(resources={"corpus": corpus})

    result = market_history_gold_sensor(context)

    assert result.run_requests == []


def test_gold_sensor_no_silver_yields_no_requests(corpus) -> None:
    context = dg.build_sensor_context(resources={"corpus": corpus})

    result = market_history_gold_sensor(context)

    assert result.run_requests == []
