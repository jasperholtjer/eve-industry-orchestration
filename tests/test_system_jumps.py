"""Tests for the system-jumps multi-derivative assets, sensors, and schedule."""

from __future__ import annotations

import dagster as dg
import pytest

from eve_industry_orchestration.defs.sensors import (
    system_jumps_availability_sensor,
    system_jumps_history_gold_sensor,
)
from eve_industry_orchestration.defs.system_jumps import (
    system_jumps_recent_gold,
)

DATASET = "system-jumps"
HISTORY = "system-traffic-history"


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


# --- Silver availability sensor -------------------------------------------


def test_silver_sensor_requests_newly_available_dates(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FAKE_EVEREF_DATES", "2024-01-15,2024-01-16")
    context = dg.build_sensor_context(resources={"corpus": corpus})

    result = system_jumps_availability_sensor(context)

    by_partition = {rr.partition_key: rr for rr in result.run_requests}
    assert sorted(by_partition) == ["2024-01-15", "2024-01-16"]
    assert by_partition["2024-01-15"].run_key == "system-jumps-silver-2024-01-15"


# --- history Gold readiness sensor ----------------------------------------


def test_history_gold_sensor_requests_ready_dates(corpus) -> None:
    _ingest(corpus, "2024-01-15")
    context = dg.build_sensor_context(resources={"corpus": corpus})

    result = system_jumps_history_gold_sensor(context)

    by_partition = {rr.partition_key: rr for rr in result.run_requests}
    assert sorted(by_partition) == ["2024-01-15"]
    # run_key is keyed on the derivative (its own Gold tree), not the dataset.
    assert (
        by_partition["2024-01-15"].run_key == "system-traffic-history-gold-2024-01-15"
    )


def test_history_gold_sensor_no_silver_yields_no_requests(corpus) -> None:
    context = dg.build_sensor_context(resources={"corpus": corpus})

    result = system_jumps_history_gold_sensor(context)

    assert result.run_requests == []


# --- recency-weighted "recent" asset --------------------------------------


def test_recent_asset_builds_latest_ready_date(corpus) -> None:
    _ingest(corpus, "2024-01-15")
    _ingest(corpus, "2024-01-16")
    context = dg.build_asset_context()

    result = system_jumps_recent_gold(context, corpus)

    assert result.metadata["built"] is True
    assert result.metadata["partition"] == "2024-01-16"


def test_recent_asset_noop_without_ready(corpus) -> None:
    context = dg.build_asset_context()

    result = system_jumps_recent_gold(context, corpus)

    assert result.metadata["built"] is False


# --- multi-derivative ambiguity (matches the binary's error) --------------


def test_gold_build_without_derivative_fails(corpus) -> None:
    _ingest(corpus, "2024-01-15")
    context = dg.build_asset_context()
    with pytest.raises(dg.Failure):
        corpus.run(
            context,
            "gold",
            "build",
            "--dataset",
            DATASET,
            "--date",
            "2024-01-15",
            "--sink-path",
            corpus.sink_path,
        )
