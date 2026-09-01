"""Tests for the industry-cost-indices assets, sensor, and live schedule (ADR-0043)."""

from __future__ import annotations

import dagster as dg
import pytest

from eve_industry_orchestration.defs.industry_cost_indices import (
    industry_cost_indices_history_gold,
    industry_cost_indices_silver,
)
from eve_industry_orchestration.defs.industry_cost_indices_live import (
    DATASET as LIVE_DATASET,
)
from eve_industry_orchestration.defs.industry_cost_indices_live import (
    industry_cost_indices_live_gold,
)
from eve_industry_orchestration.defs.sensors import (
    industry_cost_indices_availability_sensor,
    industry_cost_indices_history_gold_sensor,
    industry_cost_indices_live_schedule,
)
from tests.conftest import _assert_enriched

DATASET = "industry-cost-indices"
HISTORY = "industry-cost-indices-history"


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


# --- Silver ---------------------------------------------------------------


def test_silver_skips_absent_upstream_day(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FAKE_EVEREF_DATES", "2024-01-15")

    result = dg.materialize(
        [industry_cost_indices_silver],
        partition_key="2024-01-20",
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
    monkeypatch.setenv("FAKE_EVEREF_DATES", "2024-01-15")

    result = dg.materialize(
        [industry_cost_indices_silver],
        partition_key="2024-01-15",
        resources={"corpus": corpus},
    )

    assert result.success
    (materialisation,) = result.get_asset_materialization_events()
    metadata = materialisation.materialization.metadata
    assert metadata["dataset"].value == DATASET
    assert metadata["partition"].value == "2024-01-15"
    _assert_enriched(metadata)


# --- Silver availability sensor -------------------------------------------


def test_silver_sensor_requests_newly_available_dates(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FAKE_EVEREF_DATES", "2024-01-15,2024-01-16")
    context = dg.build_sensor_context(resources={"corpus": corpus})

    result = industry_cost_indices_availability_sensor(context)

    by_partition = {rr.partition_key: rr for rr in result.run_requests}
    assert sorted(by_partition) == ["2024-01-15", "2024-01-16"]
    assert by_partition["2024-01-15"].run_key.startswith(
        "industry-cost-indices-silver-2024-01-15-"
    )


# --- history Gold readiness sensor ----------------------------------------


def test_history_gold_sensor_requests_ready_dates(corpus) -> None:
    _ingest(corpus, "2024-01-15")
    context = dg.build_sensor_context(resources={"corpus": corpus})

    result = industry_cost_indices_history_gold_sensor(context)

    by_partition = {rr.partition_key: rr for rr in result.run_requests}
    assert sorted(by_partition) == ["2024-01-15"]
    # run_key is keyed on the derivative (its own Gold tree), not the dataset.
    assert by_partition["2024-01-15"].run_key.startswith(
        "industry-cost-indices-history-gold-2024-01-15-"
    )


def test_history_gold_sensor_no_silver_yields_no_requests(corpus) -> None:
    context = dg.build_sensor_context(resources={"corpus": corpus})

    result = industry_cost_indices_history_gold_sensor(context)

    assert result.run_requests == []


def test_history_gold_materialises_ready_day(corpus) -> None:
    _ingest(corpus, "2024-01-15")

    result = dg.materialize(
        [industry_cost_indices_history_gold],
        partition_key="2024-01-15",
        resources={"corpus": corpus},
    )

    assert result.success
    (materialisation,) = result.get_asset_materialization_events()
    metadata = materialisation.materialization.metadata
    assert metadata["derivative"].value == HISTORY
    # Gold run-state is keyed on the derivative tree, not the dataset.
    _assert_enriched(metadata)


def test_history_gold_skips_upstream_gap_day(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FAKE_EVEREF_DATES", "2024-01-15")
    _ingest(corpus, "2024-01-20")  # records the upstream gap

    result = dg.materialize(
        [industry_cost_indices_history_gold],
        partition_key="2024-01-20",
        resources={"corpus": corpus},
    )

    assert result.success
    assert result.get_asset_materialization_events() == []
    observations = result.get_asset_observation_events()
    assert len(observations) == 1
    metadata = observations[0].event_specific_data.asset_observation.metadata
    assert metadata["skip_reason"].value == "upstream_gap"


# --- live current-overwrite asset + schedule ------------------------------


def test_live_asset_overwrites_current(corpus) -> None:
    result = industry_cost_indices_live_gold(dg.build_asset_context(), corpus)

    assert result.metadata["partition"] == "current"
    assert result.metadata["dataset"] == LIVE_DATASET
    assert result.metadata["rows"] == 1
    assert "snapshot_file" in result.metadata


def test_live_asset_is_not_partitioned() -> None:
    assert industry_cost_indices_live_gold.partitions_def is None


def test_live_schedule_targets_the_asset_hourly() -> None:
    assert industry_cost_indices_live_schedule.cron_schedule == "0 * * * *"
    assert (
        industry_cost_indices_live_schedule.default_status
        is dg.DefaultScheduleStatus.STOPPED
    )
