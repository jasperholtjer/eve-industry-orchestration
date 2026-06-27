"""Tests for the market-history Silver completeness handling (ADR-0041)."""

from __future__ import annotations

import dagster as dg
import pytest

from eve_industry_orchestration.defs.market_history import market_history_silver

# Well within the resolved Silver partition range (gold served_start minus one
# rolling window), and a real day in the polluted cohort the gate targets.
_DATE = "2026-06-24"


def test_silver_leaves_incomplete_day_missing(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An incomplete upstream publication leaves the partition Missing, retryable."""
    monkeypatch.setenv("FAKE_INCOMPLETE_DATES", _DATE)

    result = dg.materialize(
        [market_history_silver],
        partition_key=_DATE,
        resources={"corpus": corpus},
    )

    assert result.success
    # No materialisation — the partition stays Missing and is re-proposed.
    assert result.get_asset_materialization_events() == []
    observations = result.get_asset_observation_events()
    assert len(observations) == 1
    metadata = observations[0].event_specific_data.asset_observation.metadata
    assert metadata["skip_reason"].value == "upstream_incomplete"


def test_silver_materialises_settled_day(corpus) -> None:
    """A settled day (no incomplete flag) materialises normally."""
    result = dg.materialize(
        [market_history_silver],
        partition_key=_DATE,
        resources={"corpus": corpus},
    )

    assert result.success
    materializations = result.get_asset_materialization_events()
    assert len(materializations) == 1
