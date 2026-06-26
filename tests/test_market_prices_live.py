"""Tests for the market-prices-live current-overwrite asset and schedule (ADR-0040)."""

from __future__ import annotations

import dagster as dg

from eve_industry_orchestration.defs.market_prices_live import (
    DATASET,
    market_prices_live_gold,
)
from eve_industry_orchestration.defs.sensors import market_prices_live_schedule


def test_live_asset_overwrites_current(corpus) -> None:
    # No Silver, no ready-dates: the asset just shells `corpus live build`, which
    # the fake binary answers with a `written` status over the flat current/ tree.
    result = market_prices_live_gold(dg.build_asset_context(), corpus)

    assert result.metadata["partition"] == "current"
    assert result.metadata["dataset"] == DATASET
    assert result.metadata["rows"] == 1
    # ESI source surfaces snapshot_at + source (not the everef snapshot_file/date).
    assert result.metadata["source"] == "esi"
    assert "snapshot_at" in result.metadata


def test_live_asset_is_not_partitioned() -> None:
    # The live asset must stay non-partitioned: it always targets current/.
    assert market_prices_live_gold.partitions_def is None


def test_live_asset_joins_no_pool() -> None:
    # ESI, not EVE Ref: the asset must not borrow the everef_download politeness
    # pool, so it obeys only the global concurrency cap.
    assert market_prices_live_gold.op.pool is None


def test_live_schedule_targets_the_asset_hourly() -> None:
    assert market_prices_live_schedule.cron_schedule == "0 * * * *"
    assert (
        market_prices_live_schedule.default_status is dg.DefaultScheduleStatus.STOPPED
    )
