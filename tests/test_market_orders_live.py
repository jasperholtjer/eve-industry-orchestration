"""Tests for the market-orders-live current-overwrite asset and schedule (ADR-0039)."""

from __future__ import annotations

import dagster as dg

from eve_industry_orchestration.defs.market_orders_live import (
    DATASET,
    market_orders_live_gold,
)
from eve_industry_orchestration.defs.sensors import market_orders_live_schedule


def test_live_asset_overwrites_current(corpus) -> None:
    # No Silver, no ready-dates: the asset just shells `corpus live build`, which
    # the fake binary answers with a `written` status over the flat current/ tree.
    result = market_orders_live_gold(dg.build_asset_context(), corpus)

    assert result.metadata["partition"] == "current"
    assert result.metadata["dataset"] == DATASET
    assert result.metadata["rows"] == 1
    assert "snapshot_file" in result.metadata


def test_live_asset_is_not_partitioned() -> None:
    # The live asset must stay non-partitioned: it always targets current/.
    assert market_orders_live_gold.partitions_def is None


def test_live_schedule_targets_the_asset_half_hourly() -> None:
    assert market_orders_live_schedule.cron_schedule == "*/30 * * * *"
    assert (
        market_orders_live_schedule.default_status is dg.DefaultScheduleStatus.STOPPED
    )


def test_live_asset_issues_no_state_query(corpus, monkeypatch) -> None:
    # `corpus live build` writes no run-state row (see the asset body), so the
    # metadata-enrichment row deliberately leaves this site unenriched. Pin that:
    # a later drive-by `partition_metadata` call here would match no row and warn
    # on every scheduled run.
    from eve_industry_orchestration.defs.corpus_resource import CorpusResource

    def _fail(self, sql: str):  # pragma: no cover - must never run
        raise AssertionError(f"live asset queried run-state: {sql}")

    monkeypatch.setattr(CorpusResource, "state_query", _fail)
    result = market_orders_live_gold(dg.build_asset_context(), corpus)

    assert result.metadata["partition"] == "current"
    assert "retention_class" not in result.metadata
