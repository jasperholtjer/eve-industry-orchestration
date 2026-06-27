"""Tests for the serving-tier load assets, resource, job, and schedule."""

from __future__ import annotations

import dagster as dg
import pytest

from eve_industry_orchestration.defs.serving import (
    serving_load_job,
    serving_load_market_history,
    serving_load_market_orders_live,
    serving_load_market_prices_live,
    serving_load_schedule,
    serving_load_sde,
)

_MARKET_ASSETS = (
    serving_load_market_history,
    serving_load_market_orders_live,
    serving_load_market_prices_live,
)
_ALL_ASSETS = [serving_load_sde, *_MARKET_ASSETS]


def _actions(result: dg.ExecuteInProcessResult) -> dict[str, str]:
    """Maps each materialised serving asset to its loaded/skipped action."""
    return {
        e.asset_key.path[-1]: e.materialization.metadata["action"].value
        for e in result.get_asset_materialization_events()
    }


# --- resource: summary parsing + exit handling -----------------------------


def test_resource_parses_loaded_summary(serving) -> None:
    status = serving.load(dg.build_asset_context(), "sde", "--latest")
    assert status == {"action": "loaded", "rows": 4096}


def test_resource_second_load_reports_skipped(serving) -> None:
    serving.load(dg.build_asset_context(), "market-history")
    status = serving.load(dg.build_asset_context(), "market-history")
    assert status == {"action": "skipped", "rows": 0}


def test_resource_nonzero_exit_raises_failure(serving) -> None:
    # A missing --dataset makes the fake loader exit non-zero.
    with pytest.raises(dg.Failure):
        serving.load(dg.build_asset_context(), "")


# --- assets: shim + idempotency --------------------------------------------


def test_sde_load_asset_materialises(serving) -> None:
    result = dg.materialize([serving_load_sde], resources={"serving": serving})
    assert result.success
    events = result.get_asset_materialization_events()
    assert len(events) == 1
    meta = events[0].materialization.metadata
    assert meta["dataset"].value == "sde"
    assert meta["action"].value == "loaded"
    assert meta["host"].value == "serving@192.168.2.212"


@pytest.mark.parametrize("asset", _MARKET_ASSETS)
def test_market_load_asset_idempotent(asset, serving) -> None:
    first = dg.materialize([asset], resources={"serving": serving})
    assert first.success
    second = dg.materialize([asset], resources={"serving": serving})
    assert second.success
    action = second.get_asset_materialization_events()[0].materialization.metadata[
        "action"
    ]
    assert action.value == "skipped"


# --- acceptance: SDE → market ordering + rebuild ----------------------------


def test_loads_run_sde_then_markets_idempotently(serving) -> None:
    # First pass: materialising the four together executes in dep order (SDE
    # first), and every load lands fresh.
    first = dg.materialize(_ALL_ASSETS, resources={"serving": serving})
    assert first.success
    actions = _actions(first)
    assert actions["serving_load_sde"] == "loaded"
    assert all(action == "loaded" for action in actions.values())

    # Second pass, unchanged Gold: everything skips.
    second = dg.materialize(_ALL_ASSETS, resources={"serving": serving})
    assert second.success
    assert all(action == "skipped" for action in _actions(second).values())


def test_sde_rebuild_reloads_markets(serving, monkeypatch: pytest.MonkeyPatch) -> None:
    # Prime: SDE + markets all loaded.
    assert dg.materialize(_ALL_ASSETS, resources={"serving": serving}).success

    # An SDE rebuild (new sha) TRUNCATEs market.* and clears their load-state, so
    # the SDE load re-runs and the markets re-load even though their own sha is
    # unchanged.
    monkeypatch.setenv("FAKE_SERVING_SHA_SDE", "2")
    result = dg.materialize(_ALL_ASSETS, resources={"serving": serving})
    assert result.success
    actions = _actions(result)
    assert actions["serving_load_sde"] == "loaded"
    assert actions["serving_load_market_history"] == "loaded"
    assert actions["serving_load_market_orders_live"] == "loaded"
    assert actions["serving_load_market_prices_live"] == "loaded"


# --- wiring: deps + non-partitioned + schedule -----------------------------


def test_assets_are_non_partitioned() -> None:
    for asset in (serving_load_sde, *_MARKET_ASSETS):
        assert asset.partitions_def is None


def test_market_loads_depend_on_sde_load() -> None:
    sde_key = serving_load_sde.key
    for asset in _MARKET_ASSETS:
        upstream = set(asset.asset_deps[asset.key])
        assert sde_key in upstream


def test_schedule_targets_job_hourly() -> None:
    assert serving_load_job.name == "serving_load_job"
    assert serving_load_schedule.cron_schedule == "0 * * * *"
    assert serving_load_schedule.default_status is dg.DefaultScheduleStatus.STOPPED
