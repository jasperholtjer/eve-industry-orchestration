"""Tests for partition start-date resolution from the dataset config."""

from __future__ import annotations

from pathlib import Path

import pytest

from eve_industry_orchestration.defs.config import (
    PartitionConfigError,
    resolve_partition_starts,
)

DATASETS_DIR = Path(__file__).parent / "fixtures" / "datasets"
DATASET = "market-history"


def test_gold_start_is_served_start_from_config() -> None:
    starts = resolve_partition_starts(DATASET, datasets_dir=str(DATASETS_DIR))
    assert starts.gold == "2021-01-01"


def test_silver_start_is_one_rolling_window_before_gold() -> None:
    # 2021-01-01 minus the 365-day max horizon lands on 2020-01-02 because 2020
    # is a leap year (366 days) — the derived Silver preload start.
    starts = resolve_partition_starts(DATASET, datasets_dir=str(DATASETS_DIR))
    assert starts.silver == "2020-01-02"


def test_env_overrides_both_tiers(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CORPUS_MARKET_HISTORY_GOLD_START", "2022-05-01")
    monkeypatch.setenv("CORPUS_MARKET_HISTORY_SILVER_START", "2021-05-01")
    starts = resolve_partition_starts(DATASET, datasets_dir=str(DATASETS_DIR))
    assert (starts.silver, starts.gold) == ("2021-05-01", "2022-05-01")


def test_missing_config_raises(tmp_path) -> None:
    with pytest.raises(PartitionConfigError):
        resolve_partition_starts(DATASET, datasets_dir=str(tmp_path))


# --- system-jumps: ADR-0025 multi-derivative list shape -------------------

SYSTEM_JUMPS = "system-jumps"
HISTORY = "system-traffic-history"
RECENT = "system-traffic-recent"


def test_history_gold_start_is_per_derivative_served_start() -> None:
    starts = resolve_partition_starts(
        SYSTEM_JUMPS, HISTORY, datasets_dir=str(DATASETS_DIR)
    )
    assert starts.gold == "2022-01-01"


def test_silver_start_clamps_to_upstream_coverage_floor() -> None:
    # The derived preload is 2022-01-01 − 365d = 2021-01-01, but the dataset
    # declares silver.served_start: 2021-07-01 (ADR-0027) — the start of EVE
    # Ref's dense hourly era. Silver clamps up to the floor: max(2021-01-01,
    # 2021-07-01) = 2021-07-01.
    starts = resolve_partition_starts(
        SYSTEM_JUMPS, HISTORY, datasets_dir=str(DATASETS_DIR)
    )
    assert starts.silver == "2021-07-01"


def test_recency_weighted_has_no_gold_start() -> None:
    # The "latest" derivative is non-partitioned; it carries no served_start, so
    # its Gold start resolves to None while Silver stays shared (floor-clamped).
    starts = resolve_partition_starts(
        SYSTEM_JUMPS, RECENT, datasets_dir=str(DATASETS_DIR)
    )
    assert starts.gold is None
    assert starts.silver == "2021-07-01"


def test_silver_env_override_wins_over_coverage_floor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # An explicit operator override beats the silver.served_start clamp, even
    # below the floor — the override is the deliberate escape hatch.
    monkeypatch.setenv("CORPUS_SYSTEM_JUMPS_SILVER_START", "2021-03-01")
    starts = resolve_partition_starts(
        SYSTEM_JUMPS, HISTORY, datasets_dir=str(DATASETS_DIR)
    )
    assert starts.silver == "2021-03-01"


def test_ambiguous_derivative_raises() -> None:
    with pytest.raises(PartitionConfigError):
        resolve_partition_starts(SYSTEM_JUMPS, datasets_dir=str(DATASETS_DIR))


def test_unknown_derivative_raises() -> None:
    with pytest.raises(PartitionConfigError):
        resolve_partition_starts(
            SYSTEM_JUMPS, "no-such-derivative", datasets_dir=str(DATASETS_DIR)
        )


def test_per_derivative_gold_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(
        "CORPUS_SYSTEM_JUMPS_SYSTEM_TRAFFIC_HISTORY_GOLD_START", "2023-03-01"
    )
    starts = resolve_partition_starts(
        SYSTEM_JUMPS, HISTORY, datasets_dir=str(DATASETS_DIR)
    )
    assert starts.gold == "2023-03-01"


# --- industry-cost-indices: single cost-index-history derivative (ADR-0043) -

INDUSTRY_COST_INDICES = "industry-cost-indices"
COST_INDEX_HISTORY = "industry-cost-indices-history"


def test_cost_index_history_gold_start_is_served_start() -> None:
    starts = resolve_partition_starts(
        INDUSTRY_COST_INDICES, COST_INDEX_HISTORY, datasets_dir=str(DATASETS_DIR)
    )
    assert starts.gold == "2022-01-01"


def test_cost_index_silver_clamps_to_coverage_floor() -> None:
    # Derived preload is 2022-01-01 − 365d = 2021-01-01, but the dataset declares
    # silver.served_start: 2021-07-01 (ADR-0027), so Silver clamps up to the floor.
    starts = resolve_partition_starts(
        INDUSTRY_COST_INDICES, COST_INDEX_HISTORY, datasets_dir=str(DATASETS_DIR)
    )
    assert starts.silver == "2021-07-01"


def test_cost_index_single_derivative_resolves_without_selector() -> None:
    # Exactly one derivative → the selector may be omitted, like market-history.
    starts = resolve_partition_starts(
        INDUSTRY_COST_INDICES, datasets_dir=str(DATASETS_DIR)
    )
    assert (starts.silver, starts.gold) == ("2021-07-01", "2022-01-01")


# --- market-orders: split orderbook shapes (ADR-0036) ---------------------

MARKET_ORDERS = "market-orders"


@pytest.mark.parametrize(
    "derivative",
    ["market-orders-snapshot", "market-orders-changes", "market-orders-events"],
)
def test_orderbook_gold_start_is_served_start(derivative: str) -> None:
    starts = resolve_partition_starts(
        MARKET_ORDERS, derivative, datasets_dir=str(DATASETS_DIR)
    )
    assert starts.gold == "2021-07-09"


@pytest.mark.parametrize(
    "derivative",
    ["market-orders-snapshot", "market-orders-changes", "market-orders-events"],
)
def test_orderbook_silver_clamps_one_day_lookback_to_floor(derivative: str) -> None:
    # Both shapes look back one day (2021-07-09 − 1d = 2021-07-08), but
    # silver.served_start is 2021-07-09 (ADR-0027/0036), so Silver clamps up:
    # max(2021-07-08, 2021-07-09) = 2021-07-09.
    starts = resolve_partition_starts(
        MARKET_ORDERS, derivative, datasets_dir=str(DATASETS_DIR)
    )
    assert starts.silver == "2021-07-09"


def test_orderbook_ambiguous_without_selector() -> None:
    # Three derivatives (ADR-0036/0042), so a selector is required.
    with pytest.raises(PartitionConfigError):
        resolve_partition_starts(MARKET_ORDERS, datasets_dir=str(DATASETS_DIR))


# --- system-kills: per-measure kills shapes (ADR-0037) --------------------

SYSTEM_KILLS = "system-kills"


@pytest.mark.parametrize(
    "derivative",
    [
        "system-kills-ship-history",
        "system-kills-npc-history",
        "system-kills-pod-history",
    ],
)
def test_kills_history_gold_start_is_served_start(derivative: str) -> None:
    starts = resolve_partition_starts(
        SYSTEM_KILLS, derivative, datasets_dir=str(DATASETS_DIR)
    )
    assert starts.gold == "2022-01-01"


def test_kills_silver_clamps_to_upstream_coverage_floor() -> None:
    # Derived preload is 2022-01-01 − 365d = 2021-01-01, but silver.served_start
    # is 2021-07-01 (ADR-0027), so Silver clamps up to the floor.
    starts = resolve_partition_starts(
        SYSTEM_KILLS, "system-kills-ship-history", datasets_dir=str(DATASETS_DIR)
    )
    assert starts.silver == "2021-07-01"


@pytest.mark.parametrize(
    "derivative",
    ["system-kills-ship-recent", "system-kills-npc-recent", "system-kills-pod-recent"],
)
def test_kills_recent_has_no_gold_start(derivative: str) -> None:
    # The kills-recent EWMA derivatives are non-partitioned; no served_start, so
    # Gold resolves to None while Silver stays shared (floor-clamped).
    starts = resolve_partition_starts(
        SYSTEM_KILLS, derivative, datasets_dir=str(DATASETS_DIR)
    )
    assert starts.gold is None
    assert starts.silver == "2021-07-01"


def test_kills_ambiguous_without_selector() -> None:
    with pytest.raises(PartitionConfigError):
        resolve_partition_starts(SYSTEM_KILLS, datasets_dir=str(DATASETS_DIR))
