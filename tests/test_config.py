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
    assert starts.gold == "2021-01-01"


def test_silver_start_is_earliest_windowed_preload() -> None:
    # flat-multi-horizon max horizon is 365d; 2021-01-01 − 365d = 2020-01-02
    # (2020 leap year). The recency-weighted derivative has no served_start and
    # imposes no reach-back, so Silver is driven by the history derivative.
    starts = resolve_partition_starts(
        SYSTEM_JUMPS, HISTORY, datasets_dir=str(DATASETS_DIR)
    )
    assert starts.silver == "2020-01-02"


def test_recency_weighted_has_no_gold_start() -> None:
    # The "latest" derivative is non-partitioned; it carries no served_start, so
    # its Gold start resolves to None while Silver stays shared.
    starts = resolve_partition_starts(
        SYSTEM_JUMPS, RECENT, datasets_dir=str(DATASETS_DIR)
    )
    assert starts.gold is None
    assert starts.silver == "2020-01-02"


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
