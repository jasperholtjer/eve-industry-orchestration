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
