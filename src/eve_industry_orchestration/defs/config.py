"""Resolve partition start dates from the corpus dataset config.

Corpus owns the dataset config (``datasets/<name>.yaml``); orchestration reads
``gold.served_start`` to anchor the partition matrix instead of hardcoding it.
Silver must reach back one full rolling window before the earliest Gold date so
the first Gold partition has its window present, hence Silver and Gold get
distinct start dates. The window length is the largest configured rolling
horizon (``gold.rolling.horizon_52w`` for market-history).
"""

from __future__ import annotations

import datetime as dt
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

_DATE_FORMAT = "%Y-%m-%d"


class PartitionConfigError(RuntimeError):
    """Raised when the dataset config cannot yield partition start dates."""


@dataclass(frozen=True)
class PartitionStarts:
    """Inclusive first partition date for each tier, as ``YYYY-MM-DD``."""

    silver: str
    gold: str


def resolve_partition_starts(
    dataset: str, datasets_dir: str | None = None
) -> PartitionStarts:
    """Resolves the Silver and Gold partition start dates for a dataset.

    Reads ``gold.served_start`` (Gold start) from the dataset YAML and derives
    the Silver start by subtracting the largest rolling horizon. Either tier
    can be overridden via ``CORPUS_<DATASET>_<TIER>_START`` so an operator can
    pin a narrower matrix without touching the corpus config.

    Args:
        dataset: Dataset name, e.g. ``market-history``.
        datasets_dir: Directory holding ``<dataset>.yaml``. Falls back to the
            ``CORPUS_DATASETS_DIR`` environment variable.

    Returns:
        The resolved start dates for both tiers.

    Raises:
        PartitionConfigError: When the config is missing or malformed and no
            environment override supplies the missing date.
    """
    gold_override = os.environ.get(_env_key(dataset, "GOLD"))
    silver_override = os.environ.get(_env_key(dataset, "SILVER"))
    if gold_override and silver_override:
        return PartitionStarts(silver=silver_override, gold=gold_override)

    cfg = _load_config(dataset, datasets_dir)
    gold_start = gold_override or _served_start(cfg)
    silver_start = silver_override or _preload_start(cfg, gold_start)
    return PartitionStarts(silver=silver_start, gold=gold_start)


def _env_key(dataset: str, tier: str) -> str:
    slug = dataset.upper().replace("-", "_")
    return f"CORPUS_{slug}_{tier}_START"


def _load_config(dataset: str, datasets_dir: str | None) -> dict[str, Any]:
    directory = datasets_dir or os.environ.get("CORPUS_DATASETS_DIR")
    if not directory:
        raise PartitionConfigError(
            "CORPUS_DATASETS_DIR is unset and no datasets_dir was provided"
        )
    path = Path(directory) / f"{dataset}.yaml"
    if not path.is_file():
        raise PartitionConfigError(f"dataset config not found: {path}")
    loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(loaded, dict):
        raise PartitionConfigError(f"dataset config is not a mapping: {path}")
    return loaded


def _served_start(cfg: dict[str, Any]) -> str:
    gold = cfg.get("gold")
    if not isinstance(gold, dict):
        raise PartitionConfigError("dataset config has no `gold` section")
    served = gold.get("served_start")
    if served is None:
        raise PartitionConfigError("dataset config has no `gold.served_start`")
    return _as_date_string(served)


def _preload_start(cfg: dict[str, Any], gold_start: str) -> str:
    horizon = _max_horizon(cfg)
    gold_date = dt.datetime.strptime(gold_start, _DATE_FORMAT).date()
    silver_date = gold_date - dt.timedelta(days=horizon)
    return silver_date.strftime(_DATE_FORMAT)


def _max_horizon(cfg: dict[str, Any]) -> int:
    gold = cfg.get("gold", {})
    rolling = gold.get("rolling", {}) if isinstance(gold, dict) else {}
    if not isinstance(rolling, dict):
        raise PartitionConfigError("`gold.rolling` is not a mapping")
    horizons: list[int] = []
    horizons.extend(rolling.get("horizons_basic", []))
    horizons.extend(rolling.get("horizons_vwap", []))
    horizon_52w = rolling.get("horizon_52w")
    if horizon_52w is not None:
        horizons.append(horizon_52w)
    if not horizons:
        raise PartitionConfigError(
            "`gold.rolling` has no horizons to derive the Silver preload window"
        )
    return max(horizons)


def _as_date_string(value: object) -> str:
    if isinstance(value, dt.date):
        return value.strftime(_DATE_FORMAT)
    if isinstance(value, str):
        dt.datetime.strptime(value, _DATE_FORMAT)  # validate
        return value
    raise PartitionConfigError(
        f"expected a date, got {type(value).__name__}: {value!r}"
    )
