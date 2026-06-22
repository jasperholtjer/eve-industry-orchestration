"""Resolve partition start dates from the corpus dataset config.

Corpus owns the dataset config (``datasets/<name>.yaml``); orchestration reads
the per-derivative ``served_start`` to anchor the Gold partition matrix instead
of hardcoding it. Silver must reach back one full look-back window before the
earliest Gold date so the first Gold partition has its window present, hence
Silver and Gold get distinct start dates.

Since ADR-0025 ``gold`` is a list of named derivatives (the only shape the corpus
binary accepts: ``DatasetConfig.gold: Option<Vec<GoldDerivative>>``), each with
its own ``served_start`` (absent for ``recency-weighted``) and its own shape block
(``rolling`` / ``flat`` / ``ewma``). A single-derivative dataset (market-history)
is just a one-element list.

Silver is shared by every derivative of a dataset, so its start is the *earliest*
preload across the dataset's windowed derivatives. Gold start is resolved per
``(dataset, derivative)``.
"""

from __future__ import annotations

import datetime as dt
import math
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

_DATE_FORMAT = "%Y-%m-%d"

# EWMA warmup, mirroring ``ingestor_system_jumps::gold::ewma_warmup_days``:
# ``ceil(half_life_hours * 10 / 24)``, floored at 1 day. The recursion only
# needs a few half-lives of priming, so the Silver reach-back is short.
_EWMA_WARMUP_HALF_LIVES = 10


class PartitionConfigError(RuntimeError):
    """Raised when the dataset config cannot yield partition start dates."""


@dataclass(frozen=True)
class PartitionStarts:
    """Inclusive first partition date for each tier, as ``YYYY-MM-DD``.

    ``gold`` is ``None`` for a derivative with no ``served_start`` (a
    ``recency-weighted`` "latest" signal has no partition matrix).
    """

    silver: str
    gold: str | None


@dataclass(frozen=True)
class _Derivative:
    """One Gold derivative resolved from the dataset config."""

    name: str
    shape: str
    served_start: str | None
    lookback_days: int | None
    """``None`` for ``recency-weighted`` without a fixed window."""


def resolve_partition_starts(
    dataset: str, derivative: str | None = None, datasets_dir: str | None = None
) -> PartitionStarts:
    """Resolves the Silver and Gold partition start dates for a dataset.

    Gold start is the named derivative's ``served_start``; Silver start is the
    earliest preload across the dataset's windowed derivatives (Silver is
    shared). With no ``derivative`` a single-derivative dataset resolves
    automatically; a multi-derivative dataset requires the selector.

    Either tier can be overridden via ``CORPUS_<DATASET>_<TIER>_START``; a
    per-derivative Gold override ``CORPUS_<DATASET>_<DERIVATIVE>_GOLD_START``
    disambiguates when a dataset has more than one windowed Gold start.

    Args:
        dataset: Dataset name, e.g. ``market-history``.
        derivative: Gold derivative name (ADR-0025). Optional when the dataset
            declares exactly one derivative.
        datasets_dir: Directory holding ``<dataset>.yaml``. Falls back to the
            ``CORPUS_DATASETS_DIR`` environment variable.

    Returns:
        The resolved start dates for both tiers.

    Raises:
        PartitionConfigError: When the config is missing or malformed and no
            environment override supplies the missing date.
    """
    cfg = _load_config(dataset, datasets_dir)
    derivatives = _derivatives(cfg)
    selected = _select_derivative(dataset, derivatives, derivative)

    gold_start = _gold_override(dataset, selected.name) or selected.served_start

    silver_override = os.environ.get(_env_key(dataset, "SILVER"))
    silver_start = silver_override or _silver_start(dataset, derivatives)

    return PartitionStarts(silver=silver_start, gold=gold_start)


def _select_derivative(
    dataset: str, derivatives: list[_Derivative], name: str | None
) -> _Derivative:
    if name is not None:
        for d in derivatives:
            if d.name == name:
                return d
        available = [d.name for d in derivatives]
        raise PartitionConfigError(
            f"dataset {dataset} has no gold derivative {name!r}; available: {available}"
        )
    if len(derivatives) == 1:
        return derivatives[0]
    names = [d.name for d in derivatives]
    raise PartitionConfigError(
        f"dataset {dataset} declares {len(derivatives)} gold derivatives; "
        f"pass a derivative (one of {names})"
    )


def _silver_start(dataset: str, derivatives: list[_Derivative]) -> str:
    """Earliest preload across the dataset's windowed derivatives.

    A windowed derivative has both a ``served_start`` and a look-back; its
    preload is ``served_start - lookback``. Silver is shared, so the dataset's
    Silver start is the minimum preload. A derivative with no ``served_start``
    (the scheduled ``recency-weighted`` "latest" model) imposes no reach-back.
    """
    preloads = [
        _subtract_days(d.served_start, d.lookback_days)
        for d in derivatives
        if d.served_start is not None and d.lookback_days is not None
    ]
    if not preloads:
        raise PartitionConfigError(
            f"dataset {dataset} has no windowed gold derivative to anchor Silver"
        )
    return min(preloads)


def _subtract_days(date_str: str, days: int) -> str:
    date = dt.datetime.strptime(date_str, _DATE_FORMAT).date()
    return (date - dt.timedelta(days=days)).strftime(_DATE_FORMAT)


def _gold_override(dataset: str, derivative: str) -> str | None:
    per_derivative = os.environ.get(_env_key(dataset, f"{derivative}_GOLD"))
    if per_derivative:
        return per_derivative
    return os.environ.get(_env_key(dataset, "GOLD"))


def _env_key(dataset: str, tier: str) -> str:
    slug = f"{dataset}_{tier}".upper().replace("-", "_")
    return f"CORPUS_{slug}_START"


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


def _derivatives(cfg: dict[str, Any]) -> list[_Derivative]:
    """Reads the ADR-0025 ``gold`` list into one entry per named derivative."""
    gold = cfg.get("gold")
    if not isinstance(gold, list):
        raise PartitionConfigError("dataset config has no `gold` list (ADR-0025)")
    if not gold:
        raise PartitionConfigError("dataset config has an empty `gold` list")
    return [_derivative_from_list_entry(entry) for entry in gold]


def _derivative_from_list_entry(entry: dict[str, Any]) -> _Derivative:
    if not isinstance(entry, dict):
        raise PartitionConfigError(f"gold derivative is not a mapping: {entry!r}")
    name = entry.get("name")
    if not isinstance(name, str):
        raise PartitionConfigError(f"gold derivative has no `name`: {entry!r}")
    shape = entry.get("shape")
    if not isinstance(shape, str):
        raise PartitionConfigError(f"gold derivative {name!r} has no `shape`")
    served = entry.get("served_start")
    served_start = _as_date_string(served) if served is not None else None
    return _Derivative(
        name=name,
        shape=shape,
        served_start=served_start,
        lookback_days=_lookback_for_shape(name, shape, entry),
    )


def _lookback_for_shape(name: str, shape: str, entry: dict[str, Any]) -> int | None:
    if shape == "rolling":
        return _rolling_lookback(entry.get("rolling"))
    if shape == "flat-multi-horizon":
        return _flat_lookback(name, entry.get("flat"))
    if shape == "recency-weighted":
        return _ewma_lookback(name, entry.get("ewma"))
    raise PartitionConfigError(f"gold derivative {name!r} has unknown shape {shape!r}")


def _rolling_lookback(rolling: Any) -> int:
    if not isinstance(rolling, dict):
        raise PartitionConfigError("`rolling` is not a mapping")
    horizons: list[int] = []
    horizons.extend(rolling.get("horizons_basic", []))
    horizons.extend(rolling.get("horizons_vwap", []))
    horizon_52w = rolling.get("horizon_52w")
    if horizon_52w is not None:
        horizons.append(horizon_52w)
    if not horizons:
        raise PartitionConfigError(
            "`rolling` has no horizons to derive the Silver preload window"
        )
    return max(horizons)


def _flat_lookback(name: str, flat: Any) -> int:
    if not isinstance(flat, dict):
        raise PartitionConfigError(f"gold derivative {name!r} has no `flat` block")
    horizons = flat.get("horizons")
    if not isinstance(horizons, list) or not horizons:
        raise PartitionConfigError(f"gold derivative {name!r} has no `flat.horizons`")
    return max(horizons)


def _ewma_lookback(name: str, ewma: Any) -> int:
    """EWMA warmup days, matching the corpus ``ewma_warmup_days`` helper.

    Only relevant if a ``recency-weighted`` derivative ever gets a partition
    matrix; under the scheduled "latest" model it carries no ``served_start``
    and so never anchors Silver.
    """
    if not isinstance(ewma, dict):
        raise PartitionConfigError(f"gold derivative {name!r} has no `ewma` block")
    half_life = ewma.get("half_life_hours")
    if not isinstance(half_life, int):
        raise PartitionConfigError(
            f"gold derivative {name!r} has no integer `ewma.half_life_hours`"
        )
    warmup_hours = half_life * _EWMA_WARMUP_HALF_LIVES
    return max(math.ceil(warmup_hours / 24), 1)


def _as_date_string(value: object) -> str:
    if isinstance(value, dt.date):
        return value.strftime(_DATE_FORMAT)
    if isinstance(value, str):
        dt.datetime.strptime(value, _DATE_FORMAT)  # validate
        return value
    raise PartitionConfigError(
        f"expected a date, got {type(value).__name__}: {value!r}"
    )
