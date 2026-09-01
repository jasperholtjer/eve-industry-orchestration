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

# ``orderbook-aggregate`` (ADR-0033) pairs each snapshot with the immediately
# preceding one; the only cross-partition need is the prior day's tail snapshot,
# so the planner loads one day of look-back (``shape_window`` → ``lookback_days:
# 1``). The Silver preload is therefore exactly one day before the Gold start.
_ORDERBOOK_LOOKBACK_DAYS = 1

# ``structures-snapshot`` (corpus ADR-0057) reads only the target day's Silver
# (``shape_window`` → ``lookback_days: 0``), so it needs no reach-back.
_STRUCTURES_SNAPSHOT_LOOKBACK_DAYS = 0

# ``sov-contests`` and ``sov-panel`` (corpus ADR-0066) reach back no further than
# their own Gold date. ``sov-contests`` treats every day as independent; ``sov-panel``
# assembles sibling *Gold* trees plus a trailing ``sov-events`` window over Gold,
# so its ``panel.flip_window_days`` constrains no Silver start.
_SOVEREIGNTY_SNAPSHOT_LOOKBACK_DAYS = 0


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
class SdeGoldDerivative:
    """One SDE Gold derivative (ADR-0030/0031).

    SDE is build-versioned, not a daily time-series, so it has no
    ``served_start`` / look-back window — :class:`PartitionStarts` does not apply.
    Each derivative fans out over the dataset's entities into its own canonical
    Gold tree, so orchestration needs only the derivative ``name`` and ``shape``.
    """

    name: str
    shape: str


def sde_entities(dataset: str, datasets_dir: str | None = None) -> list[str]:
    """Returns the configured SDE Silver entity names, in config order.

    The build-versioned ingest fans out over ``silver.entities`` (ADR-0031);
    orchestration mirrors that fan-out as one asset per entity, so the names come
    from the dataset YAML, never hardcoded.
    """
    cfg = _load_config(dataset, datasets_dir)
    silver = cfg.get("silver")
    if not isinstance(silver, dict):
        raise PartitionConfigError(f"dataset {dataset} has no `silver` block")
    entities = silver.get("entities")
    if not isinstance(entities, list) or not entities:
        raise PartitionConfigError(f"dataset {dataset} has no `silver.entities`")
    names = [e.get("name") for e in entities if isinstance(e, dict)]
    if not all(isinstance(n, str) for n in names) or len(names) != len(entities):
        raise PartitionConfigError(
            f"dataset {dataset} has a `silver.entities` entry without a `name`"
        )
    return names  # type: ignore[return-value]


def sde_gold_derivatives(
    dataset: str, datasets_dir: str | None = None
) -> list[SdeGoldDerivative]:
    """Returns the SDE Gold derivatives (name + shape) from the dataset YAML.

    Reads the ``gold`` list directly: the SDE shapes (``entity-changelog`` /
    ``entity-snapshot``) carry no look-back window, so this bypasses the
    time-series :func:`_derivatives` resolver (which would reject them).
    """
    cfg = _load_config(dataset, datasets_dir)
    gold = cfg.get("gold")
    if not isinstance(gold, list) or not gold:
        raise PartitionConfigError(f"dataset {dataset} has no `gold` list (ADR-0025)")
    out: list[SdeGoldDerivative] = []
    for entry in gold:
        if not isinstance(entry, dict):
            raise PartitionConfigError(f"gold derivative is not a mapping: {entry!r}")
        name = entry.get("name")
        shape = entry.get("shape")
        if not isinstance(name, str) or not isinstance(shape, str):
            raise PartitionConfigError(
                f"gold derivative needs `name` and `shape`: {entry!r}"
            )
        out.append(SdeGoldDerivative(name=name, shape=shape))
    return out


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
    shared), clamped up to ``silver.served_start`` when the dataset declares a
    Silver coverage floor (ADR-0027), so the matrix never reaches before the
    served upstream coverage starts. With no ``derivative`` a single-derivative
    dataset
    resolves automatically; a multi-derivative dataset requires the selector.

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
    floor = _silver_served_start(cfg)
    derived = _silver_start(dataset, derivatives)
    silver_start = silver_override or (max(derived, floor) if floor else derived)

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


def _silver_served_start(cfg: dict[str, Any]) -> str | None:
    """Reads the optional ``silver.served_start`` upstream coverage floor.

    The earliest date the dataset's Silver contract serves (ADR-0027) — the
    start of the contiguous upstream era feeding the Gold contract — owned by the
    corpus dataset YAML. ``None`` when absent (no lower bound). Distinct from a
    Gold derivative's ``served_start`` (the earliest legal Gold target).
    """
    silver = cfg.get("silver")
    if not isinstance(silver, dict):
        return None
    value = silver.get("served_start")
    if value is None:
        return None
    return _as_date_string(value)


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
    if shape in ("flat-multi-horizon", "cost-index-history", "sov-adm"):
        # cost-index-history (ADR-0043) and sov-adm (ADR-0066) carry the same
        # `flat` block — a max horizon over a daily-rollup series — so their
        # Silver preload is the max of `flat.horizons`, exactly like
        # flat-multi-horizon.
        return _flat_lookback(name, entry.get("flat"), key="flat")
    if shape == "recency-weighted":
        return _ewma_lookback(name, entry.get("ewma"), key="ewma")
    if shape in ("orderbook-aggregate", "orderbook-delta", "orderbook-events"):
        return _ORDERBOOK_LOOKBACK_DAYS
    if shape == "kills-consumption":
        # The killmails demand history (corpus ADR-0061) carries the same `flat`
        # block as flat-multi-horizon — a max horizon over a daily-rollup series —
        # so its Silver preload is max(flat.horizons) too. It is a distinct shape
        # only because its builder pins a composite key and two cross-dataset
        # joins, none of which affect the window.
        return _flat_lookback(name, entry.get("flat"), key="flat")
    if shape == "kills-flat":
        return _flat_lookback(name, entry.get("kills-flat"), key="kills-flat")
    if shape == "kills-recent":
        return _ewma_lookback(name, entry.get("kills-recent"), key="kills-recent")
    if shape == "structures-snapshot":
        # The map's structure dimension (corpus ADR-0057) is a pure function of
        # its own day's Silver — no window, so its Silver preload is the Gold
        # start itself. Zero, not None: it still anchors Silver (unlike a
        # `recency-weighted` derivative, which has no partition matrix at all).
        return _STRUCTURES_SNAPSHOT_LOOKBACK_DAYS
    if shape == "structure-population-history":
        # The predict covariate (corpus ADR-0057) diffs presence against each
        # horizon's reference day, so the preload is max(population.horizons) —
        # the same max-horizon rule as the flat shapes, different block name.
        return _flat_lookback(name, entry.get("population"), key="population")
    if shape in ("sov-ownership", "sov-events"):
        # The sovereignty tenure pair (corpus ADR-0066) censors tenure columns at
        # the left edge of a fixed trailing window, so the Silver preload is
        # `tenure.tenure_lookback_days`.
        return _tenure_lookback(name, entry.get("tenure"))
    if shape in ("sov-contests", "sov-panel"):
        # No Silver window at all — same "no reach-back" rule as
        # structures-snapshot. Zero, not None: both still anchor Silver.
        return _SOVEREIGNTY_SNAPSHOT_LOOKBACK_DAYS
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


def _flat_lookback(name: str, flat: Any, key: str = "flat") -> int:
    """Max horizon of a flat-style block (``flat`` or ``kills-flat``)."""
    if not isinstance(flat, dict):
        raise PartitionConfigError(f"gold derivative {name!r} has no `{key}` block")
    horizons = flat.get("horizons")
    if not isinstance(horizons, list) or not horizons:
        raise PartitionConfigError(f"gold derivative {name!r} has no `{key}.horizons`")
    return max(horizons)


def _tenure_lookback(name: str, tenure: Any) -> int:
    """Look-back days of a ``tenure`` block (corpus ADR-0066).

    Fails loudly rather than defaulting to zero: a mistyped key would otherwise
    silently pull the Silver start forward by half a year.
    """
    if not isinstance(tenure, dict):
        raise PartitionConfigError(f"gold derivative {name!r} has no `tenure` block")
    days = tenure.get("tenure_lookback_days")
    if not isinstance(days, int) or isinstance(days, bool) or days <= 0:
        raise PartitionConfigError(
            f"gold derivative {name!r} has no positive integer "
            "`tenure.tenure_lookback_days`"
        )
    return days


def _ewma_lookback(name: str, ewma: Any, key: str = "ewma") -> int:
    """EWMA warmup days, matching the corpus ``ewma_warmup_days`` helper.

    Only relevant if an EWMA-style derivative (``recency-weighted`` /
    ``kills-recent``) ever gets a partition matrix; under the scheduled "latest"
    model it carries no ``served_start`` and so never anchors Silver.
    """
    if not isinstance(ewma, dict):
        raise PartitionConfigError(f"gold derivative {name!r} has no `{key}` block")
    half_life = ewma.get("half_life_hours")
    if not isinstance(half_life, int):
        raise PartitionConfigError(
            f"gold derivative {name!r} has no integer `{key}.half_life_hours`"
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
