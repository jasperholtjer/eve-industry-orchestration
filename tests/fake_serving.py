"""Fake ``eve-serving`` loader, invoked as the fake ``ssh`` binary for tests.

The :class:`ServingResource` execs ``[ssh, user@host, eve-serving, load, ...]``,
so this script stands in for the SSH client: ``argv`` is ``[user@host,
eve-serving, load, --dataset X, ...]``. It drops the destination and the remote
command name, then mimics the slice of the loader the assets exercise — the
``loaded``/``skipped`` idempotency keyed on a per-dataset "sha", and the SDE
full-state rewrite that clears the market datasets' load-state (the TRUNCATE).

Load-state is a small JSON file at ``FAKE_SERVING_STATE``; each dataset's content
"sha" is injected via ``FAKE_SERVING_SHA_<DATASET>`` (default ``"1"``), so a test
re-runs an unchanged load (``skipped``) or bumps the sha to force a reload.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

# Fact datasets the SDE full-state rewrite TRUNCATEs and clears load-state for.
_MARKET_DATASETS = (
    "market-history",
    "market-orders-live",
    "market-prices-live",
    "industry-cost-indices-live",
)
# Deterministic row counts so a `loaded` summary carries a non-zero count.
_ROWS = {
    "sde": 4096,
    "market-history": 12345,
    "market-orders-live": 6789,
    "market-prices-live": 16000,
    "industry-cost-indices-live": 8003,
}


def _pop_opt(args: list[str], name: str) -> str | None:
    if name not in args:
        return None
    idx = args.index(name)
    value = args[idx + 1] if idx + 1 < len(args) else None
    del args[idx : idx + 2]
    return value


def _state_path() -> Path:
    raw = os.environ.get("FAKE_SERVING_STATE")
    if not raw:
        print("fake eve-serving: FAKE_SERVING_STATE unset", file=sys.stderr)
        raise SystemExit(2)
    return Path(raw)


def _load_state(path: Path) -> dict:
    if not path.is_file():
        return {"loaded": {}}
    state = json.loads(path.read_text(encoding="utf-8"))
    state.setdefault("loaded", {})
    return state


def _save_state(path: Path, state: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state), encoding="utf-8")


def _sha(dataset: str) -> str:
    key = f"FAKE_SERVING_SHA_{dataset.replace('-', '_').upper()}"
    return os.environ.get(key, "1")


def _do_load(dataset: str) -> int:
    path = _state_path()
    state = _load_state(path)
    loaded = state["loaded"]
    sha = _sha(dataset)

    if loaded.get(dataset) == sha:
        print(f"{dataset} skipped: 0 rows")
        return 0

    loaded[dataset] = sha
    # An SDE load is a full-state rewrite: it TRUNCATEs market.* and clears their
    # load-state, so the next market load re-runs even on an unchanged sha.
    if dataset == "sde":
        for market in _MARKET_DATASETS:
            loaded.pop(market, None)
    _save_state(path, state)
    print(f"{dataset} loaded: {_ROWS.get(dataset, 1)} rows")
    return 0


def main(argv: list[str]) -> int:
    # argv mirrors what `ssh` receives: [user@host, eve-serving, load, ...].
    args = list(argv)
    if args and "@" in args[0]:
        args.pop(0)  # drop the SSH destination
    if args and args[0] == "eve-serving":
        args.pop(0)  # drop the remote command name
    if not args or args[0] != "load":
        print(f"fake eve-serving: expected `load`, got {args!r}", file=sys.stderr)
        return 2
    args.pop(0)
    dataset = _pop_opt(args, "--dataset")
    _pop_opt(args, "--date")
    if "--latest" in args:
        args.remove("--latest")
    if not dataset:
        print("fake eve-serving: --dataset required", file=sys.stderr)
        return 2
    return _do_load(dataset)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
