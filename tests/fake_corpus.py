"""Fake ``corpus`` binary for orchestration tests.

Mimics the slice of the real CLI surface the Silver and Gold paths exercise,
without the Rust build: ``ingest`` and ``gold build`` write the ``parquet +
_INDEX.json + _DONE`` contract, ``verify`` checks it, and ``everef
missing-partitions`` / ``gold ready-dates`` / ``state query`` answer with the
same JSON shapes the real binary emits. ``gold build`` bails when the target-day
Silver partition is absent, and ``gold ready-dates`` reports the state-level
"Silver present, Gold not yet built" diff (the real binary additionally gates on
the rolling-window coverage). Run-state is a small JSON file under
``<sink>/state`` so the diffs stay consistent with what ``ingest`` / ``gold
build`` wrote — keyed on state, never on the tree.

Upstream EVE Ref availability is injected via ``FAKE_EVEREF_DATES`` (a
comma-separated list of ``YYYY-MM-DD``); exit-code paths mirror the real binary
(``verify`` on an absent partition exits 1).
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path


def _pop_opt(args: list[str], name: str) -> str | None:
    """Removes ``--name value`` from ``args`` in place and returns the value."""
    if name not in args:
        return None
    idx = args.index(name)
    value = args[idx + 1] if idx + 1 < len(args) else None
    del args[idx : idx + 2]
    return value


def _pop_flag(args: list[str], name: str) -> bool:
    if name not in args:
        return False
    args.remove(name)
    return True


def _state_file(sink: str) -> Path:
    return Path(sink) / "state" / "fake-state.json"


def _load_state(sink: str) -> dict[str, list[str]]:
    path = _state_file(sink)
    if not path.is_file():
        return {"silver": [], "gold": []}
    state = json.loads(path.read_text(encoding="utf-8"))
    state.setdefault("silver", [])
    state.setdefault("gold", [])
    return state


def _save_state(sink: str, state: dict[str, list[str]]) -> None:
    path = _state_file(sink)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state), encoding="utf-8")


def _partition_dir(sink: str, tier: str, dataset: str, date: str) -> Path:
    year, month, day = date.split("-")
    return (
        Path(sink)
        / tier
        / dataset
        / f"year={int(year)}"
        / f"month={int(month):02d}"
        / f"day={int(day):02d}"
    )


def _do_ingest(args: list[str], sink: str) -> int:
    dataset = _pop_opt(args, "--dataset")
    date = _pop_opt(args, "--date")
    if dataset is None or date is None:
        print("ingest: --dataset and --date required", file=sys.stderr)
        return 2

    pdir = _partition_dir(sink, "silver", dataset, date)
    pdir.mkdir(parents=True, exist_ok=True)
    (pdir / "data.parquet").write_bytes(b"PAR1-fake")
    year, month, day = (int(p) for p in date.split("-"))
    index = {
        "schema_version": 1,
        "dataset": dataset,
        "partition": {"year": year, "month": month, "day": day},
        "row_count": 1,
        "tier": "silver",
        "retention_class": "validated",
    }
    (pdir / "_INDEX.json").write_text(json.dumps(index), encoding="utf-8")
    (pdir / "_DONE").write_text("", encoding="utf-8")

    state = _load_state(sink)
    if date not in state["silver"]:
        state["silver"].append(date)
        _save_state(sink, state)

    print(f"wrote 1 rows -> {pdir}", file=sys.stderr)
    return 0


def _do_gold(args: list[str], sink: str) -> int:
    subcommand = args[1] if len(args) > 1 else ""
    if subcommand == "build":
        return _do_gold_build(args, sink)
    if subcommand == "ready-dates":
        return _do_gold_ready_dates(args, sink)
    print(f"gold: unsupported subcommand {subcommand!r}", file=sys.stderr)
    return 2


def _do_gold_ready_dates(args: list[str], sink: str) -> int:
    dataset = _pop_opt(args, "--dataset")
    _pop_opt(args, "--format")
    state = _load_state(sink)
    # Real binary gates on the 365-day window; the fake models only the
    # state-level "Silver present, Gold not yet built" diff, which is enough to
    # exercise the sensor (window coverage is unit-tested on the Rust side).
    built = set(state["gold"])
    ready = sorted(d for d in state["silver"] if d not in built)
    print(json.dumps({"dataset": dataset, "ready": ready}))
    return 0


def _do_gold_build(args: list[str], sink: str) -> int:
    dataset = _pop_opt(args, "--dataset")
    date = _pop_opt(args, "--date")
    if dataset is None or date is None:
        print("gold build: --dataset and --date required", file=sys.stderr)
        return 2

    # The real binary bails when the target-day Silver partition is absent; it
    # cannot derive Gold without the target row(s).
    state = _load_state(sink)
    if date not in state["silver"]:
        print(
            f"gold build: target silver partition for {date} is absent", file=sys.stderr
        )
        return 1

    pdir = _partition_dir(sink, "gold", dataset, date)
    pdir.mkdir(parents=True, exist_ok=True)
    (pdir / "data.parquet").write_bytes(b"PAR1-fake-gold")
    year, month, day = (int(p) for p in date.split("-"))
    index = {
        "schema_version": 1,
        "dataset": dataset,
        "partition": {"year": year, "month": month, "day": day},
        "row_count": 1,
        "tier": "gold",
        "retention_class": "validated",
    }
    (pdir / "_INDEX.json").write_text(json.dumps(index), encoding="utf-8")
    (pdir / "_DONE").write_text("", encoding="utf-8")

    if date not in state["gold"]:
        state["gold"].append(date)
        _save_state(sink, state)

    print(f"wrote 1 gold rows -> {pdir}", file=sys.stderr)
    return 0


def _do_verify(args: list[str], sink: str) -> int:
    dataset = _pop_opt(args, "--dataset")
    date = _pop_opt(args, "--date")
    tier = _pop_opt(args, "--tier") or "silver"
    _pop_flag(args, "--full")
    if dataset is None or date is None:
        print("verify: --dataset and --date required", file=sys.stderr)
        return 2

    pdir = _partition_dir(sink, tier, dataset, date)
    if (pdir / "_DONE").is_file():
        print(f"[{date}] ok", file=sys.stderr)
        return 0
    print(f"[{date}] absent", file=sys.stderr)
    return 1


def _do_everef(args: list[str], sink: str) -> int:
    subcommand = args[1] if len(args) > 1 else ""
    if subcommand != "missing-partitions":
        print(f"everef: unsupported subcommand {subcommand!r}", file=sys.stderr)
        return 2

    dataset = _pop_opt(args, "--dataset")
    window_days = _pop_opt(args, "--window-days")
    _pop_opt(args, "--format")
    _pop_flag(args, "--full")

    upstream_raw = os.environ.get("FAKE_EVEREF_DATES", "")
    upstream = sorted(d.strip() for d in upstream_raw.split(",") if d.strip())
    local = set(_load_state(sink)["silver"])
    missing = [d for d in upstream if d not in local]

    payload = {
        "dataset": dataset,
        "window_days": int(window_days) if window_days else 30,
        "scanned_at": "2026-06-20T00:00:00+00:00",
        "upstream_count": len(upstream),
        "local_count": len(local),
        "missing": missing,
    }
    print(json.dumps(payload))
    return 0


def _do_state(args: list[str], sink: str) -> int:
    subcommand = args[1] if len(args) > 1 else ""
    if subcommand != "query":
        print(f"state: unsupported subcommand {subcommand!r}", file=sys.stderr)
        return 2

    _pop_opt(args, "--sql")
    _pop_opt(args, "--format")
    dataset = "market-history"
    rows = [
        {"dataset": dataset, "tier": "silver", "partition_key": f"date={date}"}
        for date in sorted(_load_state(sink)["silver"])
    ]
    print(json.dumps(rows))
    return 0


def main(argv: list[str]) -> int:
    args = list(argv)
    if "--version" in args:
        print("corpus 0.0.0-fake")
        return 0

    sink = _pop_opt(args, "--sink-path") or os.environ.get("CORPUS_SINK_PATH")
    _pop_opt(args, "--state-db")
    _pop_opt(args, "--datasets-dir")
    if sink is None:
        print("fake corpus: no --sink-path or CORPUS_SINK_PATH", file=sys.stderr)
        return 2
    if not args:
        print("fake corpus: no subcommand", file=sys.stderr)
        return 2

    command = args[0]
    if command == "ingest":
        return _do_ingest(args, sink)
    if command == "gold":
        return _do_gold(args, sink)
    if command == "verify":
        return _do_verify(args, sink)
    if command == "everef":
        return _do_everef(args, sink)
    if command == "state":
        return _do_state(args, sink)
    print(f"fake corpus: unknown command {command!r}", file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
