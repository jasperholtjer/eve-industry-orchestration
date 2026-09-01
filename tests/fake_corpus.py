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
(``verify`` on an absent partition exits 1). Two further lists inject the
binary-side failures that have no state-level cause the fake can otherwise
reach, both keyed on composite ``dataset:tier-or-derivative:date`` entries
(comma-separated) rather than on date alone, so a failure injected for one
dataset/tier does not bleed into another sharing the same date:
``FAKE_GOLD_GATE_FAIL_DATES`` (entries ``dataset:derivative:date``) makes
``gold build`` exit non-zero as the real ``coverage_min_ratio`` gate does on an
incomplete rolling window, checked *after* the upstream-gap ``skipped`` branch
so a genuine ADR-0029 gap still reports ``skipped`` cleanly even when the same
date appears in this list for another dataset/derivative. ``FAKE_VERIFY_FAIL_DATES``
(entries ``dataset:tier:date``) makes ``verify`` exit non-zero on a partition
that is present but fails the contract cross-check, checked *after* the
partition-presence check so it only fires on a partition that actually exists.
A third, ``FAKE_SHORT_FLIP_WINDOW_DATES`` (entries ``dataset:derivative:date``),
makes a ``sovereignty-panel`` build report a *written* partition whose two flip
counts are NULL — the trailing-window gate, which nulls a derived column rather
than skipping the day.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sys
from pathlib import Path

import yaml


def _pop_opt(args: list[str], name: str) -> str | None:
    """Removes ``--name value`` from ``args`` in place and returns the value."""
    if name not in args:
        return None
    idx = args.index(name)
    value = args[idx + 1] if idx + 1 < len(args) else None
    del args[idx : idx + 2]
    return value


def _env_keys(name: str) -> set[str]:
    """Reads a comma-separated composite injection list (e.g. ``dataset:tier:date``)."""
    return {k.strip() for k in os.environ.get(name, "").split(",") if k.strip()}


def _pop_flag(args: list[str], name: str) -> bool:
    if name not in args:
        return False
    args.remove(name)
    return True


def _peek_opt(args: list[str], name: str) -> str | None:
    """Returns ``--name value`` without removing it (for dispatch on --dataset)."""
    if name not in args:
        return None
    idx = args.index(name)
    return args[idx + 1] if idx + 1 < len(args) else None


# Known Gold derivatives per dataset (ADR-0025). A single-derivative dataset
# resolves its lone derivative when `--derivative` is omitted; a multi-derivative
# dataset is ambiguous without the selector, mirroring the real binary. The
# derivative name is the Gold-tree path component and the Gold state key.
_DERIVATIVES: dict[str, list[str]] = {
    "market-history": ["market-history"],
    "system-jumps": ["system-traffic-history", "system-traffic-recent"],
    "market-orders": [
        "market-orders-snapshot",
        "market-orders-changes",
        "market-orders-events",
    ],
    "system-kills": [
        "system-kills-ship-history",
        "system-kills-ship-recent",
        "system-kills-npc-history",
        "system-kills-npc-recent",
        "system-kills-pod-history",
        "system-kills-pod-recent",
    ],
    "sde": ["sde-changelog", "sde-snapshot"],
    "mer": [
        "mer-money-supply",
        "mer-economy-indices",
        "mer-sinks-faucets",
        "mer-commodity-sinks-faucets",
        "mer-production-destruction",
    ],
    "industry-cost-indices": ["industry-cost-indices-history"],
    "killmails": ["killmails-consumption"],
    "structures": ["structures-snapshot", "structure-population-history"],
    "news": [
        "news-articles",
        "news-sections",
        "news-entity-mentions",
        "news-events",
    ],
    "transcripts": [
        "transcripts-videos",
        "transcripts-sections",
        "transcripts-entity-mentions",
    ],
    # Sovereignty (corpus ADR-0066): three `hourly-folder-tar` datasets, packaged
    # exactly like system-jumps / system-kills, carrying five Gold trees off one
    # Silver fold each. Only `sovereignty-map` is multi-derivative, so the other
    # two resolve their lone derivative when `--derivative` is omitted.
    "sovereignty-map": [
        "sovereignty-ownership",
        "sovereignty-changes",
        "sovereignty-panel",
    ],
    "sovereignty-structures": ["sovereignty-adm"],
    "sovereignty-campaigns": ["sovereignty-contests"],
}
# Per-derivative served_start, surfaced in `gold ready-dates` JSON.
_SERVED_START: dict[str, str | None] = {
    "system-traffic-history": "2022-01-01",
    "system-traffic-recent": None,
    "market-orders-snapshot": "2021-07-09",
    "market-orders-changes": "2021-07-09",
    "market-orders-events": "2021-07-09",
    "system-kills-ship-history": "2022-01-01",
    "system-kills-npc-history": "2022-01-01",
    "system-kills-pod-history": "2022-01-01",
    "system-kills-ship-recent": None,
    "system-kills-npc-recent": None,
    "system-kills-pod-recent": None,
    "industry-cost-indices-history": "2022-01-01",
    "killmails-consumption": "2022-01-01",
    # The two structures derivatives have different Gold starts: the dimension is
    # a pure per-day function (served from the first v2 archive), the covariate
    # needs its 30-day reference day inside the served window.
    "structures-snapshot": "2024-03-31",
    "structure-population-history": "2024-04-30",
    # All five sovereignty derivatives serve from one 180d tenure window past the
    # 2021-07-01 Silver floor; the panel sits one further 30d flip window in, so
    # its flip counts are never permanently NULL (corpus ADR-0066 decision 7/8).
    "sovereignty-ownership": "2022-01-01",
    "sovereignty-changes": "2022-01-01",
    "sovereignty-panel": "2022-01-31",
    "sovereignty-adm": "2022-01-01",
    "sovereignty-contests": "2022-01-01",
}

# Same-day Gold prerequisites the real binary's `gold ready-dates` gates on
# (corpus ADR-0066 decision 8): a Gold-over-Gold derivative reports a date ready
# only once every prerequisite tree holds that same day's Gold partition. Only
# `sovereignty-panel` has any, and `sovereignty-changes` is deliberately absent
# from its set — the binary does not gate on the flip-window tree, which is the
# gap parked in docs/questions/2026-09-01-sov-panel-flip-window-gate.md.
_SAME_DAY_GOLD_PREREQUISITES: dict[str, tuple[str, ...]] = {
    "sovereignty-panel": (
        "sovereignty-ownership",
        "sovereignty-adm",
        "sovereignty-contests",
    ),
}


def _resolve_derivative(dataset: str, derivative: str | None) -> str | None:
    """Returns the resolved derivative, or ``None`` when ambiguous."""
    derivatives = _DERIVATIVES.get(dataset, [dataset])
    if derivative is not None:
        return derivative
    if len(derivatives) == 1:
        return derivatives[0]
    return None


def _state_file(sink: str) -> Path:
    return Path(sink) / "state" / "fake-state.json"


def _load_state(sink: str) -> dict:
    path = _state_file(sink)
    if not path.is_file():
        return {
            "silver": [],
            "gold": {},
            "skipped": [],
            "sde_silver": {},
            "sde_gold": {},
            "mer_silver": [],
            "seen_documents": {},
            "seq": 0,
            "silver_at": {},
            "gold_at": {},
            "sde_silver_at": {},
            "sde_gold_at": {},
            "partition_facts": {},
        }
    state = json.loads(path.read_text(encoding="utf-8"))
    state.setdefault("silver", [])
    # Days recorded as a genuine upstream gap by a skipped ingest (ADR-0028);
    # `gold build` skips a target day in this set rather than failing (ADR-0029).
    state.setdefault("skipped", [])
    # SDE build-versioned state (ADR-0031): committed Silver builds keyed
    # build -> release_date, and Gold built builds per derivative.
    state.setdefault("sde_silver", {})
    state.setdefault("sde_gold", {})
    # MER monthly-archive state (corpus ADR-0058): committed `mer` blob-Silver
    # report-months (`YYYY-MM-01`). `mer-killdump` Silver is written to disk but
    # not tracked here (it has no Gold + no sensor keyed on its state).
    state.setdefault("mer_silver", [])
    # Context-dataset seen-ledger (ADR-0045): documents actually archived, per
    # dataset — the `seen_documents` table the real binary keeps in run-state.
    state.setdefault("seen_documents", {})
    # Monotonic write clock standing in for run-state `last_seen_at`. Only the
    # ORDER matters: a Gold partition whose Silver was written after it is stale
    # (corpus ADR-0060 drift repair), which is what `state query` reports.
    state.setdefault("seq", 0)
    state.setdefault("silver_at", {})
    state.setdefault("gold_at", {})
    # Same clock over the SDE build axis, keyed `str(build)`: a changelog Gold
    # written before the nearest lower Silver committed was diffed across a hole
    # and is stale, which is what `stale_changelog_builds` reports.
    state.setdefault("sde_silver_at", {})
    state.setdefault("sde_gold_at", {})
    # Per-partition run-state columns the real `partitions` table carries beyond
    # existence: keyed `<dataset>|<tier>|<partition_key>` with the run-state key
    # exactly as corpus writes it (`date=`/`build=`/`month=`/`latest`), so a
    # `state query` for partition metadata answers from a real prefixed key.
    state.setdefault("partition_facts", {})
    # Gold is keyed per derivative (each its own tree); tolerate the older flat
    # list shape by folding it under a dataset-named key on read.
    gold = state.get("gold", {})
    if isinstance(gold, list):
        gold = {"market-history": gold}
    state["gold"] = gold
    return state


def _record_partition(
    state: dict,
    dataset: str,
    tier: str,
    partition_key: str,
    *,
    rows: int | None = None,
    retention_class: str = "validated",
) -> None:
    """Stamps the `partitions` columns an enrichment read asks for.

    `rows` defaults to the 1 row every fake partition holds; `FAKE_PARTITION_ROWS`
    overrides it so a test can produce a genuinely zero-row partition.
    """
    if rows is None:
        rows = int(os.environ.get("FAKE_PARTITION_ROWS", "1"))
    identity = f"{dataset}|{tier}|{partition_key}"
    state.setdefault("partition_facts", {})[identity] = {
        "rows": rows,
        "retention_class": retention_class,
        "parquet_sha256": hashlib.sha256(identity.encode()).hexdigest(),
    }


def _save_state(sink: str, state: dict) -> None:
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


# --- SDE build-versioned path (ADR-0030/0031) -----------------------------

# Latest-only industry Gold derivatives: each its own flat tree, keyed on the
# literal `latest` in the run-state (ADR-0044 products, ADR-0056 the other two).
_SDE_INDUSTRY_DERIVATIVES = (
    "sde-industry-products",
    "sde-industry-facilities",
    "sde-industry-hubs",
)


def _sde_entities() -> list[str]:
    """Reads the SDE entity names from the fixture ``sde.yaml`` config.

    Mirrors the real binary fanning out over ``silver.entities``; keeps the fake
    aligned with what :func:`config.sde_entities` feeds the asset specs.
    """
    datasets_dir = os.environ.get("CORPUS_DATASETS_DIR")
    if not datasets_dir:
        return []
    cfg = yaml.safe_load((Path(datasets_dir) / "sde.yaml").read_text(encoding="utf-8"))
    return [e["name"] for e in cfg["silver"]["entities"]]


def _sde_builds_env() -> dict[int, str]:
    """Upstream SDE builds, injected via ``FAKE_SDE_BUILDS`` (``build:date,...``)."""
    raw = os.environ.get("FAKE_SDE_BUILDS", "")
    builds: dict[int, str] = {}
    for pair in raw.split(","):
        pair = pair.strip()
        if not pair:
            continue
        build_str, date = pair.split(":")
        builds[int(build_str)] = date
    return builds


def _resolve_build(args: list[str], available: dict[int, str]) -> int | None:
    build = _pop_opt(args, "--build")
    latest = _pop_flag(args, "--latest")
    if build is not None:
        return int(build)
    if latest:
        return max(available) if available else None
    return None


def _write_partition(
    sink: str, tier: str, tree: str, date: str, payload: bytes
) -> None:
    pdir = _partition_dir(sink, tier, tree, date)
    pdir.mkdir(parents=True, exist_ok=True)
    (pdir / "data.parquet").write_bytes(payload)
    year, month, day = (int(p) for p in date.split("-"))
    index = {
        "schema_version": 1,
        "dataset": tree,
        "partition": {"year": year, "month": month, "day": day},
        "row_count": 1,
        "tier": tier,
        "retention_class": "validated",
    }
    (pdir / "_INDEX.json").write_text(json.dumps(index), encoding="utf-8")
    (pdir / "_DONE").write_text("", encoding="utf-8")


def _write_flat(sink: str, tier: str, tree: str) -> None:
    """Writes a flat, non-partitioned ``<tier>/<tree>/`` partition (ADR-0032)."""
    pdir = Path(sink) / tier / tree
    pdir.mkdir(parents=True, exist_ok=True)
    (pdir / "data.parquet").write_bytes(b"PAR1-fake-gold")
    (pdir / "_INDEX.json").write_text(
        json.dumps({"schema_version": 1, "dataset": tree, "tier": tier}),
        encoding="utf-8",
    )
    (pdir / "_DONE").write_text("", encoding="utf-8")


# --- context datasets (Bronze-only archival, ADR-0045/0046/0048) ----------

# The fetch date the fake stamps its Bronze partition with. The real binary uses
# "today"; a fixed, overridable date keeps the tests deterministic.
_FAKE_CONTEXT_DATE = os.environ.get("FAKE_CONTEXT_DATE", "2026-07-10")


def _write_bronze(sink: str, dataset: str, date: str, objects: int) -> None:
    """Writes a keep-forever Bronze partition (`_MANIFEST.json` + `_DONE` last)."""
    year, month, day = date.split("-")
    pdir = (
        Path(sink)
        / "bronze"
        / dataset
        / f"year={int(year)}"
        / f"month={int(month):02d}"
        / f"day={int(day):02d}"
    )
    pdir.mkdir(parents=True, exist_ok=True)
    for i in range(objects):
        (pdir / f"doc-{i:04d}.raw").write_bytes(b"fake-raw-bytes")
    manifest = {
        "schema_version": 1,
        "dataset": dataset,
        "objects": objects,
        "retention_class": "archive",
    }
    (pdir / "_MANIFEST.json").write_text(json.dumps(manifest), encoding="utf-8")
    (pdir / "_DONE").write_text("", encoding="utf-8")


def _context_partition(date: str) -> str:
    year, month, day = date.split("-")
    return f"year={int(year)}/month={int(month):02d}/day={int(day):02d}"


def _do_context(args: list[str], sink: str) -> int:
    subcommand = args[1] if len(args) > 1 else ""
    if subcommand == "fetch":
        return _do_context_fetch(args, sink)
    if subcommand == "backfill":
        return _do_context_backfill(args, sink)
    print(f"context: unsupported subcommand {subcommand!r}", file=sys.stderr)
    return 2


def _do_context_fetch(args: list[str], sink: str) -> int:
    """Mimics `corpus context fetch` (ADR-0048): one dense fetch-date Bronze partition."""
    dataset = _pop_opt(args, "--dataset")
    if dataset is None:
        print("context fetch: --dataset required", file=sys.stderr)
        return 2
    date = _FAKE_CONTEXT_DATE
    _write_bronze(sink, dataset, date, objects=12)
    state = _load_state(sink)
    state["seen_documents"][dataset] = 12
    _save_state(sink, state)
    print(f"archived 12 {dataset} objects -> bronze/{dataset}", file=sys.stderr)
    print(
        json.dumps(
            {
                "status": "ok",
                "dataset": dataset,
                "tier": "bronze",
                "partition": _context_partition(date),
                "objects": 12,
                "new_documents": 5,
            }
        )
    )
    return 0


def _do_context_backfill(args: list[str], sink: str) -> int:
    """Mimics `corpus context backfill`: resumable historical sweep with a cap.

    Honours the paid-work cap flags (`--max-articles` for news, `--max-videos` for
    transcripts): when a cap is passed the fake reports `capped: true` (more work
    remains, so the operator re-runs), else `capped: false`.
    """
    dataset = _pop_opt(args, "--dataset")
    max_articles = _pop_opt(args, "--max-articles")
    max_videos = _pop_opt(args, "--max-videos")
    if dataset is None:
        print("context backfill: --dataset required", file=sys.stderr)
        return 2
    cap = max_articles or max_videos
    capped = cap is not None
    date = _FAKE_CONTEXT_DATE
    _write_bronze(sink, dataset, date, objects=12)
    state = _load_state(sink)
    state["seen_documents"][dataset] = 12
    _save_state(sink, state)
    print(f"backfilled {dataset} (capped={capped})", file=sys.stderr)
    status = {
        "status": "ok",
        "dataset": dataset,
        "tier": "bronze",
        "partition": _context_partition(date),
        "objects": 12,
        "new_documents": 12,
        "capped": capped,
    }
    if max_articles is not None:
        status["article_attempts"] = int(max_articles)
    if max_videos is not None:
        status["transcript_attempts"] = int(max_videos)
    print(json.dumps(status))
    return 0


def _do_enrich(args: list[str], sink: str) -> int:
    """Mimics ``corpus enrich embed`` (ADR-0053): local ONNX run, archived as a fetch.

    Fails when ``CORPUS_EMBEDDING_MODEL_DIR`` is absent, like the real binary (no
    silent fallback to an unlabeled generation). ``--limit`` caps the chunks one
    run embeds; the ledger makes a capped run resumable.
    """
    subcommand = args[1] if len(args) > 1 else ""
    if subcommand != "embed":
        print(f"enrich: unsupported subcommand {subcommand!r}", file=sys.stderr)
        return 2
    dataset = _pop_opt(args, "--dataset")
    date = _pop_opt(args, "--date")
    limit = _pop_opt(args, "--limit")
    _pop_opt(args, "--model-dir")
    if dataset is None:
        print("enrich embed: --dataset required", file=sys.stderr)
        return 2
    if not os.environ.get("CORPUS_EMBEDDING_MODEL_DIR"):
        print("enrich embed: no ONNX model artifact", file=sys.stderr)
        return 1
    chunks = int(limit) if limit is not None else 64
    _write_bronze(sink, dataset, date or _FAKE_CONTEXT_DATE, objects=1)
    print(
        json.dumps(
            {
                "status": "ok",
                "dataset": dataset,
                "tier": "bronze",
                "partition": _context_partition(date or _FAKE_CONTEXT_DATE),
                "chunks_embedded": chunks,
            }
        )
    )
    return 0


def _do_news(args: list[str], sink: str) -> int:
    """Mimics ``corpus news match-stats`` (ADR-0052): the entity-mention report.

    The asset check only reads ``stats.articles`` (the *listed* side of the
    listed-vs-archived delta) and the vocabulary fingerprint, so the fake reports
    the counts and a token of the rest of the real report's shape. ``FAKE_NEWS_
    LISTED`` injects the article count; the archived side comes from the ledger
    (``state query`` over ``seen_documents``).
    """
    subcommand = args[1] if len(args) > 1 else ""
    if subcommand != "match-stats":
        print(f"news: unsupported subcommand {subcommand!r}", file=sys.stderr)
        return 2
    listed = int(os.environ.get("FAKE_NEWS_LISTED", "19"))
    print(
        json.dumps(
            {
                "dependency_fingerprint": "sde-build-3021700",
                "silver_partitions": 1,
                "vocabulary_names": 1000,
                "vocabulary_blocked": 10,
                "stats": {
                    "articles": listed,
                    "matches_per_kind": {"type": 42},
                    "title_rule_matches": 3,
                    "prose_rule_matches": 39,
                    "top_surface_forms": [["Ishtar", 7]],
                    "blocklist_hits": {},
                },
            }
        )
    )
    return 0


def _do_transcripts(args: list[str], sink: str) -> int:
    """Mimics ``corpus transcripts match-stats`` (ADR-0055 §4c): the case-rule report.

    The asset check reads ``report.videos`` (the videos Silver scanned = the *listed*
    side of the scanned-vs-archived delta), ``report.corpus_basis`` and the
    vocabulary fingerprint, so the fake reports those plus a token of the rest of the
    real report's shape. ``FAKE_TRANSCRIPTS_VIDEOS`` injects the scanned-video count;
    the archived side comes from the ledger (``state query`` over ``seen_documents``).
    """
    subcommand = args[1] if len(args) > 1 else ""
    if subcommand != "match-stats":
        print(f"transcripts: unsupported subcommand {subcommand!r}", file=sys.stderr)
        return 2
    videos = int(os.environ.get("FAKE_TRANSCRIPTS_VIDEOS", "12"))
    print(
        json.dumps(
            {
                "dependency_fingerprint": "sde-build-3021700",
                "silver_partitions": 1,
                "vocabulary_names": 1000,
                "vocabulary_blocked": 10,
                "report": {
                    "videos": videos,
                    "sections": videos * 8,
                    "corpus_basis": "backfill measure (scope_hint=305 estimated channel)",
                    "exact": {"total_matches": 40},
                    "insensitive": {"total_matches": 44},
                    "gained_by_insensitivity": [{"form": "Ishtar", "gained": 4}],
                },
            }
        )
    )
    return 0


def _do_live(args: list[str], sink: str) -> int:
    """Mimics ``corpus live build`` (ADR-0039): overwrite a flat ``current/``.

    The live dataset has no Silver and no date matrix — it fetches the newest
    snapshot and overwrites ``gold/<dataset>/current/``. The fake writes the flat
    partition and prints the ``written`` status object the asset surfaces.
    """
    subcommand = args[1] if len(args) > 1 else ""
    if subcommand != "build":
        print(f"live: unsupported subcommand {subcommand!r}", file=sys.stderr)
        return 2
    dataset = _pop_opt(args, "--dataset")
    if dataset is None:
        print("live build: --dataset required", file=sys.stderr)
        return 2
    _write_flat(sink, "gold", f"{dataset}/current")
    print(f"wrote 1 live gold rows -> {dataset}/current", file=sys.stderr)
    # The live datasets emit different status shapes, mirroring the real binary:
    # market-orders-live (everef snapshot) carries `snapshot_file`/`date`;
    # market-prices-live (ESI, ADR-0040) carries `snapshot_at`/`source`;
    # public-contracts-live (everef `.v2.tar.bz2`, ADR-0068) carries both — the
    # filename plus the payload's own `scrape_start` as `snapshot_at`, which the
    # filename's drifting seconds field cannot give.
    status: dict[str, object] = {
        "status": "written",
        "dataset": dataset,
        "derivative": dataset,
        "rows": 1,
        "parquet_sha256": "fake",
        "partition_dir": f"{dataset}/current",
    }
    if dataset == "market-prices-live":
        status["source"] = "esi"
        status["url"] = (
            "https://esi.evetech.net/latest/markets/prices/?datasource=tranquility"
        )
        status["snapshot_at"] = "2026-06-26T12:00:00+00:00"
    elif dataset == "public-contracts-live":
        status["snapshot_file"] = "public-contracts-2026-06-26_12-00-11.v2.tar.bz2"
        status["snapshot_sha256"] = "fake"
        status["date"] = "2026-06-26"
        status["snapshot_at"] = "2026-06-26T12:00:00+00:00"
    else:
        status["snapshot_file"] = f"{dataset}-2026-06-26_12-00-00.v3.csv.bz2"
        status["date"] = "2026-06-26"
    print(json.dumps(status))
    return 0


def _do_sde_ingest(args: list[str], sink: str) -> int:
    available = _sde_builds_env()
    build = _resolve_build(args, available)
    if build is None:
        print("ingest: --build/--latest required for sde", file=sys.stderr)
        return 2
    if build not in available:
        print(f"ingest: build {build} not found upstream", file=sys.stderr)
        return 1
    release_date = available[build]

    # ADR-0032: one atomic unified Silver partition per build under `silver/sde/`.
    _write_partition(sink, "silver", "sde", release_date, b"PAR1-fake")

    state = _load_state(sink)
    state["sde_silver"][str(build)] = release_date
    state["seq"] += 1
    state["sde_silver_at"][str(build)] = state["seq"]
    _record_partition(state, "sde", "silver", f"build={build}")
    _save_state(sink, state)

    print(
        json.dumps(
            {
                "status": "written",
                "dataset": "sde",
                "build_id": build,
                "release_date": release_date,
                "partition_key": f"build={build}",
                "rows": 1,
            }
        )
    )
    return 0


def _do_sde_gold_build(args: list[str], sink: str, derivative: str) -> int:
    state = _load_state(sink)
    committed = {int(b): d for b, d in state["sde_silver"].items()}
    build = _resolve_build(args, committed)
    if build is None:
        print("gold build: --build/--latest required for sde", file=sys.stderr)
        return 2
    if build not in committed:
        print(f"gold build: build {build} has no committed Silver", file=sys.stderr)
        return 1
    release_date = committed[build]

    if derivative == "sde-changelog":
        return _do_sde_changelog(args, sink, state, committed, build, release_date)
    if derivative in _SDE_INDUSTRY_DERIVATIVES:
        return _do_sde_industry(sink, state, derivative, build, release_date)
    return _do_sde_snapshot(sink, state, build, release_date)


def _do_sde_changelog(
    args: list[str],
    sink: str,
    state: dict,
    committed: dict[int, str],
    build: int,
    release_date: str,
) -> int:
    # The baseline build (no committed predecessor < target) writes no changelog
    # partition and reports status "skipped" (ADR-0032).
    has_predecessor = any(b < build for b in committed)
    if not has_predecessor:
        print(
            json.dumps(
                {
                    "status": "skipped",
                    "dataset": "sde-changelog",
                    "derivative": "sde-changelog",
                    "build_id": build,
                    "release_date": release_date,
                    "partition_key": f"build={build}",
                    "reason": f"build {build} is baseline (no predecessor)",
                }
            )
        )
        print(f"gold build: build {build} is baseline; no changelog", file=sys.stderr)
        return 0

    # ADR-0032: one unified changelog tree `gold/sde-changelog/`.
    _write_partition(sink, "gold", "sde-changelog", release_date, b"PAR1-fake-gold")
    state["sde_gold"].setdefault("sde-changelog", [])
    if build not in state["sde_gold"]["sde-changelog"]:
        state["sde_gold"]["sde-changelog"].append(build)
    # Gold overwrites in place, so a repair rematerialise re-stamps the clock and
    # clears the build from the stale set.
    state["seq"] += 1
    state["sde_gold_at"][str(build)] = state["seq"]
    _record_partition(state, "sde-changelog", "gold", f"build={build}")
    _save_state(sink, state)

    print(
        json.dumps(
            {
                "status": "written",
                "dataset": "sde-changelog",
                "derivative": "sde-changelog",
                "build_id": build,
                "release_date": release_date,
                "partition_key": f"build={build}",
            }
        )
    )
    return 0


def _do_sde_snapshot(sink: str, state: dict, build: int, release_date: str) -> int:
    # ADR-0032: latest-only, per-entity, flat non-partitioned `gold/sde-<entity>/`.
    entities = _sde_entities()
    for entity in entities:
        _write_flat(sink, "gold", f"sde-{entity}")

    state["sde_gold"].setdefault("sde-snapshot", [])
    if build not in state["sde_gold"]["sde-snapshot"]:
        state["sde_gold"]["sde-snapshot"].append(build)
    # No run-state row for `sde-snapshot`: the real binary registers one row per
    # entity as `sde-<entity>` and uses `sde-snapshot` only as a status label.
    _save_state(sink, state)

    print(
        json.dumps(
            {
                "status": "written",
                "dataset": "sde-snapshot",
                "derivative": "sde-snapshot",
                "build_id": build,
                "release_date": release_date,
                "partition_key": "latest",
                "entities_written": len(entities),
            }
        )
    )
    return 0


def _do_sde_industry(
    sink: str, state: dict, derivative: str, build: int, release_date: str
) -> int:
    # ADR-0044/0056: latest-only industry derivatives, flat non-partitioned
    # `gold/sde-industry-products|facilities|hubs/`, overwritten each build.
    _write_flat(sink, "gold", derivative)

    rows = int(os.environ.get("FAKE_PARTITION_ROWS", "1"))
    state["sde_gold"].setdefault(derivative, [])
    if build not in state["sde_gold"][derivative]:
        state["sde_gold"][derivative].append(build)
    # The tree is flat and overwritten each build, so its run-state key is the
    # literal `latest` under the derivative's own dataset name — the row the
    # asset's enrichment read looks for.
    _record_partition(state, derivative, "gold", "latest", rows=rows)
    _save_state(sink, state)

    print(
        json.dumps(
            {
                "status": "written",
                "dataset": derivative,
                "derivative": derivative,
                "build_id": build,
                "release_date": release_date,
                "partition_key": "latest",
                "row_count": rows,
            }
        )
    )
    return 0


# --- MER monthly-archive path (corpus ADR-0058) ---------------------------


def _mer_reports_env() -> list[str]:
    """Upstream MER report-months, injected via ``FAKE_MER_REPORTS``.

    A comma-separated list of ``YYYY-MM-01`` report-months (the partition
    identity), e.g. ``2025-06-01,2025-07-01``.
    """
    raw = os.environ.get("FAKE_MER_REPORTS", "")
    return sorted({m.strip() for m in raw.split(",") if m.strip()})


def _resolve_months(args: list[str], available: list[str]) -> list[str] | None:
    """Resolves ``--month`` / ``--range`` / ``--latest`` to report-months.

    ``--month YYYY-MM`` → the matching ``YYYY-MM-01``; ``--range A..B`` → every
    available report-month in ``[A-01, B-01]``; ``--latest`` → the newest.
    """
    month = _pop_opt(args, "--month")
    rng = _pop_opt(args, "--range")
    latest = _pop_flag(args, "--latest")
    if latest:
        return [available[-1]] if available else []
    if month is not None:
        return [f"{month}-01"]
    if rng is not None:
        lo, hi = rng.split("..")
        lo, hi = f"{lo}-01", f"{hi}-01"
        return [m for m in available if lo <= m <= hi]
    return None


def _do_mer_ingest(args: list[str], sink: str) -> int:
    dataset = _pop_opt(args, "--dataset") or "mer"
    available = _mer_reports_env()
    months = _resolve_months(args, available)
    if months is None:
        print("ingest: --month/--range/--latest required for mer", file=sys.stderr)
        return 2

    written = []
    for report_month in months:
        if report_month not in available:
            print(
                f"ingest: report-month {report_month} not found upstream",
                file=sys.stderr,
            )
            return 1
        # corpus ADR-0058: Hive path year=/month=/day=01 under silver/<dataset>/.
        _write_partition(sink, "silver", dataset, report_month, b"PAR1-fake")
        written.append(report_month)

    state = _load_state(sink)
    if dataset == "mer":
        for report_month in written:
            if report_month not in state["mer_silver"]:
                state["mer_silver"].append(report_month)
        state["mer_silver"].sort()
    for report_month in written:
        _record_partition(state, dataset, "silver", f"month={report_month}")
    _save_state(sink, state)

    # The CLI prints one status object per report-month; the streaming resource
    # keeps the last, so emit the last month's status.
    last = written[-1]
    print(
        json.dumps(
            {
                "status": "written",
                "dataset": dataset,
                "report_month": last,
                "partition_key": f"month={last}",
                "rows": 1,
            }
        )
    )
    return 0


def _do_mer_gold_build(args: list[str], sink: str, derivative: str) -> int:
    # MER Gold takes no selector: a full cross-month merge over all committed
    # `mer` Silver (corpus ADR-0058 §5), year-partitioned by history_date.
    state = _load_state(sink)
    if not state["mer_silver"]:
        print("gold build: no committed mer Silver", file=sys.stderr)
        return 1

    concept = {
        "mer-money-supply": "money_supply",
        "mer-economy-indices": "economy_indices_details",
        "mer-sinks-faucets": "sinks_and_faucets_history",
        "mer-commodity-sinks-faucets": "commodity_sinks_and_faucets_history",
        "mer-production-destruction": "produced_destroyed_mined",
    }.get(derivative, "unknown")

    # Year-partition the merge by the report-months' years (a stand-in for
    # history_date years — enough to exercise the contract).
    years = sorted({int(m[:4]) for m in state["mer_silver"]})
    for year in years:
        _write_partition(sink, "gold", derivative, f"{year}-01-01", b"PAR1-fake-gold")
    state["gold"].setdefault(derivative, [])
    _save_state(sink, state)

    print(
        json.dumps(
            {
                "status": "written",
                "dataset": derivative,
                "derivative": derivative,
                "concept": concept,
                "years": len(years),
                "row_count": len(state["mer_silver"]),
            }
        )
    )
    return 0


def _do_everef_list(args: list[str], sink: str) -> int:
    dataset = _peek_opt(args, "--dataset")
    _pop_opt(args, "--dataset")
    _pop_opt(args, "--year")
    _pop_opt(args, "--format")
    if dataset in ("mer", "mer-killdump"):
        payload = [
            {
                "report_month": report_month,
                "url": f"https://data.everef.net/ccp/mer/{report_month[:4]}/EVEOnline_MER_{report_month[:7]}.zip",
                "filename": f"EVEOnline_MER_{report_month[:7]}.zip",
                "size": 1,
                "last_modified": f"{report_month}T00:00:00+00:00",
            }
            for report_month in _mer_reports_env()
        ]
        print(json.dumps(payload))
        return 0
    builds = _sde_builds_env()
    payload = [
        {
            "build": build,
            "release_date": date,
            "url": f"https://data.everef.net/ccp/sde/{build}.zip",
            "size": 1,
        }
        for build, date in sorted(builds.items())
    ]
    print(json.dumps(payload))
    return 0


def _do_ingest(args: list[str], sink: str) -> int:
    if _peek_opt(args, "--dataset") in ("mer", "mer-killdump"):
        return _do_mer_ingest(args, sink)
    if "--build" in args or "--latest" in args:
        return _do_sde_ingest(args, sink)
    dataset = _pop_opt(args, "--dataset")
    date = _pop_opt(args, "--date")
    if dataset is None or date is None:
        print("ingest: --dataset and --date required", file=sys.stderr)
        return 2

    # Mirror ADR-0028: a day absent upstream skips cleanly — no partition, a
    # "skipped" status object on stdout, exit 0. Upstream availability comes from
    # FAKE_EVEREF_DATES; when it is unset the fake assumes every day is present
    # (keeps the always-write behaviour the other tests rely on).
    # Mirror ADR-0041: a day whose upstream publication is still incomplete
    # reports status "incomplete" — no partition, exit 0 — and is retried (no
    # permanent skip recorded). Controlled by FAKE_INCOMPLETE_DATES.
    incomplete_raw = os.environ.get("FAKE_INCOMPLETE_DATES", "")
    incomplete = {d.strip() for d in incomplete_raw.split(",") if d.strip()}
    if date in incomplete:
        print(
            json.dumps(
                {
                    "status": "incomplete",
                    "dataset": dataset,
                    "date": date,
                    "partition_key": f"date={date}",
                    "reason": "source Last-Modified below the stability floor",
                }
            )
        )
        print(f"{date} incomplete, will retry", file=sys.stderr)
        return 0

    upstream_raw = os.environ.get("FAKE_EVEREF_DATES", "")
    upstream = {d.strip() for d in upstream_raw.split(",") if d.strip()}
    if upstream and date not in upstream:
        state = _load_state(sink)
        if date not in state["skipped"]:
            state["skipped"].append(date)
            _save_state(sink, state)
        print(
            json.dumps(
                {
                    "status": "skipped",
                    "dataset": dataset,
                    "date": date,
                    "partition_key": f"date={date}",
                    "reason": f"per-date folder not found ({date})",
                }
            )
        )
        print(f"upstream absent for {date}", file=sys.stderr)
        return 0

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
    # Always re-stamp: a re-ingest of an already-committed day (the ADR-0060
    # drift repair) is exactly the case the staleness diff must see.
    state["seq"] += 1
    state["silver_at"][date] = state["seq"]
    _record_partition(state, dataset, "silver", f"date={date}")
    _save_state(sink, state)

    print(f"wrote 1 rows -> {pdir}", file=sys.stderr)
    print(
        json.dumps(
            {
                "status": "written",
                "dataset": dataset,
                "date": date,
                "partition_key": f"date={date}",
                "rows": 1,
                "parquet_sha256": "fake",
            }
        )
    )
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
    derivative_arg = _pop_opt(args, "--derivative")
    _pop_opt(args, "--format")
    if dataset is None:
        print("gold ready-dates: --dataset required", file=sys.stderr)
        return 2
    derivative = _resolve_derivative(dataset, derivative_arg)
    if derivative is None:
        print(
            f"gold ready-dates: dataset {dataset} declares multiple derivatives; "
            "pass --derivative",
            file=sys.stderr,
        )
        return 2

    state = _load_state(sink)
    # Real binary gates on the look-back window; the fake models only the
    # state-level "Silver present, Gold not yet built" diff, which is enough to
    # exercise the sensor (window coverage is unit-tested on the Rust side).
    built = set(state["gold"].get(derivative, []))
    ready = sorted(d for d in state["silver"] if d not in built)
    # A Gold-over-Gold derivative additionally waits on its same-day sibling Gold
    # partitions; a date whose prerequisites are not all built is not ready.
    for prerequisite in _SAME_DAY_GOLD_PREREQUISITES.get(derivative, ()):
        sibling = set(state["gold"].get(prerequisite, []))
        ready = [d for d in ready if d in sibling]
    payload = {
        "dataset": dataset,
        "derivative": derivative,
        "served_start": _SERVED_START.get(derivative),
        "ready": ready,
    }
    print(json.dumps(payload))
    return 0


def _do_gold_build(args: list[str], sink: str) -> int:
    if _peek_opt(args, "--dataset") == "mer":
        _pop_opt(args, "--dataset")
        derivative_arg = _pop_opt(args, "--derivative")
        derivative = _resolve_derivative("mer", derivative_arg)
        if derivative is None:
            print(
                "gold build: mer declares multiple derivatives; pass --derivative",
                file=sys.stderr,
            )
            return 2
        return _do_mer_gold_build(args, sink, derivative)

    if "--build" in args or "--latest" in args:
        dataset = _pop_opt(args, "--dataset")
        derivative_arg = _pop_opt(args, "--derivative")
        derivative = _resolve_derivative(dataset or "sde", derivative_arg)
        if derivative is None:
            print(
                f"gold build: dataset {dataset} declares multiple derivatives; "
                "pass --derivative",
                file=sys.stderr,
            )
            return 2
        return _do_sde_gold_build(args, sink, derivative)

    dataset = _pop_opt(args, "--dataset")
    derivative_arg = _pop_opt(args, "--derivative")
    date = _pop_opt(args, "--date")
    if dataset is None or date is None:
        print("gold build: --dataset and --date required", file=sys.stderr)
        return 2
    derivative = _resolve_derivative(dataset, derivative_arg)
    if derivative is None:
        print(
            f"gold build: dataset {dataset} declares multiple derivatives; "
            "pass --derivative",
            file=sys.stderr,
        )
        return 2

    # The real binary bails when the target-day Silver partition is absent — but
    # a target day recorded as an upstream gap (ADR-0029) skips cleanly instead,
    # so a Gold backfill glides over gaps. An un-ingested target still fails.
    state = _load_state(sink)
    if date not in state["silver"]:
        if date in state["skipped"]:
            print(
                json.dumps(
                    {
                        "status": "skipped",
                        "dataset": dataset,
                        "derivative": derivative,
                        "date": date,
                        "partition_key": f"date={date}",
                        "reason": f"target silver day {date} is an upstream gap",
                    }
                )
            )
            print(f"gold build: {date} is an upstream gap; skipping", file=sys.stderr)
            return 0
        print(
            f"gold build: target silver partition for {date} is absent", file=sys.stderr
        )
        return 1

    # The rolling-window coverage gate lives in the binary: an incomplete
    # [date - max_horizon, date] window exits non-zero rather than writing a
    # degraded partition. The fake models only the state-level diff, so the gate
    # rejection is injected per dataset:derivative:date instead of derived from
    # a window, and checked after the upstream-gap branch so a genuine ADR-0029
    # gap still reports "skipped" cleanly even for a date in this list.
    if f"{dataset}:{derivative}:{date}" in _env_keys("FAKE_GOLD_GATE_FAIL_DATES"):
        print(
            f"gold build: silver coverage for {date} below coverage_min_ratio",
            file=sys.stderr,
        )
        return 1

    # Gold writes to its own derivative-named tree (gold/<derivative>/...).
    pdir = _partition_dir(sink, "gold", derivative, date)
    pdir.mkdir(parents=True, exist_ok=True)
    (pdir / "data.parquet").write_bytes(b"PAR1-fake-gold")
    year, month, day = (int(p) for p in date.split("-"))
    index = {
        "schema_version": 1,
        "dataset": dataset,
        "derivative": derivative,
        "partition": {"year": year, "month": month, "day": day},
        "row_count": 1,
        "tier": "gold",
        "retention_class": "validated",
    }
    (pdir / "_INDEX.json").write_text(json.dumps(index), encoding="utf-8")
    (pdir / "_DONE").write_text("", encoding="utf-8")

    built = state["gold"].setdefault(derivative, [])
    if date not in built:
        built.append(date)
    state["seq"] += 1
    state["gold_at"].setdefault(derivative, {})[date] = state["seq"]
    _record_partition(state, derivative, "gold", f"date={date}")
    _save_state(sink, state)

    print(f"wrote 1 gold rows -> {pdir}", file=sys.stderr)
    status: dict[str, object] = {
        "status": "written",
        "dataset": dataset,
        "derivative": derivative,
        "date": date,
        "partition_key": f"date={date}",
        "rows": 1,
        "parquet_sha256": "fake",
    }
    # The panel's trailing 30-day flip window is the *other* gate (corpus
    # ADR-0066 decision 8): a short window nulls the two flip counts and warns,
    # but still writes the partition — it is never a skip. The window itself is
    # not something the fake can derive, so it is injected per
    # dataset:derivative:date like the other binary-side outcomes.
    if derivative == "sovereignty-panel":
        short = f"{dataset}:{derivative}:{date}" in _env_keys(
            "FAKE_SHORT_FLIP_WINDOW_DATES"
        )
        if short:
            print(
                f"gold build: flip window for {date} is short; flip counts null",
                file=sys.stderr,
            )
        status["constellation_flips_30d"] = None if short else 3
        status["region_flips_30d"] = None if short else 1
    print(json.dumps(status))
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
    if not (pdir / "_DONE").is_file():
        print(f"[{date}] absent", file=sys.stderr)
        return 1

    # A present-but-corrupt partition (sha256 / _INDEX cross-check mismatch) has
    # no state-level cause the fake can reach, so it is injected per
    # dataset:tier:date, checked only after presence so it fires exclusively
    # on a partition that actually exists.
    if f"{dataset}:{tier}:{date}" in _env_keys("FAKE_VERIFY_FAIL_DATES"):
        print(f"[{date}] sha256 mismatch", file=sys.stderr)
        return 1

    print(f"[{date}] ok", file=sys.stderr)
    return 0


def _do_everef(args: list[str], sink: str) -> int:
    subcommand = args[1] if len(args) > 1 else ""
    if subcommand == "list":
        return _do_everef_list(args, sink)
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


_PARTITION_METADATA_SQL = re.compile(
    r"dataset = '([^']*)' AND tier = '([^']*)' AND partition_key = '([^']*)'"
)


def _do_state(args: list[str], sink: str) -> int:
    subcommand = args[1] if len(args) > 1 else ""
    if subcommand != "query":
        print(f"state: unsupported subcommand {subcommand!r}", file=sys.stderr)
        return 2

    sql = _pop_opt(args, "--sql") or ""
    _pop_opt(args, "--format")
    # A broken run-state read, on demand: the enrichment path treats a non-zero
    # `state query` as advisory, and that is only testable if the fake can fail.
    if os.environ.get("FAKE_STATE_QUERY_FAIL"):
        print("state query: run-state is locked", file=sys.stderr)
        return 1
    state = _load_state(sink)
    # Materialisation-metadata enrichment: the per-partition columns for one
    # `(dataset, tier, partition_key)` triple, keyed on the run-state key
    # (`date=`/`build=`/`month=`/`latest`) — a bare Dagster key matches nothing,
    # exactly as against the real binary.
    if "retention_class" in sql:
        match = _PARTITION_METADATA_SQL.search(sql)
        if match is None:
            print("state query: unrecognised partitions query", file=sys.stderr)
            return 2
        dataset, tier, partition_key = match.groups()
        facts = state["partition_facts"].get(f"{dataset}|{tier}|{partition_key}")
        print(json.dumps([facts] if facts is not None else []))
        return 0
    # The news/transcripts listed-vs-archived asset checks count the seen-ledger
    # (ADR-0045), keyed on the dataset named in the WHERE clause.
    if "seen_documents" in sql:
        dataset = ""
        if "'news'" in sql:
            dataset = "news"
        elif "'transcripts'" in sql:
            dataset = "transcripts"
        archived = int(state["seen_documents"].get(dataset, 0))
        print(json.dumps([{"archived": archived}]))
        return 0
    # The killmails Gold-repair sensor asks which Gold partitions predate their
    # own Silver (corpus ADR-0060): `silver.last_seen_at > gold.last_seen_at`.
    if "s.last_seen_at > g.last_seen_at" in sql:
        derivative = ""
        for name in state.get("gold_at", {}):
            if f"'{name}'" in sql:
                derivative = name
                break
        gold_at = state["gold_at"].get(derivative, {})
        rows = [
            {"date": date}
            for date, written in sorted(gold_at.items())
            if state["silver_at"].get(date, 0) > written
        ]
        print(json.dumps(rows))
        return 0
    # The SDE Gold-repair term asks which changelog Gold partitions were built
    # before the *nearest* lower `sde` Silver committed — i.e. diffed across a
    # hole in the build sequence. Must precede the `dataset = 'sde-changelog'`
    # branch below: this SQL contains that substring too.
    if "g.last_seen_at < (" in sql:
        silver_at = {int(b): seq for b, seq in state["sde_silver_at"].items()}
        rows = []
        gold_at = sorted(state["sde_gold_at"].items(), key=lambda kv: int(kv[0]))
        for raw_build, written in gold_at:
            build = int(raw_build)
            lower = [b for b in silver_at if b < build]
            if not lower:
                # Baseline: the correlated subquery yields NULL, row drops out.
                continue
            if written < silver_at[max(lower)]:
                rows.append({"build": build})
        print(json.dumps(rows))
        return 0
    # The SDE Gold sensor subtracts the changelog builds it has already built
    # (dataset = 'sde-changelog', tier = gold) from the committed Silver set.
    if "dataset = 'sde-changelog'" in sql:
        rows = [
            {
                "dataset": "sde-changelog",
                "tier": "gold",
                "partition_key": f"build={build}",
            }
            for build in sorted(state["sde_gold"].get("sde-changelog", []))
        ]
        print(json.dumps(rows))
        return 0
    # The SDE Gold sensor + snapshot asset query committed Silver builds
    # (dataset = 'sde', ADR-0032).
    if "dataset = 'sde'" in sql:
        rows = [
            {"dataset": "sde", "tier": "silver", "partition_key": f"build={build}"}
            for build in sorted(int(b) for b in state["sde_silver"])
        ]
        print(json.dumps(rows))
        return 0
    # The MER Gold assets query committed `mer` blob Silver (corpus ADR-0058 §5).
    if "dataset = 'mer'" in sql:
        rows = [
            {
                "dataset": "mer",
                "tier": "silver",
                "partition_key": f"month={report_month}",
            }
            for report_month in sorted(state["mer_silver"])
        ]
        print(json.dumps(rows))
        return 0
    rows = [
        {"dataset": "market-history", "tier": "silver", "partition_key": f"date={date}"}
        for date in sorted(state["silver"])
    ]
    print(json.dumps(rows))
    return 0


def _do_killmails(args: list[str], sink: str) -> int:
    """Mimics `corpus killmails freshness` (corpus ADR-0060): the drift report.

    Upstream per-day counts come from `FAKE_KILLMAILS_TOTALS` (`date:count,...`);
    a committed Silver day whose recorded token differs is reported drifted. The
    fake stores the token the same way the real binary does — as the count the
    ingest materialised — via `FAKE_KILLMAILS_INGESTED` (defaulting to 1), so a
    test can make a day drift by raising only its upstream total.
    """
    subcommand = args[1] if len(args) > 1 else ""
    if subcommand != "freshness":
        print(f"killmails: unsupported subcommand {subcommand!r}", file=sys.stderr)
        return 2
    _pop_opt(args, "--dataset")
    _pop_opt(args, "--format")

    totals: dict[str, int] = {}
    for pair in os.environ.get("FAKE_KILLMAILS_TOTALS", "").split(","):
        pair = pair.strip()
        if not pair:
            continue
        date, count = pair.split(":")
        totals[date] = int(count)
    ingested = int(os.environ.get("FAKE_KILLMAILS_INGESTED", "1"))

    state = _load_state(sink)
    drifted = [
        {"date": date, "ingested_count": ingested, "upstream_count": totals[date]}
        for date in sorted(state["silver"])
        if date in totals and totals[date] != ingested
    ]
    print(f"[killmails] {len(drifted)} drifted", file=sys.stderr)
    print(json.dumps(drifted))
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
    if command == "live":
        return _do_live(args, sink)
    if command == "context":
        return _do_context(args, sink)
    if command == "state":
        return _do_state(args, sink)
    if command == "killmails":
        return _do_killmails(args, sink)
    if command == "news":
        return _do_news(args, sink)
    if command == "transcripts":
        return _do_transcripts(args, sink)
    if command == "enrich":
        return _do_enrich(args, sink)
    print(f"fake corpus: unknown command {command!r}", file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
