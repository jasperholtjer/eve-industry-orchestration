"""Tests for the CorpusResource subprocess wrapper against the fake binary."""

from __future__ import annotations

from pathlib import Path

import dagster as dg
import pytest


def test_run_writes_contract_then_verifies(corpus) -> None:
    context = dg.build_asset_context()
    corpus.run(
        context,
        "ingest",
        "--dataset",
        "market-history",
        "--date",
        "2024-01-15",
        "--sink-path",
        corpus.sink_path,
    )
    corpus.run(
        context,
        "verify",
        "--dataset",
        "market-history",
        "--date",
        "2024-01-15",
        "--tier",
        "silver",
        "--sink-path",
        corpus.sink_path,
    )

    pdir = (
        Path(corpus.sink_path)
        / "silver"
        / "market-history"
        / "year=2024"
        / "month=01"
        / "day=15"
    )
    assert (pdir / "_DONE").is_file()
    assert (pdir / "_INDEX.json").is_file()
    assert (pdir / "data.parquet").is_file()


def test_nonzero_exit_raises_failure(corpus) -> None:
    context = dg.build_asset_context()
    # verify on a never-ingested partition exits 1 (mirrors the real binary).
    with pytest.raises(dg.Failure) as excinfo:
        corpus.run(
            context,
            "verify",
            "--dataset",
            "market-history",
            "--date",
            "2099-01-01",
            "--tier",
            "silver",
            "--sink-path",
            corpus.sink_path,
        )
    # The Failure surfaces the corpus output tail, not just the command line, so
    # the real reason is diagnosable straight from the Dagster Failure.
    description = excinfo.value.description or ""
    assert "corpus exited 1" in description
    assert "absent" in description


def test_gold_builds_then_verifies(corpus) -> None:
    context = dg.build_asset_context()
    corpus.run(
        context, "ingest", "--dataset", "market-history",
        "--date", "2024-01-15", "--sink-path", corpus.sink_path,
    )  # fmt: skip
    corpus.run(
        context, "gold", "build", "--dataset", "market-history",
        "--date", "2024-01-15", "--sink-path", corpus.sink_path,
    )  # fmt: skip
    corpus.run(
        context, "verify", "--dataset", "market-history", "--date", "2024-01-15",
        "--tier", "gold", "--sink-path", corpus.sink_path,
    )  # fmt: skip

    pdir = (
        Path(corpus.sink_path)
        / "gold"
        / "market-history"
        / "year=2024"
        / "month=01"
        / "day=15"
    )
    assert (pdir / "_DONE").is_file()
    assert (pdir / "_INDEX.json").is_file()
    assert (pdir / "data.parquet").is_file()


def test_gold_without_target_silver_raises_failure(corpus) -> None:
    context = dg.build_asset_context()
    # The builder cannot derive Gold without the target-day Silver row(s).
    with pytest.raises(dg.Failure):
        corpus.run(
            context, "gold", "build", "--dataset", "market-history",
            "--date", "2024-01-15", "--sink-path", corpus.sink_path,
        )  # fmt: skip


def test_interrupt_mid_stream_kills_subprocess(corpus, monkeypatch) -> None:
    from eve_industry_orchestration.defs import corpus_resource

    killed = {"kill": False, "wait": 0}

    class _FakeStream:
        def __iter__(self):
            yield "first line\n"
            # Mirror Dagster converting a SIGINT/SIGTERM into this error while
            # the run loop is consuming the corpus stream.
            raise dg.DagsterExecutionInterruptedError

    class _FakeProcess:
        stdout = _FakeStream()

        def poll(self):
            # Still running when the loop is interrupted, then dead after kill.
            return None if not killed["kill"] else -9

        def kill(self):
            killed["kill"] = True

        def wait(self):
            killed["wait"] += 1
            return -9

    monkeypatch.setattr(
        corpus_resource.subprocess, "Popen", lambda *a, **k: _FakeProcess()
    )
    context = dg.build_asset_context()
    with pytest.raises(dg.DagsterExecutionInterruptedError):
        corpus.run(
            context, "ingest", "--dataset", "market-history",
            "--date", "2024-01-15", "--sink-path", corpus.sink_path,
        )  # fmt: skip
    assert killed["kill"] is True
    assert killed["wait"] >= 1


def test_state_query_returns_rows(corpus) -> None:
    context = dg.build_asset_context()
    corpus.run(
        context,
        "ingest",
        "--dataset",
        "market-history",
        "--date",
        "2024-01-15",
        "--sink-path",
        corpus.sink_path,
    )
    rows = corpus.state_query("SELECT partition_key FROM partitions")
    assert {
        "dataset": "market-history",
        "tier": "silver",
        "partition_key": "date=2024-01-15",
    } in rows


def _sde_ingest(corpus, build: int) -> None:
    corpus.run(
        dg.build_asset_context(),
        "ingest",
        "--dataset",
        "sde",
        "--build",
        str(build),
        "--sink-path",
        corpus.sink_path,
    )


def _sde_changelog(corpus, build: int) -> None:
    corpus.run(
        dg.build_asset_context(),
        "gold",
        "build",
        "--dataset",
        "sde",
        "--derivative",
        "sde-changelog",
        "--build",
        str(build),
        "--sink-path",
        corpus.sink_path,
    )


def test_stale_changelog_builds_reports_a_diff_across_a_hole(
    corpus, monkeypatch
) -> None:
    """A changelog built while 200 was still missing is diffed against 100."""
    monkeypatch.setenv(
        "FAKE_SDE_BUILDS", "100:2025-09-01,200:2025-09-02,300:2025-09-03"
    )
    _sde_ingest(corpus, 100)
    _sde_ingest(corpus, 300)  # 300 raced ahead of 200 under `everef_download`
    _sde_changelog(corpus, 300)  # diffed against 100: the hole is not visible yet
    assert corpus.stale_changelog_builds() == []

    _sde_ingest(corpus, 200)
    # 200 is now the nearest lower committed Silver and postdates 300's Gold.
    assert corpus.stale_changelog_builds() == [300]


def test_stale_changelog_builds_clears_after_a_repair_rebuild(
    corpus, monkeypatch
) -> None:
    monkeypatch.setenv(
        "FAKE_SDE_BUILDS", "100:2025-09-01,200:2025-09-02,300:2025-09-03"
    )
    _sde_ingest(corpus, 100)
    _sde_ingest(corpus, 300)
    _sde_changelog(corpus, 300)
    _sde_ingest(corpus, 200)
    assert corpus.stale_changelog_builds() == [300]

    _sde_changelog(corpus, 300)  # rematerialise overwrites in place
    assert corpus.stale_changelog_builds() == []


def test_stale_changelog_builds_ignores_the_ordered_sequence(
    corpus, monkeypatch
) -> None:
    """No hole, no stale build — and a baseline never reports."""
    monkeypatch.setenv("FAKE_SDE_BUILDS", "100:2025-09-01,200:2025-09-02")
    _sde_ingest(corpus, 100)
    _sde_changelog(corpus, 100)  # baseline: no lower Silver, subquery is NULL
    _sde_ingest(corpus, 200)
    _sde_changelog(corpus, 200)
    assert corpus.stale_changelog_builds() == []
