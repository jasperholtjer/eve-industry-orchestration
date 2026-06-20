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
    with pytest.raises(dg.Failure):
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
