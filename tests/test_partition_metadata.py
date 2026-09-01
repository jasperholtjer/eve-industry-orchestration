"""Tests for the advisory run-state enrichment behind ``MaterializeResult``.

The load-bearing case is the key scheme: run-state prefixes a partition key with
its scheme, so a bare Dagster key matches no row. Because the read is advisory
that failure is silent — an empty mapping on a green test — so every scheme test
asserts the metadata is *populated*, not merely that nothing raised.
"""

from __future__ import annotations

import subprocess

import dagster as dg
import pytest

from eve_industry_orchestration.defs.corpus_resource import (
    LATEST_KEY,
    build_key,
    date_key,
    month_key,
)

DATE = "2024-01-15"
SDE_BUILDS = "100:2025-09-01,200:2025-09-02"
MER_REPORTS = "2025-06-01,2025-07-01"


def _run(corpus, *args: str) -> None:
    corpus.run(dg.build_asset_context(), *args, "--sink-path", corpus.sink_path)


def _ingest_date(corpus, dataset: str = "market-history", date: str = DATE) -> None:
    _run(corpus, "ingest", "--dataset", dataset, "--date", date)


def test_key_helpers_produce_the_run_state_forms() -> None:
    assert date_key(DATE) == "date=2024-01-15"
    assert build_key(200) == "build=200"
    assert build_key("200") == "build=200"
    assert month_key("2025-06-01") == "month=2025-06-01"
    assert LATEST_KEY == "latest"


def test_date_key_partition_is_found(corpus) -> None:
    _ingest_date(corpus)
    metadata = corpus.partition_metadata("market-history", "silver", date_key(DATE))
    assert metadata["rows"] == 1
    assert metadata["retention_class"] == "validated"
    assert metadata["parquet_sha256"]


def test_build_key_partition_is_found(corpus, monkeypatch) -> None:
    monkeypatch.setenv("FAKE_SDE_BUILDS", SDE_BUILDS)
    _run(corpus, "ingest", "--dataset", "sde", "--build", "100")
    metadata = corpus.partition_metadata("sde", "silver", build_key(100))
    assert metadata["rows"] == 1
    assert metadata["retention_class"] == "validated"


def test_month_key_partition_is_found(corpus, monkeypatch) -> None:
    monkeypatch.setenv("FAKE_MER_REPORTS", MER_REPORTS)
    _run(corpus, "ingest", "--dataset", "mer", "--month", "2025-06")
    metadata = corpus.partition_metadata("mer", "silver", month_key("2025-06-01"))
    assert metadata["rows"] == 1
    assert metadata["retention_class"] == "validated"


def test_latest_key_partition_is_found(corpus, monkeypatch) -> None:
    monkeypatch.setenv("FAKE_SDE_BUILDS", SDE_BUILDS)
    _run(corpus, "ingest", "--dataset", "sde", "--build", "100")
    _run(
        corpus, "gold", "build", "--dataset", "sde",
        "--derivative", "sde-industry-products", "--build", "100",
    )  # fmt: skip
    metadata = corpus.partition_metadata("sde-industry-products", "gold", LATEST_KEY)
    assert metadata["rows"] == 1
    assert metadata["retention_class"] == "validated"


def test_bare_dagster_key_matches_nothing(corpus) -> None:
    """The mistake the named helpers exist to prevent, pinned as a fact."""
    _ingest_date(corpus)
    assert corpus.partition_metadata("market-history", "silver", DATE) == {}


def test_zero_row_partition_reports_its_zero(corpus, monkeypatch) -> None:
    """``rows: 0`` is a fact about the day, not an absent field."""
    monkeypatch.setenv("FAKE_PARTITION_ROWS", "0")
    _ingest_date(corpus)
    metadata = corpus.partition_metadata("market-history", "silver", date_key(DATE))
    assert metadata["rows"] == 0
    assert metadata["retention_class"] == "validated"


def test_absent_row_returns_an_empty_mapping(corpus) -> None:
    assert (
        corpus.partition_metadata("market-history", "silver", date_key("2099-01-01"))
        == {}
    )


def test_failing_query_is_advisory(corpus, monkeypatch) -> None:
    """A broken run-state read must not fail an already-successful run."""
    _ingest_date(corpus)
    monkeypatch.setenv("FAKE_STATE_QUERY_FAIL", "1")
    assert corpus.partition_metadata("market-history", "silver", date_key(DATE)) == {}


def test_unparseable_output_is_advisory(corpus, monkeypatch) -> None:
    from eve_industry_orchestration.defs import corpus_resource

    monkeypatch.setattr(
        corpus_resource.CorpusResource,
        "_capture",
        lambda self, *args, **kwargs: "not json at all",
    )
    assert corpus.partition_metadata("market-history", "silver", date_key(DATE)) == {}


def test_timing_out_query_is_advisory(corpus, monkeypatch) -> None:
    """A stalled sink must not hold the run open on a cosmetic read."""
    from eve_industry_orchestration.defs import corpus_resource

    seen: dict[str, float | None] = {}

    def _timeout(cmd, **kwargs):
        seen["timeout"] = kwargs.get("timeout")
        raise subprocess.TimeoutExpired(cmd, kwargs.get("timeout") or 0)

    monkeypatch.setattr(corpus_resource.subprocess, "run", _timeout)
    assert corpus.partition_metadata("market-history", "silver", date_key(DATE)) == {}
    # The bound is passed, not merely handled: without it the call cannot expire.
    assert seen["timeout"] == corpus_resource._STATE_QUERY_TIMEOUT_SECONDS


def test_malformed_key_never_raises(corpus) -> None:
    assert corpus.partition_metadata("market-history", "silver", "date='; DROP") == {}


@pytest.mark.parametrize("bad", ["Market-History", "silver'"])
def test_malformed_identifier_never_raises(corpus, bad: str) -> None:
    assert corpus.partition_metadata(bad, bad, date_key(DATE)) == {}
