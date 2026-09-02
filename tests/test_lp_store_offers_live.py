"""Tests for the lp-store-offers-live current-overwrite asset and schedule (ADR-0070)."""

from __future__ import annotations

from pathlib import Path

import dagster as dg

from eve_industry_orchestration.defs.lp_store_offers_live import (
    DATASET,
    lp_store_offers_live_gold,
)
from eve_industry_orchestration.defs.sensors import lp_store_offers_live_schedule


def test_live_asset_writes_both_trees(corpus) -> None:
    # No Silver, no ready-dates: the asset shells `corpus live build`, which the
    # fake answers with the multi-partition `written` status over both flat trees.
    result = lp_store_offers_live_gold(dg.build_asset_context(), corpus)

    assert result.metadata["dataset"] == DATASET
    assert result.metadata["partition"] == "current"
    assert result.metadata["source"] == "esi"
    assert "snapshot_at" in result.metadata
    # The fan-out's own shape: how many stores were polled, and how many were
    # legitimately empty — the number that says a thin result is real.
    assert result.metadata["corporations"] == 283
    assert result.metadata["empty_stores"] == 102

    sink = Path(corpus.sink_path)
    assert (sink / "gold" / "lp-store-offers" / "current" / "_DONE").exists()
    assert (sink / "gold" / "lp-store-offer-items" / "current" / "_DONE").exists()


def test_metadata_carries_a_row_count_per_derivative(corpus) -> None:
    # The one live status shape that is multi-partition: row counts arrive per
    # derivative, so reading a top-level `rows` would silently record nothing.
    result = lp_store_offers_live_gold(dg.build_asset_context(), corpus)

    assert result.metadata["rows.lp-store-offers"] == 2
    assert result.metadata["rows.lp-store-offer-items"] == 3
    assert "rows" not in result.metadata


def test_one_invocation_writes_both_trees(corpus, monkeypatch) -> None:
    # One fan-out, one asset: two invocations would re-fetch the same 284 ESI
    # responses and could leave one tree fresh against a stale other.
    from eve_industry_orchestration.defs.corpus_resource import CorpusResource

    calls: list[tuple[str, ...]] = []
    original = CorpusResource.run

    def _record(self, context, *args: str):
        calls.append(args)
        return original(self, context, *args)

    monkeypatch.setattr(CorpusResource, "run", _record)
    lp_store_offers_live_gold(dg.build_asset_context(), corpus)

    assert len(calls) == 1
    assert calls[0][:4] == ("live", "build", "--dataset", DATASET)
    # `--sink-path` is an option of `live build`, not a global flag.
    assert calls[0][4] == "--sink-path"


def test_absent_status_key_is_not_invented(corpus, monkeypatch) -> None:
    # A key the binary omits is left out rather than defaulted, and the
    # materialisation still succeeds.
    from eve_industry_orchestration.defs.corpus_resource import CorpusResource

    monkeypatch.setattr(
        CorpusResource,
        "run",
        lambda self, context, *args: {"status": "written", "source": "esi"},
    )
    result = lp_store_offers_live_gold(dg.build_asset_context(), corpus)

    assert result.metadata["partition"] == "current"
    assert "snapshot_at" not in result.metadata
    assert "corporations" not in result.metadata
    assert not any(str(key).startswith("rows.") for key in result.metadata)


def test_live_asset_is_not_partitioned() -> None:
    # Non-partitioned: every run targets the same two current/ trees.
    assert lp_store_offers_live_gold.partitions_def is None


def test_live_asset_joins_no_pool() -> None:
    # ESI, not EVE Ref: it must not borrow the everef_download politeness pool,
    # and ~6 MB of JSON is not memory-bearing, so not `heavy` either. Global cap
    # only.
    assert lp_store_offers_live_gold.op.pool is None


def test_live_schedule_targets_the_asset_daily() -> None:
    # Daily past the 11:05 UTC cache roll, not hourly: every store expires at the
    # same instant, so one run a day fetches one clean generation.
    assert lp_store_offers_live_schedule.cron_schedule == "30 11 * * *"
    assert (
        lp_store_offers_live_schedule.default_status is dg.DefaultScheduleStatus.STOPPED
    )


def test_live_asset_issues_no_state_query(corpus, monkeypatch) -> None:
    # `corpus live build` writes no run-state row, so a `partition_metadata`
    # enrichment here would match nothing and warn on every scheduled run.
    from eve_industry_orchestration.defs.corpus_resource import CorpusResource

    def _fail(self, sql: str):  # pragma: no cover - must never run
        raise AssertionError(f"live asset queried run-state: {sql}")

    monkeypatch.setattr(CorpusResource, "state_query", _fail)
    result = lp_store_offers_live_gold(dg.build_asset_context(), corpus)

    assert result.metadata["partition"] == "current"
    assert "retention_class" not in result.metadata
