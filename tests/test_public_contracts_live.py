"""Tests for the public-contracts-live current-overwrite asset and schedule (ADR-0068)."""

from __future__ import annotations

from pathlib import Path

import dagster as dg

from eve_industry_orchestration.defs.public_contracts_live import (
    DATASET,
    public_contracts_live_gold,
)
from eve_industry_orchestration.defs.sensors import public_contracts_live_schedule


def test_live_asset_overwrites_current(corpus) -> None:
    # No Silver, no ready-dates: the asset just shells `corpus live build`, which
    # the fake binary answers with a `written` status over the flat current/ tree.
    result = public_contracts_live_gold(dg.build_asset_context(), corpus)

    partition_dir = Path(corpus.sink_path) / "gold" / DATASET / "current"
    assert (partition_dir / "_DONE").exists()

    assert result.metadata["partition"] == "current"
    assert result.metadata["dataset"] == DATASET
    assert result.metadata["tier"] == "gold"
    assert result.metadata["rows"] == 1
    # `snapshot_at` is the payload's own scrape instant — the freshness the
    # `.v2.tar.bz2` filename cannot give, because its seconds field drifts.
    assert result.metadata["snapshot_at"] == "2026-06-26T12:00:00+00:00"
    assert result.metadata["snapshot_file"].endswith(".v2.tar.bz2")
    assert result.metadata["date"] == "2026-06-26"


def test_live_asset_is_not_partitioned() -> None:
    # The live asset must stay non-partitioned: it always targets current/.
    assert public_contracts_live_gold.partitions_def is None


def test_live_schedule_targets_the_asset_half_hourly() -> None:
    # The upstream publishes on a ~30-minute rhythm (~47 snapshots a day).
    assert public_contracts_live_schedule.cron_schedule == "15,45 * * * *"
    assert (
        public_contracts_live_schedule.default_status
        is dg.DefaultScheduleStatus.STOPPED
    )
    assert public_contracts_live_schedule.job.selection.resolve(
        [public_contracts_live_gold]
    ) == {dg.AssetKey("public_contracts_live_gold")}


def test_live_asset_issues_no_state_query(corpus, monkeypatch) -> None:
    # `corpus live build` writes no run-state row (see the asset body), so the
    # metadata-enrichment row deliberately leaves this site unenriched. Pin that:
    # a later drive-by `partition_metadata` call here would match no row and warn
    # on every scheduled run.
    from eve_industry_orchestration.defs.corpus_resource import CorpusResource

    def _fail(self, sql: str):  # pragma: no cover - must never run
        raise AssertionError(f"live asset queried run-state: {sql}")

    monkeypatch.setattr(CorpusResource, "state_query", _fail)
    result = public_contracts_live_gold(dg.build_asset_context(), corpus)

    assert result.metadata["partition"] == "current"
    assert "retention_class" not in result.metadata


def test_absent_freshness_key_is_omitted_not_defaulted(corpus, monkeypatch) -> None:
    # The advisory rule: a freshness key the binary did not report stays absent,
    # so the metadata never claims a freshness the run did not observe. The fake
    # binary gains no knob for this — its job is to mimic the contract the real
    # binary honours — so the status dict is stubbed at the resource seam.
    from eve_industry_orchestration.defs.corpus_resource import CorpusResource

    def _run_without_snapshot_at(self, context, *args: str):
        return {
            "status": "written",
            "dataset": DATASET,
            "rows": 1,
            "date": "2026-06-26",
        }

    monkeypatch.setattr(CorpusResource, "run", _run_without_snapshot_at)
    result = public_contracts_live_gold(dg.build_asset_context(), corpus)

    assert "snapshot_at" not in result.metadata
    assert "snapshot_file" not in result.metadata
    assert result.metadata["rows"] == 1
    assert result.metadata["date"] == "2026-06-26"


def test_null_snapshot_at_is_omitted_not_defaulted(corpus, monkeypatch) -> None:
    # `corpus live build` always emits `"snapshot_at": batch_snapshot_at(&batch)`,
    # which serialises to JSON `null` when the built batch has zero rows (a
    # truncated upstream snapshot whose contracts.csv decodes to no rows). The
    # key is present but `null`, and must be treated the same as absent.
    from eve_industry_orchestration.defs.corpus_resource import CorpusResource

    def _run_with_null_snapshot_at(self, context, *args: str):
        return {
            "status": "written",
            "dataset": DATASET,
            "rows": 0,
            "date": "2026-06-26",
            "snapshot_at": None,
        }

    monkeypatch.setattr(CorpusResource, "run", _run_with_null_snapshot_at)
    result = public_contracts_live_gold(dg.build_asset_context(), corpus)

    assert "snapshot_at" not in result.metadata
    assert result.metadata["rows"] == 0
    assert result.metadata["date"] == "2026-06-26"
