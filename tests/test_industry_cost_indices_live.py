"""Pins that the industry-cost-indices-live asset reads no run-state (ADR-0043).

The asset's own behaviour (current-overwrite, non-partitioned, hourly schedule)
is covered in ``test_industry_cost_indices.py``; this module holds only the
metadata-enrichment decision, so the reason it was made lives next to the test
that would catch it being undone.
"""

from __future__ import annotations

import dagster as dg

from eve_industry_orchestration.defs.industry_cost_indices_live import (
    industry_cost_indices_live_gold,
)


def test_live_asset_issues_no_state_query(corpus, monkeypatch) -> None:
    # `corpus live build` writes no run-state row (see the asset body), so the
    # metadata-enrichment row deliberately leaves this site unenriched. Pin that:
    # a later drive-by `partition_metadata` call here would match no row and warn
    # on every scheduled run.
    from eve_industry_orchestration.defs.corpus_resource import CorpusResource

    def _fail(self, sql: str):  # pragma: no cover - must never run
        raise AssertionError(f"live asset queried run-state: {sql}")

    monkeypatch.setattr(CorpusResource, "state_query", _fail)
    result = industry_cost_indices_live_gold(dg.build_asset_context(), corpus)

    assert result.metadata["partition"] == "current"
    assert "retention_class" not in result.metadata
