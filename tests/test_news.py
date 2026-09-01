"""Tests for the news Silver/Gold materialisation metadata (corpus ADR-0050/0053).

The news chain is non-partitioned and fetch-date driven, so what these cover is
the *run-state key* each site builds: a Gold tree is keyed on the derivative, a
single-derivative dataset (``news-embeddings``) on the dataset name itself. A
mismatched key matches no row and enriches nothing, silently — so the assertions
are on the presence of the run-state facts, not merely on a successful run.
"""

from __future__ import annotations

import dagster as dg
import pytest

from eve_industry_orchestration.defs import news

_DATE = "2026-07-10"


def _ingest(corpus, dataset: str) -> None:
    corpus.run(
        dg.build_asset_context(),
        "ingest",
        "--dataset",
        dataset,
        "--date",
        _DATE,
        "--sink-path",
        corpus.sink_path,
    )


def _run_config(asset_name: str, config: dict[str, object]) -> dg.RunConfig:
    return dg.RunConfig(ops={asset_name: {"config": config}})


def _metadata(result: dg.ExecuteInProcessResult) -> dict:
    (materialization,) = result.get_asset_materialization_events()
    return materialization.materialization.metadata


def test_silver_records_the_run_state_facts(corpus) -> None:
    result = dg.materialize(
        [news.news_silver],
        resources={"corpus": corpus},
        run_config=_run_config("news_silver", {"date": _DATE}),
    )

    assert result.success
    metadata = _metadata(result)
    # Identifying fields survive the merge, and the facts corpus recorded for the
    # partition it just wrote sit alongside them.
    assert metadata["dataset"].value == news.DATASET
    assert metadata["tier"].value == "silver"
    assert metadata["partition"].value == _DATE
    assert metadata["rows"].value == 1
    assert metadata["retention_class"].value == "validated"
    assert metadata["parquet_sha256"].value


def test_gold_keys_run_state_on_the_derivative(corpus) -> None:
    _ingest(corpus, news.DATASET)

    result = dg.materialize(
        [news.news_articles_gold],
        selection=[news.news_articles_gold],
        resources={"corpus": corpus},
        run_config=_run_config("news_articles_gold", {"date": _DATE}),
    )

    assert result.success
    metadata = _metadata(result)
    assert metadata["derivative"].value == "news-articles"
    # `corpus gold build` records the run-state row under the derivative name;
    # keying it on "news" would have matched no row.
    assert metadata["rows"].value == 1
    assert metadata["retention_class"].value == "validated"
    assert metadata["parquet_sha256"].value


def test_embeddings_gold_keys_run_state_on_the_dataset(corpus) -> None:
    # news-embeddings declares a single Gold derivative named after itself.
    _ingest(corpus, news.EMBEDDINGS_DATASET)

    result = dg.materialize(
        [news.news_embeddings_gold],
        selection=[news.news_embeddings_gold],
        resources={"corpus": corpus},
        run_config=_run_config("news_embeddings_gold", {"date": _DATE}),
    )

    assert result.success
    metadata = _metadata(result)
    assert metadata["dataset"].value == news.EMBEDDINGS_DATASET
    assert metadata["rows"].value == 1
    assert metadata["parquet_sha256"].value


def test_enrichment_is_advisory(corpus, monkeypatch: pytest.MonkeyPatch) -> None:
    """A broken run-state read must not fail a materialisation corpus completed."""
    monkeypatch.setenv("FAKE_STATE_QUERY_FAIL", "1")

    result = dg.materialize(
        [news.news_silver],
        resources={"corpus": corpus},
        run_config=_run_config("news_silver", {"date": _DATE}),
    )

    assert result.success
    metadata = _metadata(result)
    assert metadata["partition"].value == _DATE
    assert "rows" not in metadata


def test_bronze_is_deliberately_unenriched(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    # `news_bronze` archives raw bytes under `_MANIFEST.json`; corpus records a
    # `partitions` row only for the parquet tiers, so there is no row to read.
    # Pin that: a later drive-by `partition_metadata` call here would match no
    # row and warn on every scheduled run.
    from eve_industry_orchestration.defs.corpus_resource import CorpusResource

    def _fail(self, dataset, tier, partition_key):  # pragma: no cover - never runs
        raise AssertionError(f"news bronze enriched against {dataset}/{tier}")

    monkeypatch.setattr(CorpusResource, "partition_metadata", _fail)

    result = news.news_bronze(dg.build_asset_context(), corpus)

    assert result.metadata["dataset"] == news.DATASET
    assert result.metadata["tier"] == "bronze"
    assert "retention_class" not in result.metadata
    assert "rows" not in result.metadata


def test_embeddings_bronze_is_deliberately_unenriched(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    # `news_embeddings_bronze` archives raw vectors the same way; no run-state
    # row describes them either. Pin the same decision for this asset.
    from eve_industry_orchestration.defs.corpus_resource import CorpusResource

    def _fail(self, dataset, tier, partition_key):  # pragma: no cover - never runs
        raise AssertionError(
            f"news embeddings bronze enriched against {dataset}/{tier}"
        )

    monkeypatch.setattr(CorpusResource, "partition_metadata", _fail)

    result = news.news_embeddings_bronze(
        dg.build_asset_context(), corpus, news.NewsEmbedConfig(date=_DATE)
    )

    assert result.metadata["dataset"] == news.EMBEDDINGS_DATASET
    assert result.metadata["tier"] == "bronze"
    assert result.metadata["partition"] == _DATE
    assert "retention_class" not in result.metadata
    assert "rows" not in result.metadata
