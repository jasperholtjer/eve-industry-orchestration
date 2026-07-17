"""Tests for the Bronze-only context datasets news + transcripts (corpus ADR-0045/0046/0048).

These datasets break the Silver/Gold mould: a single non-partitioned asset per
dataset shells ``corpus context fetch`` (one dense fetch-date Bronze partition),
driven by a daily schedule; the historical sweep is a manually-triggered backfill
job with a paid-work cap. No Silver, no Gold, no ready-dates sensor.
"""

from __future__ import annotations

import dagster as dg
import pytest

from eve_industry_orchestration.defs.corpus_resource import CorpusResource
from eve_industry_orchestration.defs.news import (
    GOLD_ASSETS,
    KNOWN_LISTED_NOT_ARCHIVED,
    NewsBackfillConfig,
    NewsDateConfig,
    NewsEmbedConfig,
    news_backfill_job,
    news_bronze,
    news_embeddings_bronze,
    news_embeddings_gold,
    news_embeddings_silver,
    news_entity_mentions_gold,
    news_listed_vs_archived,
    news_silver,
)
from eve_industry_orchestration.defs.sensors import (
    news_daily_schedule,
    transcripts_daily_schedule,
)
from eve_industry_orchestration.defs.transcripts import (
    GOLD_ASSETS as TRANSCRIPTS_GOLD_ASSETS,
)
from eve_industry_orchestration.defs.transcripts import (
    KNOWN_SCANNED_NOT_ARCHIVED,
    TranscriptsBackfillConfig,
    TranscriptsDateConfig,
    TranscriptsEmbedConfig,
    transcripts_backfill_job,
    transcripts_bronze,
    transcripts_embeddings_bronze,
    transcripts_embeddings_gold,
    transcripts_embeddings_silver,
    transcripts_entity_mentions_gold,
    transcripts_listed_vs_archived,
    transcripts_silver,
)
from tests.conftest import DATASETS_DIR

# --- daily fetch asset -----------------------------------------------------


def test_news_bronze_fetches_and_surfaces_metadata(corpus) -> None:
    result = news_bronze(dg.build_asset_context(), corpus)

    assert result.metadata["dataset"] == "news"
    assert result.metadata["tier"] == "bronze"
    assert result.metadata["partition"] == "year=2026/month=07/day=10"
    assert result.metadata["objects"] == 12
    assert result.metadata["new_documents"] == 5


def test_transcripts_bronze_fetches_and_surfaces_metadata(corpus) -> None:
    result = transcripts_bronze(dg.build_asset_context(), corpus)

    assert result.metadata["dataset"] == "transcripts"
    assert result.metadata["tier"] == "bronze"
    assert result.metadata["partition"] == "year=2026/month=07/day=10"


def test_context_assets_are_not_partitioned() -> None:
    # Keyed on the fetch date, one dense partition per daily run — never a Dagster
    # partition matrix. Both assets must stay non-partitioned.
    assert news_bronze.partitions_def is None
    assert transcripts_bronze.partitions_def is None


def test_context_assets_join_no_pool() -> None:
    # Neither dataset touches EVE Ref or ESI, so neither borrows a concurrency pool;
    # request pacing lives in the binary.
    assert news_bronze.op.pool is None
    assert transcripts_bronze.op.pool is None


# --- daily schedules -------------------------------------------------------


def test_schedules_are_daily_and_stopped() -> None:
    assert news_daily_schedule.cron_schedule == "0 22 * * *"
    assert transcripts_daily_schedule.cron_schedule == "30 22 * * *"
    assert news_daily_schedule.default_status is dg.DefaultScheduleStatus.STOPPED
    assert transcripts_daily_schedule.default_status is dg.DefaultScheduleStatus.STOPPED


# --- news Silver + Gold chain (corpus ADR-0050/0052) ----------------------

_DATE = "2026-07-10"


def test_news_silver_ingests_the_configured_fetch_date(corpus) -> None:
    result = news_silver(dg.build_asset_context(), corpus, NewsDateConfig(date=_DATE))

    assert result.metadata["tier"] == "silver"
    assert result.metadata["partition"] == _DATE


def test_news_silver_defaults_to_today(corpus) -> None:
    # No run-config: the chain processes the fetch date `news_bronze` just archived.
    assert NewsDateConfig().date is None


def test_news_gold_builds_every_derivative(corpus) -> None:
    news_silver(dg.build_asset_context(), corpus, NewsDateConfig(date=_DATE))

    for gold in GOLD_ASSETS:
        result = gold(dg.build_asset_context(), corpus, NewsDateConfig(date=_DATE))
        assert result.metadata["tier"] == "gold"
        assert result.metadata["partition"] == _DATE


def test_news_chain_is_not_partitioned() -> None:
    # Fetch-date keyed like Bronze; a past date is re-run via run-config, never a
    # Dagster partition matrix (news.yaml declares no served_start to anchor one).
    assert news_silver.partitions_def is None
    for gold in GOLD_ASSETS:
        assert gold.partitions_def is None


def test_entity_mentions_depends_on_the_sde_snapshot_gold() -> None:
    # Cross-dataset Gold input (ADR-0052): the vocabulary comes from the `sde-*`
    # snapshot trees, so the SDE snapshot Gold is a real upstream.
    deps = news_entity_mentions_gold.asset_deps[
        dg.AssetKey(["news_entity_mentions_gold"])
    ]
    upstream = {key.to_user_string() for key in deps}
    assert upstream == {"news_silver", "sde_snapshot"}


# --- news-embeddings: enrich → Silver → Gold (corpus ADR-0053) -------------


def test_news_embeddings_chain_materialises(corpus) -> None:
    bronze = news_embeddings_bronze(
        dg.build_asset_context(), corpus, NewsEmbedConfig(date=_DATE, limit=100)
    )
    silver = news_embeddings_silver(
        dg.build_asset_context(), corpus, NewsDateConfig(date=_DATE)
    )
    gold = news_embeddings_gold(
        dg.build_asset_context(), corpus, NewsDateConfig(date=_DATE)
    )

    for result, tier in ((bronze, "bronze"), (silver, "silver"), (gold, "gold")):
        assert result.metadata["dataset"] == "news-embeddings"
        assert result.metadata["tier"] == tier
        assert result.metadata["partition"] == _DATE


def test_news_embeddings_bronze_holds_its_own_limit_one_pool() -> None:
    # 4.4 GB RSS per embed run: its own pool (limit 1 in redeploy.sh) is what stops
    # two embeds from ever overlapping. The `heavy` pool's limit of 2 would not.
    assert news_embeddings_bronze.op.pool == "news_embed"
    # The deterministic halves are cheap — no pool, global cap only.
    assert news_embeddings_silver.op.pool is None
    assert news_embeddings_gold.op.pool is None


def test_news_embeddings_gold_depends_on_the_section_tree(corpus) -> None:
    # Cross-dataset Gold input (ADR-0053): the join reads `gold/news-sections`, so
    # it is a real upstream of the Gold build, not only of the embed step.
    deps = news_embeddings_gold.asset_deps[dg.AssetKey(["news_embeddings_gold"])]
    upstream = {key.to_user_string() for key in deps}
    assert upstream == {"news_embeddings_silver", "news_sections_gold"}


def test_news_embeddings_bronze_fails_without_the_model_artifact(
    corpus_binary, tmp_path
) -> None:
    # No ONNX artifact ⇒ loud failure, never a silent fallback generation.
    sink = tmp_path / "naked-sink"
    sink.mkdir()
    naked = CorpusResource(
        binary_path=corpus_binary,
        datasets_dir=str(DATASETS_DIR),
        sink_path=str(sink),
    )

    with pytest.raises(dg.Failure):
        news_embeddings_bronze(dg.build_asset_context(), naked, NewsEmbedConfig())


def test_news_embeddings_ride_the_news_group_schedule() -> None:
    # No new schedule: the group-targeted daily schedule picks them up in dep order.
    embeddings = (news_embeddings_bronze, news_embeddings_silver, news_embeddings_gold)
    for asset in embeddings:
        assert asset.get_asset_spec().group_name == "news"
        assert asset.partitions_def is None


# --- asset check: listed vs archived (design §1.4) -------------------------


def test_listed_vs_archived_reports_the_delta_without_failing(corpus) -> None:
    # 19 listed (fake match-stats) vs 12 archived (fake seen-ledger) = the known
    # 7-slug cohort that 500s at CCP. Expected metadata, never a failure.
    news_bronze(dg.build_asset_context(), corpus)

    result = news_listed_vs_archived(dg.build_asset_check_context(), corpus)

    assert result.passed
    assert result.severity is dg.AssetCheckSeverity.WARN
    assert result.metadata["listed"].value == 19
    assert result.metadata["archived"].value == 12
    assert result.metadata["listed_not_archived"].value == KNOWN_LISTED_NOT_ARCHIVED


def test_listed_vs_archived_passes_when_the_cohort_grows(corpus, monkeypatch) -> None:
    monkeypatch.setenv("FAKE_NEWS_LISTED", "25")
    news_bronze(dg.build_asset_context(), corpus)

    result = news_listed_vs_archived(dg.build_asset_check_context(), corpus)

    assert result.passed
    assert result.metadata["listed_not_archived"].value == 13


def test_listed_vs_archived_is_non_blocking() -> None:
    assert (
        news_listed_vs_archived.check_specs_by_output_name["result"].blocking is False
    )


# --- backfill jobs (manually-triggered, run-config cap) --------------------


def test_news_backfill_job_runs_capped(corpus) -> None:
    result = news_backfill_job.execute_in_process(
        run_config=dg.RunConfig(
            ops={"news_backfill_op": NewsBackfillConfig(max_articles=100)}
        ),
        resources={"corpus": corpus},
    )

    assert result.success


def test_news_backfill_job_runs_uncapped(corpus) -> None:
    # No run-config: max_articles defaults to None → an uncapped sweep.
    result = news_backfill_job.execute_in_process(resources={"corpus": corpus})

    assert result.success


def test_transcripts_backfill_job_runs_capped(corpus) -> None:
    result = transcripts_backfill_job.execute_in_process(
        run_config=dg.RunConfig(
            ops={"transcripts_backfill_op": TranscriptsBackfillConfig(max_videos=50)}
        ),
        resources={"corpus": corpus},
    )

    assert result.success


def test_transcripts_backfill_defaults_to_90() -> None:
    # A bare run must never sweep uncapped and blow the paid Supadata budget.
    assert TranscriptsBackfillConfig().max_videos == 90


def test_transcripts_backfill_job_runs_with_default_cap(corpus) -> None:
    # No run-config: the 90-video default applies, so the op still passes
    # --max-videos to the binary.
    result = transcripts_backfill_job.execute_in_process(resources={"corpus": corpus})

    assert result.success


# --- transcripts Silver + Gold chain (corpus ADR-0055) --------------------


def test_transcripts_silver_ingests_the_configured_fetch_date(corpus) -> None:
    result = transcripts_silver(
        dg.build_asset_context(), corpus, TranscriptsDateConfig(date=_DATE)
    )

    assert result.metadata["tier"] == "silver"
    assert result.metadata["partition"] == _DATE


def test_transcripts_silver_defaults_to_today(corpus) -> None:
    # No run-config: the chain processes the fetch date `transcripts_bronze` archived.
    assert TranscriptsDateConfig().date is None


def test_transcripts_gold_builds_every_derivative(corpus) -> None:
    transcripts_silver(
        dg.build_asset_context(), corpus, TranscriptsDateConfig(date=_DATE)
    )

    for gold in TRANSCRIPTS_GOLD_ASSETS:
        result = gold(
            dg.build_asset_context(), corpus, TranscriptsDateConfig(date=_DATE)
        )
        assert result.metadata["tier"] == "gold"
        assert result.metadata["partition"] == _DATE


def test_transcripts_chain_is_not_partitioned() -> None:
    # Fetch-date keyed like Bronze; a past date is re-run via run-config, never a
    # Dagster partition matrix (transcripts.yaml declares no served_start).
    assert transcripts_silver.partitions_def is None
    for gold in TRANSCRIPTS_GOLD_ASSETS:
        assert gold.partitions_def is None


def test_transcripts_entity_mentions_depends_on_the_sde_snapshot_gold() -> None:
    # Cross-dataset Gold input (ADR-0055): the vocabulary comes from the `sde-*`
    # snapshot trees, so the SDE snapshot Gold is a real upstream.
    deps = transcripts_entity_mentions_gold.asset_deps[
        dg.AssetKey(["transcripts_entity_mentions_gold"])
    ]
    upstream = {key.to_user_string() for key in deps}
    assert upstream == {"transcripts_silver", "sde_snapshot"}


# --- transcripts-embeddings: enrich → Silver → Gold (corpus ADR-0053) ------


def test_transcripts_embeddings_chain_materialises(corpus) -> None:
    bronze = transcripts_embeddings_bronze(
        dg.build_asset_context(), corpus, TranscriptsEmbedConfig(date=_DATE, limit=100)
    )
    silver = transcripts_embeddings_silver(
        dg.build_asset_context(), corpus, TranscriptsDateConfig(date=_DATE)
    )
    gold = transcripts_embeddings_gold(
        dg.build_asset_context(), corpus, TranscriptsDateConfig(date=_DATE)
    )

    for result, tier in ((bronze, "bronze"), (silver, "silver"), (gold, "gold")):
        assert result.metadata["dataset"] == "transcripts-embeddings"
        assert result.metadata["tier"] == tier
        assert result.metadata["partition"] == _DATE


def test_transcripts_embeddings_bronze_shares_the_news_embed_pool() -> None:
    # Both datasets' embeds run the same ~4.4 GB ONNX model, so they share ONE
    # limit-1 pool — no two embeds (news or transcripts) ever overlap on the box.
    assert transcripts_embeddings_bronze.op.pool == "news_embed"
    # The deterministic halves are cheap — no pool, global cap only.
    assert transcripts_embeddings_silver.op.pool is None
    assert transcripts_embeddings_gold.op.pool is None


def test_transcripts_embeddings_gold_depends_on_the_section_tree(corpus) -> None:
    # Cross-dataset Gold input (ADR-0053): the join reads `gold/transcripts-sections`,
    # so it is a real upstream of the Gold build, not only of the embed step.
    deps = transcripts_embeddings_gold.asset_deps[
        dg.AssetKey(["transcripts_embeddings_gold"])
    ]
    upstream = {key.to_user_string() for key in deps}
    assert upstream == {"transcripts_embeddings_silver", "transcripts_sections_gold"}


def test_transcripts_embeddings_bronze_fails_without_the_model_artifact(
    corpus_binary, tmp_path
) -> None:
    # No ONNX artifact ⇒ loud failure, never a silent fallback generation.
    sink = tmp_path / "naked-sink"
    sink.mkdir()
    naked = CorpusResource(
        binary_path=corpus_binary,
        datasets_dir=str(DATASETS_DIR),
        sink_path=str(sink),
    )

    with pytest.raises(dg.Failure):
        transcripts_embeddings_bronze(
            dg.build_asset_context(), naked, TranscriptsEmbedConfig()
        )


def test_transcripts_embeddings_ride_the_transcripts_group_schedule() -> None:
    # No new schedule: the group-targeted daily schedule picks them up in dep order.
    embeddings = (
        transcripts_embeddings_bronze,
        transcripts_embeddings_silver,
        transcripts_embeddings_gold,
    )
    for asset in embeddings:
        assert asset.get_asset_spec().group_name == "transcripts"
        assert asset.partitions_def is None


# --- asset check: scanned vs archived (plan §3) ---------------------------


def test_transcripts_listed_vs_archived_reports_the_delta_without_failing(
    corpus,
) -> None:
    # 12 scanned (fake match-stats) vs 12 archived (fake seen-ledger) = delta 0, the
    # healthy ledger↔Silver reconciliation. Expected metadata, never a failure.
    transcripts_bronze(dg.build_asset_context(), corpus)

    result = transcripts_listed_vs_archived(dg.build_asset_check_context(), corpus)

    assert result.passed
    assert result.severity is dg.AssetCheckSeverity.WARN
    assert result.metadata["listed"].value == 12
    assert result.metadata["archived"].value == 12
    assert result.metadata["scanned_not_archived"].value == KNOWN_SCANNED_NOT_ARCHIVED


def test_transcripts_listed_vs_archived_passes_when_the_delta_grows(
    corpus, monkeypatch
) -> None:
    monkeypatch.setenv("FAKE_TRANSCRIPTS_VIDEOS", "15")
    transcripts_bronze(dg.build_asset_context(), corpus)

    result = transcripts_listed_vs_archived(dg.build_asset_check_context(), corpus)

    assert result.passed
    assert result.metadata["scanned_not_archived"].value == 3


def test_transcripts_listed_vs_archived_is_non_blocking() -> None:
    assert (
        transcripts_listed_vs_archived.check_specs_by_output_name["result"].blocking
        is False
    )
