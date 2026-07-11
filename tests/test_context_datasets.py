"""Tests for the Bronze-only context datasets news + transcripts (corpus ADR-0045/0046/0048).

These datasets break the Silver/Gold mould: a single non-partitioned asset per
dataset shells ``corpus context fetch`` (one dense fetch-date Bronze partition),
driven by a daily schedule; the historical sweep is a manually-triggered backfill
job with a paid-work cap. No Silver, no Gold, no ready-dates sensor.
"""

from __future__ import annotations

import dagster as dg

from eve_industry_orchestration.defs.news import (
    GOLD_ASSETS,
    KNOWN_LISTED_NOT_ARCHIVED,
    NewsBackfillConfig,
    NewsDateConfig,
    news_backfill_job,
    news_bronze,
    news_entity_mentions_gold,
    news_listed_vs_archived,
    news_silver,
)
from eve_industry_orchestration.defs.sensors import (
    news_daily_schedule,
    transcripts_bronze_schedule,
)
from eve_industry_orchestration.defs.transcripts import (
    TranscriptsBackfillConfig,
    transcripts_backfill_job,
    transcripts_bronze,
)

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
    assert transcripts_bronze_schedule.cron_schedule == "30 22 * * *"
    assert news_daily_schedule.default_status is dg.DefaultScheduleStatus.STOPPED
    assert (
        transcripts_bronze_schedule.default_status is dg.DefaultScheduleStatus.STOPPED
    )


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
