"""Tests for the Bronze-only context datasets news + transcripts (corpus ADR-0045/0046/0048).

These datasets break the Silver/Gold mould: a single non-partitioned asset per
dataset shells ``corpus context fetch`` (one dense fetch-date Bronze partition),
driven by a daily schedule; the historical sweep is a manually-triggered backfill
job with a paid-work cap. No Silver, no Gold, no ready-dates sensor.
"""

from __future__ import annotations

import dagster as dg

from eve_industry_orchestration.defs.news import (
    NewsBackfillConfig,
    news_backfill_job,
    news_bronze,
)
from eve_industry_orchestration.defs.sensors import (
    news_bronze_schedule,
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
    assert news_bronze_schedule.cron_schedule == "0 22 * * *"
    assert transcripts_bronze_schedule.cron_schedule == "30 22 * * *"
    assert news_bronze_schedule.default_status is dg.DefaultScheduleStatus.STOPPED
    assert (
        transcripts_bronze_schedule.default_status is dg.DefaultScheduleStatus.STOPPED
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
