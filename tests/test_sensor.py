"""Tests for the market-history availability sensor."""

from __future__ import annotations

import dagster as dg
import pytest

from eve_industry_orchestration.defs.sensor_util import MAX_PARTITIONS_PER_TICK
from eve_industry_orchestration.defs.sensors import (
    market_history_availability_sensor,
    market_history_gold_sensor,
)


def _ingest(corpus, date: str) -> None:
    corpus.run(
        dg.build_asset_context(),
        "ingest",
        "--dataset",
        "market-history",
        "--date",
        date,
        "--sink-path",
        corpus.sink_path,
    )


def _build_gold(corpus, date: str) -> None:
    corpus.run(
        dg.build_asset_context(),
        "gold",
        "build",
        "--dataset",
        "market-history",
        "--date",
        date,
        "--sink-path",
        corpus.sink_path,
    )


def test_requests_runs_for_newly_available_dates(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FAKE_EVEREF_DATES", "2024-01-15,2024-01-16")
    context = dg.build_sensor_context(resources={"corpus": corpus})

    result = market_history_availability_sensor(context)

    by_partition = {rr.partition_key: rr for rr in result.run_requests}
    assert sorted(by_partition) == ["2024-01-15", "2024-01-16"]
    assert all(rr.run_key for rr in result.run_requests)
    # The run_key carries a rotating per-tick token so a still-missing date can be
    # re-requested; only its partition-identifying stem is stable.
    assert by_partition["2024-01-15"].run_key.startswith(
        "market-history-silver-2024-01-15-"
    )


def test_excludes_already_materialised_dates(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Status is keyed on corpus run-state: ingesting one date records it in the
    # state file, so missing-partitions (and thus the sensor) drops it.
    monkeypatch.setenv("FAKE_EVEREF_DATES", "2024-01-15,2024-01-16")
    asset_context = dg.build_asset_context()
    corpus.run(
        asset_context,
        "ingest",
        "--dataset",
        "market-history",
        "--date",
        "2024-01-15",
        "--sink-path",
        corpus.sink_path,
    )

    context = dg.build_sensor_context(resources={"corpus": corpus})
    result = market_history_availability_sensor(context)

    assert [rr.partition_key for rr in result.run_requests] == ["2024-01-16"]


def test_still_missing_date_is_retried_with_fresh_run_key(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A date that stays `missing` (e.g. upstream still incomplete, ADR-0041, so
    # the Silver no-op never commits it) must be re-requested on the next tick.
    # A static run_key would be deduped by Dagster after the first no-op run; the
    # rotating per-tick token keeps the retry a distinct, launchable run.
    monkeypatch.setenv("FAKE_EVEREF_DATES", "2024-01-15")

    first = market_history_availability_sensor(
        dg.build_sensor_context(resources={"corpus": corpus})
    )
    second = market_history_availability_sensor(
        dg.build_sensor_context(resources={"corpus": corpus}, cursor=first.cursor)
    )

    (first_rr,) = first.run_requests
    (second_rr,) = second.run_requests
    assert first_rr.partition_key == second_rr.partition_key == "2024-01-15"
    assert first_rr.run_key != second_rr.run_key


def test_in_flight_date_is_not_re_requested(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The rotating run_key must not launch a second run for a date whose prior
    # run is still in flight — that would race two corpus writers on one contract
    # dir. The in-flight guard drops such dates for this tick.
    monkeypatch.setenv("FAKE_EVEREF_DATES", "2024-01-15,2024-01-16")
    monkeypatch.setattr(
        "eve_industry_orchestration.defs.sensor_util._in_flight_partitions",
        lambda context, asset_key: {"2024-01-15"},
    )
    context = dg.build_sensor_context(resources={"corpus": corpus})

    result = market_history_availability_sensor(context)

    assert [rr.partition_key for rr in result.run_requests] == ["2024-01-16"]


def test_no_missing_dates_yields_no_requests(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FAKE_EVEREF_DATES", "")
    context = dg.build_sensor_context(resources={"corpus": corpus})

    result = market_history_availability_sensor(context)

    assert result.run_requests == []


def test_gold_sensor_requests_ready_dates(corpus) -> None:
    # A Silver partition whose window is complete (per the binary) shows up as
    # ready until its Gold partition is built.
    _ingest(corpus, "2024-01-15")
    context = dg.build_sensor_context(resources={"corpus": corpus})

    result = market_history_gold_sensor(context)

    by_partition = {rr.partition_key: rr for rr in result.run_requests}
    assert sorted(by_partition) == ["2024-01-15"]
    assert by_partition["2024-01-15"].run_key.startswith(
        "market-history-gold-2024-01-15-"
    )


def test_gold_sensor_excludes_already_built_dates(corpus) -> None:
    _ingest(corpus, "2024-01-15")
    _build_gold(corpus, "2024-01-15")
    context = dg.build_sensor_context(resources={"corpus": corpus})

    result = market_history_gold_sensor(context)

    assert result.run_requests == []


def test_gold_sensor_no_silver_yields_no_requests(corpus) -> None:
    context = dg.build_sensor_context(resources={"corpus": corpus})

    result = market_history_gold_sensor(context)

    assert result.run_requests == []


# --- Gold readiness: what the sensor may and may not decide -----------------


def test_gold_sensor_ignores_a_date_not_reported_ready(corpus) -> None:
    """Readiness comes from corpus alone: a date it drops is not requested.

    The built date leaves the readiness report while its Silver stays on disk,
    so a sensor deriving readiness from the tree would still request it.
    """
    _ingest(corpus, "2024-01-15")
    _ingest(corpus, "2024-01-16")
    _build_gold(corpus, "2024-01-15")
    context = dg.build_sensor_context(resources={"corpus": corpus})

    result = market_history_gold_sensor(context)

    assert [rr.partition_key for rr in result.run_requests] == ["2024-01-16"]


def test_gold_sensor_ignores_a_date_outside_the_partition_range(corpus) -> None:
    """Gold starts at served_start; an older ready date is not a valid key."""
    # 2020-06-01 is inside the Silver preload window (from 2020-01-02) but before
    # the Gold served_start of 2021-01-01, so corpus reports it ready while
    # Dagster has no partition for it.
    _ingest(corpus, "2020-06-01")
    _ingest(corpus, "2024-01-15")
    context = dg.build_sensor_context(resources={"corpus": corpus})

    result = market_history_gold_sensor(context)

    assert [rr.partition_key for rr in result.run_requests] == ["2024-01-15"]


def test_gold_sensor_caps_the_tick_at_the_oldest_dates(
    corpus, monkeypatch: pytest.MonkeyPatch, capfd: pytest.CaptureFixture[str]
) -> None:
    """A backlog drains oldest-first over ticks rather than enqueuing at once."""
    dates = [f"2024-01-{day:02d}" for day in range(5, 5 + MAX_PARTITIONS_PER_TICK + 1)]
    for date in dates:
        _ingest(corpus, date)

    # corpus's real `ready-dates` output order is not part of the contract; feed
    # the sensor a deliberately shuffled report so a passing test proves the
    # sensor itself sorts oldest-first, rather than merely preserving an
    # already-sorted upstream list.
    shuffled = dates[::2] + dates[1::2]
    assert sorted(shuffled) != shuffled
    monkeypatch.setattr(
        "eve_industry_orchestration.defs.corpus_resource.CorpusResource.gold_ready_dates",
        lambda self, *a, **k: {"ready": shuffled},
    )

    first = market_history_gold_sensor(
        dg.build_sensor_context(resources={"corpus": corpus})
    )

    assert [rr.partition_key for rr in first.run_requests] == dates[
        :MAX_PARTITIONS_PER_TICK
    ]
    # The deferred count is logged so a stalled backlog is visible on the tick.
    assert "1 deferred" in capfd.readouterr().err

    # Once the requested ones are built they leave the readiness report and the
    # next tick picks up the remainder.
    for date in dates[:MAX_PARTITIONS_PER_TICK]:
        _build_gold(corpus, date)
    monkeypatch.setattr(
        "eve_industry_orchestration.defs.corpus_resource.CorpusResource.gold_ready_dates",
        lambda self, *a, **k: {"ready": [dates[-1]]},
    )
    second = market_history_gold_sensor(
        dg.build_sensor_context(resources={"corpus": corpus}, cursor=first.cursor)
    )

    assert [rr.partition_key for rr in second.run_requests] == [dates[-1]]


def test_gold_sensor_re_requests_a_still_ready_date_on_a_later_tick(corpus) -> None:
    """A run that never materialised must not be suppressed by dedup.

    Gold has no incomplete-skip path, but a failed or lost run leaves the date
    ready; a static run_key would swallow the retry, so the key rotates per tick.
    """
    _ingest(corpus, "2024-01-15")

    first = market_history_gold_sensor(
        dg.build_sensor_context(resources={"corpus": corpus})
    )
    second = market_history_gold_sensor(
        dg.build_sensor_context(resources={"corpus": corpus}, cursor=first.cursor)
    )

    (first_rr,) = first.run_requests
    (second_rr,) = second.run_requests
    assert first_rr.partition_key == second_rr.partition_key == "2024-01-15"
    assert first_rr.run_key != second_rr.run_key


def test_gold_sensor_skips_an_in_flight_date(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The rotating key must not put a second writer on an in-flight partition."""
    _ingest(corpus, "2024-01-15")
    _ingest(corpus, "2024-01-16")
    monkeypatch.setattr(
        "eve_industry_orchestration.defs.sensor_util._in_flight_partitions",
        lambda context, asset_key: {"2024-01-15"},
    )
    context = dg.build_sensor_context(resources={"corpus": corpus})

    result = market_history_gold_sensor(context)

    assert [rr.partition_key for rr in result.run_requests] == ["2024-01-16"]
