"""Tests for the system-kills per-measure assets, sensors, and schedules (ADR-0037)."""

from __future__ import annotations

import dagster as dg
import pytest

from eve_industry_orchestration.defs import sensors as s
from eve_industry_orchestration.defs import system_kills as sk

DATASET = "system-kills"

# (history asset, history sensor, recent asset, derivative names) per measure.
_MEASURE_CASES = [
    (
        sk.system_kills_ship_history_gold,
        s.system_kills_ship_history_gold_sensor,
        sk.system_kills_ship_recent_gold,
        "system-kills-ship-history",
    ),
    (
        sk.system_kills_npc_history_gold,
        s.system_kills_npc_history_gold_sensor,
        sk.system_kills_npc_recent_gold,
        "system-kills-npc-history",
    ),
    (
        sk.system_kills_pod_history_gold,
        s.system_kills_pod_history_gold_sensor,
        sk.system_kills_pod_recent_gold,
        "system-kills-pod-history",
    ),
]


def _ingest(corpus, date: str) -> None:
    corpus.run(
        dg.build_asset_context(),
        "ingest",
        "--dataset",
        DATASET,
        "--date",
        date,
        "--sink-path",
        corpus.sink_path,
    )


# --- Silver ingest skip on absent upstream day (ADR-0028) -----------------


def test_silver_skips_absent_upstream_day(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FAKE_EVEREF_DATES", "2024-01-15")

    result = dg.materialize(
        [sk.system_kills_silver],
        partition_key="2024-01-20",
        resources={"corpus": corpus},
    )

    assert result.success
    assert result.get_asset_materialization_events() == []
    observations = result.get_asset_observation_events()
    assert len(observations) == 1
    metadata = observations[0].event_specific_data.asset_observation.metadata
    assert metadata["skip_reason"].value == "upstream_absent"


def test_silver_materialises_present_upstream_day(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FAKE_EVEREF_DATES", "2024-01-15")

    result = dg.materialize(
        [sk.system_kills_silver],
        partition_key="2024-01-15",
        resources={"corpus": corpus},
    )

    assert result.success
    assert len(result.get_asset_materialization_events()) == 1


# --- Silver availability sensor -------------------------------------------


def test_silver_sensor_requests_newly_available_dates(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FAKE_EVEREF_DATES", "2024-01-15,2024-01-16")
    context = dg.build_sensor_context(resources={"corpus": corpus})

    result = s.system_kills_availability_sensor(context)

    by_partition = {rr.partition_key: rr for rr in result.run_requests}
    assert sorted(by_partition) == ["2024-01-15", "2024-01-16"]
    assert by_partition["2024-01-15"].run_key == "system-kills-silver-2024-01-15"


# --- history Gold readiness sensors (one per measure) ---------------------


@pytest.mark.parametrize(("_asset", "sensor", "_recent", "derivative"), _MEASURE_CASES)
def test_history_gold_sensor_requests_ready_dates(
    corpus, _asset, sensor, _recent, derivative
) -> None:
    _ingest(corpus, "2024-01-15")
    context = dg.build_sensor_context(resources={"corpus": corpus})

    result = sensor(context)

    by_partition = {rr.partition_key: rr for rr in result.run_requests}
    assert sorted(by_partition) == ["2024-01-15"]
    assert by_partition["2024-01-15"].run_key == f"{derivative}-gold-2024-01-15"


@pytest.mark.parametrize(("asset", "_sensor", "_recent", "_derivative"), _MEASURE_CASES)
def test_history_gold_skips_upstream_gap_day(
    corpus, monkeypatch: pytest.MonkeyPatch, asset, _sensor, _recent, _derivative
) -> None:
    monkeypatch.setenv("FAKE_EVEREF_DATES", "2024-01-15")
    _ingest(corpus, "2024-01-20")  # records the upstream gap

    result = dg.materialize(
        [asset],
        partition_key="2024-01-20",
        resources={"corpus": corpus},
    )

    assert result.success
    assert result.get_asset_materialization_events() == []
    observations = result.get_asset_observation_events()
    assert len(observations) == 1
    metadata = observations[0].event_specific_data.asset_observation.metadata
    assert metadata["skip_reason"].value == "upstream_gap"


# --- kills-recent "danger-now" assets (one per measure) -------------------


@pytest.mark.parametrize(("_asset", "_sensor", "recent", "_derivative"), _MEASURE_CASES)
def test_recent_asset_builds_latest_ready_date(
    corpus, _asset, _sensor, recent, _derivative
) -> None:
    _ingest(corpus, "2024-01-15")
    _ingest(corpus, "2024-01-16")
    context = dg.build_asset_context()

    result = recent(context, corpus)

    assert result.metadata["built"] is True
    assert result.metadata["partition"] == "2024-01-16"


@pytest.mark.parametrize(("_asset", "_sensor", "recent", "_derivative"), _MEASURE_CASES)
def test_recent_asset_noop_without_ready(
    corpus, _asset, _sensor, recent, _derivative
) -> None:
    context = dg.build_asset_context()

    result = recent(context, corpus)

    assert result.metadata["built"] is False


# --- multi-derivative ambiguity (matches the binary's error) --------------


def test_gold_build_without_derivative_fails(corpus) -> None:
    _ingest(corpus, "2024-01-15")
    context = dg.build_asset_context()
    with pytest.raises(dg.Failure):
        corpus.run(
            context,
            "gold",
            "build",
            "--dataset",
            DATASET,
            "--date",
            "2024-01-15",
            "--sink-path",
            corpus.sink_path,
        )
