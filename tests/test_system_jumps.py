"""Tests for the system-jumps multi-derivative assets, sensors, and schedule."""

from __future__ import annotations

import dagster as dg
import pytest

from eve_industry_orchestration.defs.sensors import (
    system_jumps_availability_sensor,
    system_jumps_history_gold_sensor,
)
from eve_industry_orchestration.defs.system_jumps import (
    system_jumps_history_gold,
    system_jumps_recent_gold,
    system_jumps_silver,
)

DATASET = "system-jumps"
HISTORY = "system-traffic-history"


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


def _run_state_facts(metadata) -> dict:
    """The run-state columns `partition_metadata` merged in, unwrapped.

    A materialisation event wraps its metadata in `MetadataValue`s while a
    directly-called asset returns the raw dict; one accessor reads both.
    """
    return {
        key: getattr(value, "value", value)
        for key, value in metadata.items()
        if key in ("rows", "retention_class", "parquet_sha256")
    }


def _assert_enriched(metadata) -> None:
    """Asserts the run-state facts for the partition corpus just wrote are there.

    Keyed on the run-state key, not the bare Dagster partition key: a mismatched
    key matches no row and enriches nothing, silently, so this asserts presence.
    """
    facts = _run_state_facts(metadata)
    assert facts["rows"] == 1
    assert facts["retention_class"] == "validated"
    assert facts["parquet_sha256"]


# --- Silver ingest skip on absent upstream day (ADR-0028) -----------------


def test_silver_skips_absent_upstream_day(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Upstream has 2024-01-15 only; 2024-01-20 is a gap → corpus reports
    # "skipped", so the partition is left Missing (no materialisation) with an
    # observation recording why — not a failure, not an empty materialisation.
    monkeypatch.setenv("FAKE_EVEREF_DATES", "2024-01-15")

    result = dg.materialize(
        [system_jumps_silver],
        partition_key="2024-01-20",
        resources={"corpus": corpus},
    )

    assert result.success
    # No materialisation event → the partition is left Missing, not materialised.
    assert result.get_asset_materialization_events() == []
    # An observation records the skip reason instead.
    observations = result.get_asset_observation_events()
    assert len(observations) == 1
    metadata = observations[0].event_specific_data.asset_observation.metadata
    assert metadata["skip_reason"].value == "upstream_absent"


def test_silver_materialises_present_upstream_day(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FAKE_EVEREF_DATES", "2024-01-15")

    result = dg.materialize(
        [system_jumps_silver],
        partition_key="2024-01-15",
        resources={"corpus": corpus},
    )

    assert result.success
    (materialisation,) = result.get_asset_materialization_events()
    metadata = materialisation.materialization.metadata
    assert metadata["dataset"].value == DATASET
    assert metadata["partition"].value == "2024-01-15"
    _assert_enriched(metadata)


# --- Silver availability sensor -------------------------------------------


def test_silver_sensor_requests_newly_available_dates(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FAKE_EVEREF_DATES", "2024-01-15,2024-01-16")
    context = dg.build_sensor_context(resources={"corpus": corpus})

    result = system_jumps_availability_sensor(context)

    by_partition = {rr.partition_key: rr for rr in result.run_requests}
    assert sorted(by_partition) == ["2024-01-15", "2024-01-16"]
    assert by_partition["2024-01-15"].run_key.startswith(
        "system-jumps-silver-2024-01-15-"
    )


# --- history Gold readiness sensor ----------------------------------------


def test_history_gold_sensor_requests_ready_dates(corpus) -> None:
    _ingest(corpus, "2024-01-15")
    context = dg.build_sensor_context(resources={"corpus": corpus})

    result = system_jumps_history_gold_sensor(context)

    by_partition = {rr.partition_key: rr for rr in result.run_requests}
    assert sorted(by_partition) == ["2024-01-15"]
    # run_key is keyed on the derivative (its own Gold tree), not the dataset.
    assert by_partition["2024-01-15"].run_key.startswith(
        "system-traffic-history-gold-2024-01-15-"
    )


def test_history_gold_materialises_ready_day(corpus) -> None:
    _ingest(corpus, "2024-01-15")

    result = dg.materialize(
        [system_jumps_history_gold],
        partition_key="2024-01-15",
        resources={"corpus": corpus},
    )

    assert result.success
    (materialisation,) = result.get_asset_materialization_events()
    metadata = materialisation.materialization.metadata
    assert metadata["derivative"].value == HISTORY
    # Gold run-state is keyed on the derivative tree, not the dataset.
    _assert_enriched(metadata)


def test_history_gold_skips_upstream_gap_day(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    # 2024-01-20 is a gap: a Silver ingest records it skipped, so the Gold build
    # for that target day skips too — partition left Missing with an observation,
    # not failed (ADR-0029).
    monkeypatch.setenv("FAKE_EVEREF_DATES", "2024-01-15")
    _ingest(corpus, "2024-01-20")  # records the upstream gap

    result = dg.materialize(
        [system_jumps_history_gold],
        partition_key="2024-01-20",
        resources={"corpus": corpus},
    )

    assert result.success
    assert result.get_asset_materialization_events() == []
    observations = result.get_asset_observation_events()
    assert len(observations) == 1
    metadata = observations[0].event_specific_data.asset_observation.metadata
    assert metadata["skip_reason"].value == "upstream_gap"


def test_history_gold_sensor_no_silver_yields_no_requests(corpus) -> None:
    context = dg.build_sensor_context(resources={"corpus": corpus})

    result = system_jumps_history_gold_sensor(context)

    assert result.run_requests == []


# --- recency-weighted "recent" asset --------------------------------------


def test_recent_asset_builds_latest_ready_date(corpus) -> None:
    _ingest(corpus, "2024-01-15")
    _ingest(corpus, "2024-01-16")
    context = dg.build_asset_context()

    result = system_jumps_recent_gold(context, corpus)

    assert result.metadata["built"] is True
    assert result.metadata["partition"] == "2024-01-16"
    # `latest` is a date, so the run-state key is `date=2024-01-16`, not `latest`.
    _assert_enriched(result.metadata)


def test_recent_asset_noop_without_ready(corpus) -> None:
    context = dg.build_asset_context()

    result = system_jumps_recent_gold(context, corpus)

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
