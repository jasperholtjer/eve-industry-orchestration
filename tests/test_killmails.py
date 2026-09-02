"""Tests for the killmails assets and sensors (corpus ADR-0059/0060/0061).

The forward path (availability → Silver → readiness → Gold) mirrors every other
dataset. What is unique here, and what most of this file covers, is the
**mutable-partition** repair loop: killmail days grow upstream after first
archival, so a day already materialised must be able to become actionable again —
which the two normal signals cannot express.
"""

from __future__ import annotations

import dagster as dg
import pytest

from eve_industry_orchestration.defs import killmails as km
from eve_industry_orchestration.defs import sensors as s

DATASET = "killmails"
DERIVATIVE = "killmails-consumption"


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


def _build_gold(corpus, date: str) -> None:
    corpus.run(
        dg.build_asset_context(),
        "gold",
        "build",
        "--dataset",
        DATASET,
        "--derivative",
        DERIVATIVE,
        "--date",
        date,
        "--sink-path",
        corpus.sink_path,
    )


# --- partition matrices come from the corpus config -----------------------


def test_partition_starts_come_from_the_dataset_yaml() -> None:
    # Gold's served_start, and Silver clamped to the silver.served_start floor
    # (= Gold minus the 365-day look-back). Neither is hardcoded in Python.
    assert km.gold_partitions.start.strftime("%Y-%m-%d") == "2022-01-01"
    assert km.silver_partitions.start.strftime("%Y-%m-%d") == "2021-01-01"


# --- Silver ---------------------------------------------------------------


def test_silver_skips_absent_upstream_day(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FAKE_EVEREF_DATES", "2024-03-07")

    result = dg.materialize(
        [km.killmails_silver],
        partition_key="2024-03-08",
        resources={"corpus": corpus},
    )

    assert result.success
    assert result.get_asset_materialization_events() == []
    observations = result.get_asset_observation_events()
    assert len(observations) == 1
    metadata = observations[0].event_specific_data.asset_observation.metadata
    assert metadata["skip_reason"].value == "upstream_absent"


def test_silver_leaves_an_incomplete_upstream_day_missing(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A day still mid-publication is retryable, not a permanent gap.

    Reachable for killmails via the ``daily-tar-of-json`` arm of
    ``classify_absent_date`` (corpus ADR-0028, extended 2026-09-01):
    ``IndexVerdict::NotYetPublished`` → ``finalize_incomplete``. Without the
    branch the run would fall through to ``corpus verify`` and go red on a
    partition that was deliberately never written — once per sensor tick.
    """
    monkeypatch.setenv("FAKE_INCOMPLETE_DATES", "2024-03-07")

    result = dg.materialize(
        [km.killmails_silver],
        partition_key="2024-03-07",
        resources={"corpus": corpus},
    )

    assert result.success
    assert result.get_asset_materialization_events() == []
    (observation,) = result.get_asset_observation_events()
    metadata = observation.event_specific_data.asset_observation.metadata
    assert metadata["skip_reason"].value == "upstream_incomplete"
    # Told apart from the permanent absence: the remedies differ.
    assert metadata["skip_reason"].value != "upstream_absent"


def test_silver_materialises_present_upstream_day(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FAKE_EVEREF_DATES", "2024-03-07")

    result = dg.materialize(
        [km.killmails_silver],
        partition_key="2024-03-07",
        resources={"corpus": corpus},
    )

    assert result.success
    (materialization,) = result.get_asset_materialization_events()
    metadata = materialization.materialization.metadata
    # Identifying fields survive the merge, and the run-state facts corpus
    # recorded for the partition it just wrote sit alongside them.
    assert metadata["dataset"].value == DATASET
    assert metadata["partition"].value == "2024-03-07"
    assert metadata["rows"].value == 1
    assert metadata["retention_class"].value == "validated"
    assert metadata["parquet_sha256"].value


def test_silver_metadata_enrichment_is_advisory(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A broken run-state read must not fail a materialisation corpus completed."""
    monkeypatch.setenv("FAKE_STATE_QUERY_FAIL", "1")

    result = dg.materialize(
        [km.killmails_silver],
        partition_key="2024-03-07",
        resources={"corpus": corpus},
    )

    assert result.success
    (materialization,) = result.get_asset_materialization_events()
    metadata = materialization.materialization.metadata
    assert metadata["partition"].value == "2024-03-07"
    assert "rows" not in metadata


def test_silver_sensor_requests_newly_available_dates(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FAKE_EVEREF_DATES", "2024-03-07,2024-03-08")
    context = dg.build_sensor_context(resources={"corpus": corpus})

    result = s.killmails_availability_sensor(context)

    by_partition = {rr.partition_key: rr for rr in result.run_requests}
    assert sorted(by_partition) == ["2024-03-07", "2024-03-08"]
    assert by_partition["2024-03-07"].run_key.startswith("killmails-silver-2024-03-07-")


# --- Gold -----------------------------------------------------------------


def test_gold_sensor_requests_ready_dates(corpus) -> None:
    _ingest(corpus, "2024-03-07")
    context = dg.build_sensor_context(resources={"corpus": corpus})

    result = s.killmails_consumption_gold_sensor(context)

    by_partition = {rr.partition_key: rr for rr in result.run_requests}
    assert sorted(by_partition) == ["2024-03-07"]
    assert by_partition["2024-03-07"].run_key.startswith(
        "killmails-consumption-gold-2024-03-07-"
    )


def test_gold_materialises_ready_day(corpus) -> None:
    _ingest(corpus, "2024-03-07")

    result = dg.materialize(
        [km.killmails_consumption_gold],
        partition_key="2024-03-07",
        selection=[km.killmails_consumption_gold],
        resources={"corpus": corpus},
    )

    assert result.success
    (materialization,) = result.get_asset_materialization_events()
    metadata = materialization.materialization.metadata
    assert metadata["derivative"].value == DERIVATIVE
    # `corpus gold build` records the run-state row under the derivative name;
    # keying the read on "killmails" would have matched no row.
    assert metadata["rows"].value == 1
    assert metadata["retention_class"].value == "validated"
    assert metadata["parquet_sha256"].value


def test_gold_skips_upstream_gap_day(corpus, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FAKE_EVEREF_DATES", "2024-03-07")
    _ingest(corpus, "2024-03-08")  # records the upstream gap

    result = dg.materialize(
        [km.killmails_consumption_gold],
        partition_key="2024-03-08",
        selection=[km.killmails_consumption_gold],
        resources={"corpus": corpus},
    )

    assert result.success
    assert result.get_asset_materialization_events() == []
    observations = result.get_asset_observation_events()
    assert len(observations) == 1
    metadata = observations[0].event_specific_data.asset_observation.metadata
    assert metadata["skip_reason"].value == "upstream_gap"


def test_gold_depends_on_both_cross_dataset_upstreams() -> None:
    """The SDE region map and the market-history price are real inputs.

    The builder reads both and folds them into ``dependency_fingerprint``; a
    missing one fails the run in the binary. Declaring them keeps the lineage
    honest rather than implying Gold needs only its own Silver.
    """
    from eve_industry_orchestration.defs import market_history, sde

    asset = km.killmails_consumption_gold
    dep_keys = {dep.asset_key for dep in asset.op.ins.values()} | set(
        asset.asset_deps[asset.key]
    )
    assert km.killmails_silver.key in dep_keys
    assert sde.sde_snapshot_gold.key in dep_keys
    assert market_history.market_history_gold.key in dep_keys


# --- mutable partitions: the drift repair loop (ADR-0060) -----------------


def test_freshness_sensor_ignores_a_day_matching_upstream(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    _ingest(corpus, "2024-03-07")
    # Upstream reports exactly what the ingest materialised → no drift.
    monkeypatch.setenv("FAKE_KILLMAILS_TOTALS", "2024-03-07:1")
    context = dg.build_sensor_context(resources={"corpus": corpus})

    result = s.killmails_freshness_sensor(context)

    assert result.run_requests == []


def test_freshness_sensor_re_proposes_a_grown_day(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A day already materialised must become actionable again when it grows.

    This is the whole point of ADR-0060: ``everef missing-partitions`` reports
    nothing (the partition exists), so without this sensor the late-discovered
    kills would never enter the corpus.
    """
    _ingest(corpus, "2024-03-07")
    monkeypatch.setenv("FAKE_KILLMAILS_TOTALS", "2024-03-07:99")
    context = dg.build_sensor_context(resources={"corpus": corpus})

    result = s.killmails_freshness_sensor(context)

    by_partition = {rr.partition_key: rr for rr in result.run_requests}
    assert sorted(by_partition) == ["2024-03-07"]
    # A distinct run_key stem from the availability sensor, so the two cannot
    # dedup against each other despite targeting the same asset.
    assert by_partition["2024-03-07"].run_key.startswith(
        "killmails-freshness-2024-03-07-"
    )


def test_freshness_sensor_ignores_a_day_outside_the_served_matrix(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    _ingest(corpus, "2020-06-01")  # before silver.served_start
    monkeypatch.setenv("FAKE_KILLMAILS_TOTALS", "2020-06-01:99")
    context = dg.build_sensor_context(resources={"corpus": corpus})

    result = s.killmails_freshness_sensor(context)

    assert result.run_requests == []


def test_gold_repair_sensor_is_quiet_on_the_normal_forward_path(corpus) -> None:
    _ingest(corpus, "2024-03-07")
    _build_gold(corpus, "2024-03-07")
    context = dg.build_sensor_context(resources={"corpus": corpus})

    result = s.killmails_consumption_gold_repair_sensor(context)

    assert result.run_requests == []


def test_gold_repair_sensor_rebuilds_gold_whose_silver_moved(corpus) -> None:
    """After a repair ingest, the day's Gold predates its own Silver.

    ``gold ready-dates`` cannot see this — the Gold partition exists, so it is
    never reported ready — which is exactly the gap this sensor closes.
    """
    _ingest(corpus, "2024-03-07")
    _build_gold(corpus, "2024-03-07")
    _ingest(corpus, "2024-03-07")  # the drift repair
    context = dg.build_sensor_context(resources={"corpus": corpus})

    result = s.killmails_consumption_gold_repair_sensor(context)

    by_partition = {rr.partition_key: rr for rr in result.run_requests}
    assert sorted(by_partition) == ["2024-03-07"]
    assert by_partition["2024-03-07"].run_key.startswith(
        "killmails-consumption-repair-2024-03-07-"
    )


def test_gold_repair_sensor_ignores_a_day_with_no_gold_yet(corpus) -> None:
    """A never-built day belongs to the readiness sensor, not the repair one."""
    _ingest(corpus, "2024-03-07")
    context = dg.build_sensor_context(resources={"corpus": corpus})

    result = s.killmails_consumption_gold_repair_sensor(context)

    assert result.run_requests == []


def test_stale_gold_dates_is_scoped_to_the_derivative(corpus) -> None:
    _ingest(corpus, "2024-03-07")
    _build_gold(corpus, "2024-03-07")
    _ingest(corpus, "2024-03-07")

    assert corpus.stale_gold_dates(DATASET, DERIVATIVE) == ["2024-03-07"]
    # Another dataset's Gold tree shares neither the diff nor the repair.
    assert corpus.stale_gold_dates(DATASET, "system-kills-ship-history") == []
