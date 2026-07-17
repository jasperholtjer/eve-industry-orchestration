"""Tests for the monthly-archive MER assets and sensor (corpus ADR-0058)."""

from __future__ import annotations

import dagster as dg
import pytest

from eve_industry_orchestration.defs import mer
from eve_industry_orchestration.defs.mer import (
    mer_killdump_silver,
    mer_money_supply_gold,
    mer_production_destruction_gold,
    mer_silver,
)
from eve_industry_orchestration.defs.sensors import mer_report_discovery_sensor

REPORTS = "2025-06-01,2025-07-01"


def _ingest_month(corpus, dataset: str, month: str) -> None:
    corpus.run(
        dg.build_asset_context(),
        "ingest",
        "--dataset",
        dataset,
        "--month",
        month,
        "--sink-path",
        corpus.sink_path,
    )


def _instance_with_months(*months: str) -> dg.DagsterInstance:
    instance = dg.DagsterInstance.ephemeral()
    instance.add_dynamic_partitions(mer.report_partitions.name, list(months))
    return instance


# --- Silver assets (one atomic partition per report-month) -----------------


def test_mer_silver_materialises_one_partition(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FAKE_MER_REPORTS", REPORTS)
    instance = _instance_with_months("2025-06-01")

    result = dg.materialize(
        [mer_silver],
        partition_key="2025-06-01",
        instance=instance,
        resources={"corpus": corpus},
    )

    assert result.success
    events = result.get_asset_materialization_events()
    assert len(events) == 1
    assert events[0].asset_key.to_user_string() == "mer_silver"
    assert events[0].materialization.metadata["report_month"].value == "2025-06-01"


def test_mer_killdump_silver_materialises_one_partition(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FAKE_MER_REPORTS", REPORTS)
    instance = _instance_with_months("2025-06-01")

    result = dg.materialize(
        [mer_killdump_silver],
        partition_key="2025-06-01",
        instance=instance,
        resources={"corpus": corpus},
    )

    assert result.success
    events = result.get_asset_materialization_events()
    assert len(events) == 1
    assert events[0].asset_key.to_user_string() == "mer_killdump_silver"


# --- kern-series Gold (full merge, non-partitioned; corpus ADR-0058 §5) ----


def test_mer_gold_materialises_from_committed_silver(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FAKE_MER_REPORTS", REPORTS)
    instance = _instance_with_months("2025-06-01", "2025-07-01")
    _ingest_month(corpus, "mer", "2025-06")
    _ingest_month(corpus, "mer", "2025-07")

    result = dg.materialize(
        [mer_money_supply_gold],
        instance=instance,
        resources={"corpus": corpus},
    )

    assert result.success
    events = result.get_asset_materialization_events()
    assert len(events) == 1
    assert events[0].asset_key.to_user_string() == "mer_money_supply"
    assert events[0].materialization.metadata["built"].value is True
    assert events[0].materialization.metadata["concept"].value == "money_supply"


def test_mer_gold_skips_when_no_silver_committed(corpus) -> None:
    # The schedule may fire on a cold corpus: no committed Silver → built False,
    # a clean skip (not a failure).
    instance = dg.DagsterInstance.ephemeral()

    result = dg.materialize(
        [mer_production_destruction_gold],
        instance=instance,
        resources={"corpus": corpus},
    )

    assert result.success
    events = result.get_asset_materialization_events()
    assert len(events) == 1
    assert events[0].materialization.metadata["built"].value is False


# --- report-discovery sensor -----------------------------------------------


def test_report_discovery_sensor_registers_and_requests(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FAKE_MER_REPORTS", REPORTS)
    instance = dg.DagsterInstance.ephemeral()
    context = dg.build_sensor_context(resources={"corpus": corpus}, instance=instance)

    result = mer_report_discovery_sensor(context)

    by_partition = {rr.partition_key: rr for rr in result.run_requests}
    assert sorted(by_partition) == ["2025-06-01", "2025-07-01"]
    assert by_partition["2025-06-01"].run_key == "mer-silver-2025-06-01"
    added = {
        key for req in result.dynamic_partitions_requests for key in req.partition_keys
    }
    assert added == {"2025-06-01", "2025-07-01"}


def test_report_discovery_sensor_skips_known_months(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FAKE_MER_REPORTS", REPORTS)
    instance = _instance_with_months("2025-06-01")
    context = dg.build_sensor_context(resources={"corpus": corpus}, instance=instance)

    result = mer_report_discovery_sensor(context)

    assert [rr.partition_key for rr in result.run_requests] == ["2025-07-01"]


# --- multi-derivative ambiguity (matches the binary) -----------------------


def test_mer_gold_build_without_derivative_fails(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FAKE_MER_REPORTS", REPORTS)
    _ingest_month(corpus, "mer", "2025-06")
    with pytest.raises(dg.Failure):
        corpus.run(
            dg.build_asset_context(),
            "gold",
            "build",
            "--dataset",
            "mer",
            "--sink-path",
            corpus.sink_path,
        )
