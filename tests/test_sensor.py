"""Tests for the market-history, sovereignty and public-contracts sensors."""

from __future__ import annotations

from collections.abc import Callable

import dagster as dg
import pytest

from eve_industry_orchestration.defs import public_contracts as pc
from eve_industry_orchestration.defs import sensors as s
from eve_industry_orchestration.defs.sensor_util import MAX_PARTITIONS_PER_TICK
from eve_industry_orchestration.defs.sensors import (
    market_history_availability_sensor,
    market_history_gold_sensor,
    sovereignty_campaigns_availability_sensor,
    sovereignty_map_availability_sensor,
    sovereignty_structures_availability_sensor,
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


# --- sovereignty availability sensors (corpus ADR-0066) --------------------
#
# One dataset per sink, deliberately: the fake binary's ingested-date state is
# global rather than per dataset, so ingesting a second dataset into the same
# sink would leak its dates into the first's `missing-partitions` report. Each
# test therefore drives exactly one of the three through its own `corpus`
# fixture.

SOVEREIGNTY_SENSORS = [
    (sovereignty_map_availability_sensor, "sovereignty-map"),
    (sovereignty_structures_availability_sensor, "sovereignty-structures"),
    (sovereignty_campaigns_availability_sensor, "sovereignty-campaigns"),
]


def _ingest_dataset(corpus, dataset: str, date: str) -> None:
    corpus.run(
        dg.build_asset_context(),
        "ingest",
        "--dataset",
        dataset,
        "--date",
        date,
        "--sink-path",
        corpus.sink_path,
    )


@pytest.mark.parametrize(("sensor", "dataset"), SOVEREIGNTY_SENSORS)
def test_sovereignty_sensor_requests_missing_partitions(
    sensor: Callable[..., dg.SensorResult],
    dataset: str,
    corpus,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FAKE_EVEREF_DATES", "2024-01-15,2024-01-16")
    context = dg.build_sensor_context(resources={"corpus": corpus})

    result = sensor(context)

    by_partition = {rr.partition_key: rr for rr in result.run_requests}
    assert sorted(by_partition) == ["2024-01-15", "2024-01-16"]
    # The run_key stem is the dataset's own, so two sovereignty sensors ticking
    # on the same date cannot dedup each other away.
    assert by_partition["2024-01-15"].run_key.startswith(
        f"{dataset}-silver-2024-01-15-"
    )


@pytest.mark.parametrize(("sensor", "dataset"), SOVEREIGNTY_SENSORS)
def test_sovereignty_sensor_skips_dates_already_in_the_run_state(
    sensor: Callable[..., dg.SensorResult],
    dataset: str,
    corpus,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Availability is the run-state diff, not a listing of the storage tree."""
    monkeypatch.setenv("FAKE_EVEREF_DATES", "2024-01-15,2024-01-16")
    _ingest_dataset(corpus, dataset, "2024-01-15")
    context = dg.build_sensor_context(resources={"corpus": corpus})

    result = sensor(context)

    assert [rr.partition_key for rr in result.run_requests] == ["2024-01-16"]


@pytest.mark.parametrize(("sensor", "dataset"), SOVEREIGNTY_SENSORS)
def test_sovereignty_sensor_requests_nothing_when_nothing_is_missing(
    sensor: Callable[..., dg.SensorResult],
    dataset: str,
    corpus,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FAKE_EVEREF_DATES", "")
    context = dg.build_sensor_context(resources={"corpus": corpus})

    result = sensor(context)

    assert result.run_requests == []


@pytest.mark.parametrize(("sensor", "dataset"), SOVEREIGNTY_SENSORS)
def test_sovereignty_sensor_caps_the_tick_and_carries_the_backlog(
    sensor: Callable[..., dg.SensorResult],
    dataset: str,
    corpus,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A backlog longer than the cap drains oldest-first over ticks, losing none."""
    dates = [f"2024-01-{day:02d}" for day in range(5, 5 + MAX_PARTITIONS_PER_TICK + 1)]
    monkeypatch.setenv("FAKE_EVEREF_DATES", ",".join(dates))

    first = sensor(dg.build_sensor_context(resources={"corpus": corpus}))

    assert [rr.partition_key for rr in first.run_requests] == dates[
        :MAX_PARTITIONS_PER_TICK
    ]

    # The remainder is not dropped: once the requested dates commit they leave
    # the run-state diff and the next tick picks up what was deferred.
    for date in dates[:MAX_PARTITIONS_PER_TICK]:
        _ingest_dataset(corpus, dataset, date)
    second = sensor(
        dg.build_sensor_context(resources={"corpus": corpus}, cursor=first.cursor)
    )

    assert [rr.partition_key for rr in second.run_requests] == [dates[-1]]


# --- public-contracts Gold readiness (four derivatives, corpus ADR-0068) ---
#
# The four sensors come out of one factory, so what distinguishes them is the
# `--derivative` each polls with, the single asset each targets and the matrix
# each validates against; the behaviour is parametrised over the family. One
# dataset per sink: the fake binary's Silver state is global rather than per
# dataset, so these cases ingest `public-contracts` and nothing else.

PUBLIC_CONTRACTS_GOLD_SENSORS = [
    pytest.param(
        s.contract_facts_gold_sensor,
        "contract-facts",
        pc.contract_facts_gold,
        id="contract-facts",
    ),
    pytest.param(
        s.contract_item_facts_gold_sensor,
        "contract-item-facts",
        pc.contract_item_facts_gold,
        id="contract-item-facts",
    ),
    pytest.param(
        s.contract_item_prices_gold_sensor,
        "contract-item-prices",
        pc.contract_item_prices_gold,
        id="contract-item-prices",
    ),
    pytest.param(
        s.courier_rates_gold_sensor,
        "courier-rates",
        pc.courier_rates_gold,
        id="courier-rates",
    ),
]

_READY_DATES = (
    "eve_industry_orchestration.defs.corpus_resource.CorpusResource.gold_ready_dates"
)


def _build_derivative_gold(corpus, derivative: str, date: str) -> None:
    corpus.run(
        dg.build_asset_context(),
        "gold",
        "build",
        "--dataset",
        "public-contracts",
        "--derivative",
        derivative,
        "--date",
        date,
        "--sink-path",
        corpus.sink_path,
    )


@pytest.mark.parametrize(
    ("sensor", "derivative", "_asset"), PUBLIC_CONTRACTS_GOLD_SENSORS
)
def test_public_contracts_gold_sensor_requests_a_date_whose_silver_is_sealed(
    corpus, sensor: Callable[..., dg.SensorResult], derivative: str, _asset
) -> None:
    """Readiness is the run-state diff, never a listing of the storage tree.

    The day drops out again once that derivative's own Gold is built, which is
    the state-level diff the sensor runs on: the Silver stays on disk, so a
    sensor reading the tree would keep requesting it.
    """
    _ingest_dataset(corpus, "public-contracts", "2024-01-15")
    context = dg.build_sensor_context(resources={"corpus": corpus})

    result = sensor(context)

    by_partition = {rr.partition_key: rr for rr in result.run_requests}
    assert sorted(by_partition) == ["2024-01-15"]
    # Keyed on the derivative tree, not the dataset: four sensors ticking on the
    # same date cannot dedup each other away.
    assert by_partition["2024-01-15"].run_key.startswith(
        f"{derivative}-gold-2024-01-15-"
    )

    _build_derivative_gold(corpus, derivative, "2024-01-15")
    after = sensor(dg.build_sensor_context(resources={"corpus": corpus}))
    assert after.run_requests == []


@pytest.mark.parametrize(
    ("sensor", "_derivative", "_asset"), PUBLIC_CONTRACTS_GOLD_SENSORS
)
def test_public_contracts_gold_sensor_requests_nothing_without_silver(
    corpus, sensor: Callable[..., dg.SensorResult], _derivative, _asset
) -> None:
    """No built Silver partition, so corpus reports no ready date and no run."""
    result = sensor(dg.build_sensor_context(resources={"corpus": corpus}))

    assert result.run_requests == []


@pytest.mark.parametrize(
    ("sensor", "_derivative", "_asset"), PUBLIC_CONTRACTS_GOLD_SENSORS
)
def test_public_contracts_gold_sensor_ignores_a_date_before_its_own_start(
    corpus, sensor: Callable[..., dg.SensorResult], _derivative, _asset
) -> None:
    """Each derivative filters against its own matrix, not the dataset's Silver.

    A date corpus reports ready but that the derivative has no partition key for
    would otherwise be asked of Dagster as a non-existent key.
    """
    _ingest_dataset(corpus, "public-contracts", "2021-06-01")
    _ingest_dataset(corpus, "public-contracts", "2024-01-15")
    context = dg.build_sensor_context(resources={"corpus": corpus})

    result = sensor(context)

    assert [rr.partition_key for rr in result.run_requests] == ["2024-01-15"]


@pytest.mark.parametrize(
    ("sensor", "derivative", "_asset"), PUBLIC_CONTRACTS_GOLD_SENSORS
)
def test_public_contracts_gold_sensor_polls_with_its_own_derivative(
    corpus,
    monkeypatch: pytest.MonkeyPatch,
    sensor: Callable[..., dg.SensorResult],
    derivative: str,
    _asset,
) -> None:
    """One dataset, four trees: the selector is what separates the four polls."""
    polls: list[tuple[str, str | None]] = []

    def _ready(self, dataset: str, *, derivative: str | None = None) -> dict[str, list]:
        polls.append((dataset, derivative))
        return {"ready": []}

    monkeypatch.setattr(_READY_DATES, _ready)

    sensor(dg.build_sensor_context(resources={"corpus": corpus}))

    assert polls == [("public-contracts", derivative)]


@pytest.mark.parametrize(
    ("sensor", "_derivative", "asset"), PUBLIC_CONTRACTS_GOLD_SENSORS
)
def test_public_contracts_gold_sensor_targets_only_its_own_asset(
    sensor: dg.SensorDefinition, _derivative, asset: dg.AssetsDefinition
) -> None:
    """``deps=`` expresses build order; a sensor never fans out over the family."""
    targeted = {
        key
        for target in sensor.targets
        for assets_def in target.assets_defs
        for key in assets_def.keys
    }

    assert targeted == {asset.key}


def test_the_four_public_contracts_gold_sensors_are_named_per_derivative() -> None:
    sensors = [case.values[0] for case in PUBLIC_CONTRACTS_GOLD_SENSORS]

    assert [sensor.name for sensor in sensors] == [
        "contract_facts_gold_sensor",
        "contract_item_facts_gold_sensor",
        "contract_item_prices_gold_sensor",
        "courier_rates_gold_sensor",
    ]
