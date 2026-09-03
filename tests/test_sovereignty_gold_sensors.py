"""Tests for the five sovereignty Gold readiness sensors (corpus ADR-0066).

All five come out of one factory, so most assertions are parametrised over the
family: what distinguishes the sensors is the pair they poll with (dataset plus
``--derivative``), the single asset each targets, and the partition matrix each
validates against. The panel is in the same parametrisation on purpose — it is
built by the same factory and differs only in its start date and in what the
binary considers ready.

One dataset per sink: the fake binary's Silver state is global rather than per
dataset, so a test that ingested two datasets into one sink would see the
second's dates in the first's report. The ``corpus`` fixture is function-scoped,
so each parametrised case owns a fresh sink and touches one dataset only.
"""

from __future__ import annotations

from typing import Any

import dagster as dg
import pytest

from eve_industry_orchestration.defs import sensors as s
from eve_industry_orchestration.defs import sovereignty_campaigns as sc
from eve_industry_orchestration.defs import sovereignty_map as sm
from eve_industry_orchestration.defs import sovereignty_structures as ss
from eve_industry_orchestration.defs.corpus_resource import CorpusResource
from eve_industry_orchestration.defs.sensor_util import MAX_PARTITIONS_PER_TICK
from tests.fake_corpus import seed_flip_window

DATE = "2024-01-15"

# (sensor, dataset, derivative, target asset, partitions def) per derivative.
SENSOR_CASES = [
    pytest.param(
        s.sovereignty_ownership_gold_sensor,
        "sovereignty-map",
        "sovereignty-ownership",
        sm.sovereignty_ownership_gold,
        sm.ownership_gold_partitions,
        id="ownership",
    ),
    pytest.param(
        s.sovereignty_changes_gold_sensor,
        "sovereignty-map",
        "sovereignty-changes",
        sm.sovereignty_changes_gold,
        sm.changes_gold_partitions,
        id="changes",
    ),
    pytest.param(
        s.sovereignty_adm_gold_sensor,
        "sovereignty-structures",
        "sovereignty-adm",
        ss.sovereignty_adm_gold,
        ss.adm_gold_partitions,
        id="adm",
    ),
    pytest.param(
        s.sovereignty_contests_gold_sensor,
        "sovereignty-campaigns",
        "sovereignty-contests",
        sc.sovereignty_contests_gold,
        sc.contests_gold_partitions,
        id="contests",
    ),
    pytest.param(
        s.sovereignty_panel_gold_sensor,
        "sovereignty-map",
        "sovereignty-panel",
        sm.sovereignty_panel_gold,
        sm.panel_gold_partitions,
        id="panel",
    ),
]

_READY_DATES = (
    "eve_industry_orchestration.defs.corpus_resource.CorpusResource.gold_ready_dates"
)


def _ingest(corpus, dataset: str, date: str) -> None:
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


def _gold_build(corpus, dataset: str, derivative: str, date: str) -> None:
    corpus.run(
        dg.build_asset_context(),
        "gold",
        "build",
        "--dataset",
        dataset,
        "--derivative",
        derivative,
        "--date",
        date,
        "--sink-path",
        corpus.sink_path,
    )


# The panel's same-day Gold prerequisites: corpus reports a panel date ready
# only once these three trees hold that day (corpus ADR-0066 decision 8). They
# span three datasets, but only `sovereignty-map` is ever ingested per sink —
# the fake's Silver state is by date, so a sibling Gold build over the same day
# needs no second ingest.
PANEL_PREREQUISITES = [
    ("sovereignty-map", "sovereignty-ownership"),
    ("sovereignty-structures", "sovereignty-adm"),
    ("sovereignty-campaigns", "sovereignty-contests"),
]


def _build_panel_prerequisites(corpus, date: str) -> None:
    for dataset, derivative in PANEL_PREREQUISITES:
        _gold_build(corpus, dataset, derivative, date)


def _make_panel_ready(corpus, date: str) -> None:
    """Clears both of the panel's Gold-over-Gold gates for ``date``."""
    _build_panel_prerequisites(corpus, date)
    seed_flip_window(corpus.sink_path, date)


def _target_keys(sensor: dg.SensorDefinition) -> set[dg.AssetKey]:
    return {
        key
        for target in sensor.targets
        for assets_def in target.assets_defs
        for key in assets_def.keys
    }


def _record_polls(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, str | None]]:
    """Records the (dataset, derivative) pair each ``ready-dates`` poll asks for."""
    polls: list[tuple[str, str | None]] = []

    def _ready(
        self: CorpusResource, dataset: str, *, derivative: str | None = None
    ) -> dict[str, Any]:
        polls.append((dataset, derivative))
        return {"ready": []}

    monkeypatch.setattr(_READY_DATES, _ready)
    return polls


# --- the factory requests what corpus reports ready ------------------------


@pytest.mark.parametrize(
    ("sensor", "dataset", "derivative", "_asset", "_partitions"), SENSOR_CASES
)
def test_gold_sensor_requests_the_dates_corpus_reports_ready(
    corpus, sensor, dataset: str, derivative: str, _asset, _partitions
) -> None:
    """Readiness is corpus run-state, never a listing of the NAS tree.

    The day drops out again once that derivative's Gold is built, which is the
    state-level diff the sensor runs on. The panel additionally waits on its
    three same-day sibling trees, so this case builds them first; the direction
    that gating runs in is asserted on its own below.
    """
    _ingest(corpus, dataset, DATE)
    if derivative == "sovereignty-panel":
        _make_panel_ready(corpus, DATE)
    context = dg.build_sensor_context(resources={"corpus": corpus})

    result = sensor(context)

    by_partition = {rr.partition_key: rr for rr in result.run_requests}
    assert sorted(by_partition) == [DATE]
    # The run_key is keyed on the derivative tree (not the dataset) and carries a
    # rotating per-tick token, so a still-ready date can be re-requested.
    assert by_partition[DATE].run_key.startswith(f"{derivative}-gold-{DATE}-")

    _gold_build(corpus, dataset, derivative, DATE)
    after = sensor(dg.build_sensor_context(resources={"corpus": corpus}))
    assert after.run_requests == []


@pytest.mark.parametrize(
    ("sensor", "_dataset", "_derivative", "_asset", "_partitions"), SENSOR_CASES
)
def test_gold_sensor_requests_nothing_when_no_date_is_ready(
    corpus, sensor, _dataset, _derivative, _asset, _partitions
) -> None:
    """Nothing ingested, so corpus reports no ready date and no run is queued."""
    result = sensor(dg.build_sensor_context(resources={"corpus": corpus}))

    assert result.run_requests == []


# --- each sensor is bound to its own derivative and its own asset ----------


@pytest.mark.parametrize(
    ("sensor", "dataset", "derivative", "_asset", "_partitions"), SENSOR_CASES
)
def test_gold_sensor_polls_with_its_own_dataset_and_derivative(
    corpus,
    monkeypatch: pytest.MonkeyPatch,
    sensor,
    dataset,
    derivative,
    _asset,
    _partitions,
) -> None:
    """The factory takes the dataset too: the family spans three of them."""
    polls = _record_polls(monkeypatch)

    sensor(dg.build_sensor_context(resources={"corpus": corpus}))

    assert polls == [(dataset, derivative)]


@pytest.mark.parametrize(
    ("sensor", "_dataset", "_derivative", "asset", "_partitions"), SENSOR_CASES
)
def test_gold_sensor_targets_only_its_own_asset(
    sensor, _dataset, _derivative, asset, _partitions
) -> None:
    """``deps=`` expresses build order; a sensor never fans out over the family."""
    assert _target_keys(sensor) == {asset.key}


def _panel_tick(corpus) -> dg.SensorResult:
    return s.sovereignty_panel_gold_sensor(
        dg.build_sensor_context(resources={"corpus": corpus})
    )


def _panel_requests(corpus) -> list[str]:
    return [rr.partition_key for rr in _panel_tick(corpus).run_requests]


def test_panel_readiness_waits_for_its_three_siblings_and_a_settled_flip_window(
    corpus,
) -> None:
    """The panel's two distinguishing gates, in every direction that matters.

    Ingested Silver alone does not make a panel date ready: corpus gates it on
    the same day's ownership, ADM and contests Gold, and *also* on the trailing
    30-day `sovereignty-changes` window (corpus ADR-0066 §8). Building the three
    siblings is therefore not enough while that window is unsettled — a panel
    day is never sealed with NULL flip counts because its changes tree lagged.
    """
    _ingest(corpus, "sovereignty-map", DATE)
    assert _panel_requests(corpus) == []

    _gold_build(corpus, "sovereignty-map", "sovereignty-changes", DATE)
    assert _panel_requests(corpus) == []

    _build_panel_prerequisites(corpus, DATE)
    assert _panel_requests(corpus) == []

    seed_flip_window(corpus.sink_path, DATE)
    assert _panel_requests(corpus) == [DATE]


def test_a_flip_window_of_recorded_gaps_settles_the_panel_gate(corpus) -> None:
    """The disjunct that keeps a permanent upstream outage from stalling forever.

    A day in the window is settled when it is built in `sovereignty-changes`
    Gold *or* is a recorded gap on `sovereignty-map` Silver (ADR-0028). Without
    the second half, EVE Ref's two multi-day outages would block every panel
    date whose window touches them, for good.
    """
    _ingest(corpus, "sovereignty-map", DATE)
    _build_panel_prerequisites(corpus, DATE)
    assert _panel_requests(corpus) == []

    seed_flip_window(corpus.sink_path, DATE, as_gaps=True)
    assert _panel_requests(corpus) == [DATE]


def test_a_panel_tick_that_requests_nothing_reports_what_blocked_it(corpus) -> None:
    """The gate made a wrong-data failure a no-data one; this is what says so.

    The binary decides readiness and already publishes `blocked[]`; the sensor
    only records what it said, so a lagging changes tree is visible on the tick
    instead of silent.
    """
    _ingest(corpus, "sovereignty-map", DATE)
    _build_panel_prerequisites(corpus, DATE)

    result = _panel_tick(corpus)

    assert result.run_requests == []
    assert result.skip_reason is not None
    assert f"1 blocked, earliest {DATE} on window" in result.skip_reason.skip_message


def test_a_tick_with_nothing_blocked_carries_no_skip_reason(corpus) -> None:
    """Nothing ingested is not a block: no candidate date was held back."""
    result = _panel_tick(corpus)

    assert result.run_requests == []
    assert result.skip_reason is None


def test_a_ready_date_outside_this_tick_is_not_reported_as_blocked(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The skip is keyed on the report, not on whether a run was requested.

    A ready date the in-flight guard or the partition matrix holds back leaves
    the tick empty while nothing is actually blocked; reporting the unrelated
    blocked entry would send an operator after a tree that is fine.
    """

    def _ready(self: CorpusResource, dataset: str, *, derivative: str | None = None):
        return {
            "ready": ["2019-01-01"],  # before the panel's served start
            "blocked": [{"date": DATE, "block": "window"}],
        }

    monkeypatch.setattr(_READY_DATES, _ready)

    result = _panel_tick(corpus)

    assert result.run_requests == []
    assert result.skip_reason is None


def test_the_five_sensors_are_distinct_and_named_per_derivative() -> None:
    sensors = [case.values[0] for case in SENSOR_CASES]
    assert [sensor.name for sensor in sensors] == [
        "sovereignty_ownership_gold_sensor",
        "sovereignty_changes_gold_sensor",
        "sovereignty_adm_gold_sensor",
        "sovereignty_contests_gold_sensor",
        "sovereignty_panel_gold_sensor",
    ]


# --- the shared cap and partition-range filter still hold ------------------


def test_gold_sensor_caps_the_tick_and_carries_the_backlog(
    corpus, monkeypatch: pytest.MonkeyPatch, capfd: pytest.CaptureFixture[str]
) -> None:
    """A backlog drains oldest-first over ticks rather than enqueuing at once."""
    dates = [f"2024-01-{day:02d}" for day in range(5, 5 + MAX_PARTITIONS_PER_TICK + 1)]
    # corpus's `ready-dates` output order is not part of the contract, so feed a
    # deliberately shuffled report: a passing test then proves the sensor sorts
    # oldest-first rather than preserving an already-sorted upstream list.
    shuffled = dates[::2] + dates[1::2]
    assert sorted(shuffled) != shuffled
    monkeypatch.setattr(_READY_DATES, lambda self, *a, **k: {"ready": shuffled})

    first = s.sovereignty_ownership_gold_sensor(
        dg.build_sensor_context(resources={"corpus": corpus})
    )

    assert [rr.partition_key for rr in first.run_requests] == (
        dates[:MAX_PARTITIONS_PER_TICK]
    )
    # The deferred count is logged so a stalled backlog is visible on the tick.
    assert "1 deferred" in capfd.readouterr().err

    # The remainder is carried: it is still ready on the next tick and the
    # rotating cursor token makes it a fresh, non-deduped request.
    monkeypatch.setattr(_READY_DATES, lambda self, *a, **k: {"ready": [dates[-1]]})
    second = s.sovereignty_ownership_gold_sensor(
        dg.build_sensor_context(resources={"corpus": corpus}, cursor=first.cursor)
    )

    assert [rr.partition_key for rr in second.run_requests] == [dates[-1]]


def test_gold_sensor_ignores_a_ready_date_before_the_derivative_start(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Each derivative validates against its own matrix, and they differ.

    The panel serves one flip window after the tenure pair (2022-01-31 against
    2022-01-01), so a date corpus reports ready for a sibling is not necessarily
    a partition key the panel has. Filtering against the wrong matrix would ask
    Dagster for a key that does not exist.
    """
    assert sm.ownership_gold_partitions.start < sm.panel_gold_partitions.start
    early, late = "2022-01-15", "2024-01-15"
    monkeypatch.setattr(_READY_DATES, lambda self, *a, **k: {"ready": [early, late]})
    context = dg.build_sensor_context(resources={"corpus": corpus})

    panel = s.sovereignty_panel_gold_sensor(context)
    ownership = s.sovereignty_ownership_gold_sensor(
        dg.build_sensor_context(resources={"corpus": corpus})
    )

    assert [rr.partition_key for rr in panel.run_requests] == [late]
    assert [rr.partition_key for rr in ownership.run_requests] == [early, late]
