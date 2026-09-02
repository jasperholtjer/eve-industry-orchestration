"""Tests for the public-contracts history Silver asset and its sensor (ADR-0068).

The history tier is a Silver-only dataset: its YAML declares no ``gold:`` block,
so its partition start comes from the ``silver.served_start`` coverage floor and
there is no Gold asset or readiness sensor to exercise. The live twin
``public-contracts-live`` is a separate dataset and is asserted untouched here
rather than assumed so.
"""

from __future__ import annotations

from pathlib import Path

import dagster as dg
import pytest
import yaml

from eve_industry_orchestration.defs.corpus_resource import CorpusResource
from eve_industry_orchestration.defs.public_contracts import (
    DATASET,
    public_contracts_silver,
    silver_partitions,
)
from eve_industry_orchestration.defs.public_contracts_live import (
    public_contracts_live_gold,
)
from eve_industry_orchestration.defs.sensor_util import MAX_PARTITIONS_PER_TICK
from eve_industry_orchestration.defs.sensors import public_contracts_availability_sensor
from tests.conftest import DATASETS_DIR, _assert_enriched, _run_state_facts

DATE = "2024-01-15"


def _record_runs(
    monkeypatch: pytest.MonkeyPatch, *, fail_on: str | None = None
) -> list[tuple[str, ...]]:
    """Records every ``corpus`` subcommand the asset invokes, in order."""
    calls: list[tuple[str, ...]] = []
    original = CorpusResource.run

    def _run(self, context, *args: str):  # type: ignore[no-untyped-def]
        calls.append(args)
        if fail_on is not None and args[0] == fail_on:
            raise dg.Failure(description=f"corpus {fail_on} exited 1")
        return original(self, context, *args)

    monkeypatch.setattr(CorpusResource, "run", _run)
    return calls


def _subcommands(calls: list[tuple[str, ...]]) -> list[str]:
    return [args[0] for args in calls]


# --- the partition matrix -------------------------------------------------


def test_silver_starts_at_the_declared_coverage_floor() -> None:
    """The start is read from the dataset config, never carried as a literal.

    The expectation source is the YAML itself: a floor moved in corpus must move
    the matrix here, and a test that hardcoded the date would hide that.
    """
    config = yaml.safe_load(
        (DATASETS_DIR / f"{DATASET}.yaml").read_text(encoding="utf-8")
    )
    # YAML types a bare `2021-06-17` as a date; the partition keys are ISO strings.
    floor = str(config["silver"]["served_start"])
    # The premise of the resolution: every derivative folds one day of Silver
    # into one day of Gold (corpus ADR-0068), so none reaches back past the
    # floor and the derived preload lands on the floor itself.
    assert [d["name"] for d in config["gold"]] == [
        "contract-facts",
        "contract-item-facts",
        "contract-item-prices",
        "courier-rates",
    ]

    keys = silver_partitions.get_partition_keys()
    assert keys[0] == floor


def test_the_asset_module_carries_no_date_literal() -> None:
    """No start date is hardcoded in the module the matrix is built from."""
    from eve_industry_orchestration.defs import public_contracts

    source = Path(public_contracts.__file__).read_text(encoding="utf-8")
    code = "\n".join(
        line for line in source.splitlines() if not line.lstrip().startswith("#")
    )
    assert "2021-06-17" not in code


def test_the_code_location_registers_the_asset() -> None:
    """`load_from_defs_folder` picks the module up; nothing is registered by hand."""
    from eve_industry_orchestration.definitions import defs

    keys = {spec.key for spec in defs().resolve_all_asset_specs()}
    assert dg.AssetKey("public_contracts_silver") in keys


# --- ingest, then verify --------------------------------------------------


def test_successful_ingest_is_followed_by_verify(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = _record_runs(monkeypatch)

    result = dg.materialize(
        [public_contracts_silver], partition_key=DATE, resources={"corpus": corpus}
    )

    assert result.success
    assert _subcommands(calls) == ["ingest", "verify"]
    ingest, verify = calls
    assert ingest[:5] == ("ingest", "--dataset", DATASET, "--date", DATE)
    assert verify[:5] == ("verify", "--dataset", DATASET, "--date", DATE)
    assert verify[5:7] == ("--tier", "silver")
    # The binary owns the contract; the asset only shelled out to it.
    partition_dir = (
        Path(corpus.sink_path)
        / "silver"
        / DATASET
        / "year=2024"
        / "month=01"
        / "day=15"
    )
    assert (partition_dir / "_DONE").exists()


def test_failing_ingest_fails_without_verifying(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = _record_runs(monkeypatch, fail_on="ingest")

    result = dg.materialize(
        [public_contracts_silver],
        partition_key=DATE,
        resources={"corpus": corpus},
        raise_on_error=False,
    )

    assert not result.success
    assert _subcommands(calls) == ["ingest"]
    assert result.get_asset_materialization_events() == []


def test_failing_verify_fails_the_materialisation(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The fake keys its failure on ``dataset:tier:date``, so only a verify
    carrying exactly those three values can trip it."""
    monkeypatch.setenv("FAKE_VERIFY_FAIL_DATES", f"{DATASET}:silver:{DATE}")

    result = dg.materialize(
        [public_contracts_silver],
        partition_key=DATE,
        resources={"corpus": corpus},
        raise_on_error=False,
    )

    assert not result.success


def test_every_date_is_requested_the_same_way(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A day holding 28 archives and a day holding one are one argument vector.

    How many snapshots a day carries, and how they are packaged, is the binary's
    to resolve from the dataset config; a Python branch on it would be a storage-
    boundary violation.
    """
    calls = _record_runs(monkeypatch)
    early, late = "2021-06-17", DATE

    for date in (early, late):
        assert dg.materialize(
            [public_contracts_silver], partition_key=date, resources={"corpus": corpus}
        ).success

    ingests = [args for args in calls if args[0] == "ingest"]
    assert len(ingests) == 2
    before, after = ingests
    assert [a.replace(early, "<date>") for a in before] == [
        a.replace(late, "<date>") for a in after
    ]


# --- an absent upstream day (ADR-0028) ------------------------------------


def test_absent_upstream_day_is_observed_not_materialised(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FAKE_EVEREF_DATES", DATE)
    calls = _record_runs(monkeypatch)

    result = dg.materialize(
        [public_contracts_silver],
        partition_key="2024-01-20",
        resources={"corpus": corpus},
    )

    assert result.success
    # No verify: it would 404 on a partition that was deliberately not written.
    assert _subcommands(calls) == ["ingest"]
    # No materialisation → the partition stays Missing, not empty.
    assert result.get_asset_materialization_events() == []
    (observation,) = result.get_asset_observation_events()
    metadata = observation.event_specific_data.asset_observation.metadata
    assert metadata["skip_reason"].value == "upstream_absent"
    assert "2024-01-20" in metadata["detail"].value


def test_an_absent_day_does_not_stop_neighbouring_days(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FAKE_EVEREF_DATES", "2024-01-15,2024-01-17")

    outcomes = {
        date: dg.materialize(
            [public_contracts_silver], partition_key=date, resources={"corpus": corpus}
        )
        for date in ("2024-01-15", "2024-01-16", "2024-01-17")
    }

    assert all(result.success for result in outcomes.values())
    assert outcomes["2024-01-16"].get_asset_materialization_events() == []
    for date in ("2024-01-15", "2024-01-17"):
        assert len(outcomes[date].get_asset_materialization_events()) == 1


# --- a publication-frontier day (ADR-0041 classifier) ----------------------


def test_incomplete_upstream_day_is_observed_not_materialised(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FAKE_INCOMPLETE_DATES", DATE)
    calls = _record_runs(monkeypatch)

    result = dg.materialize(
        [public_contracts_silver], partition_key=DATE, resources={"corpus": corpus}
    )

    assert result.success
    # No verify: it would 404 on a partition that was deliberately not written.
    assert _subcommands(calls) == ["ingest"]
    # No materialisation → the partition stays Missing, not empty.
    assert result.get_asset_materialization_events() == []
    (observation,) = result.get_asset_observation_events()
    metadata = observation.event_specific_data.asset_observation.metadata
    assert metadata["skip_reason"].value == "upstream_incomplete"
    assert metadata["skip_reason"].value != "upstream_absent"


# --- run-state enrichment -------------------------------------------------


def test_materialisation_records_run_state_facts(corpus) -> None:
    result = dg.materialize(
        [public_contracts_silver], partition_key=DATE, resources={"corpus": corpus}
    )

    assert result.success
    (materialisation,) = result.get_asset_materialization_events()
    metadata = materialisation.materialization.metadata
    assert metadata["dataset"].value == DATASET
    assert metadata["tier"].value == "silver"
    assert metadata["partition"].value == DATE
    # Keyed on the run-state key (`date=<date>`), not the bare partition key.
    _assert_enriched(metadata)


def test_missing_run_state_row_still_succeeds_and_warns(
    corpus, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """The enrichment is advisory: no row is a warning, not a failed partition."""
    monkeypatch.setattr(CorpusResource, "state_query", lambda self, sql, **kw: [])

    result = dg.materialize(
        [public_contracts_silver], partition_key=DATE, resources={"corpus": corpus}
    )

    assert result.success
    (materialisation,) = result.get_asset_materialization_events()
    metadata = materialisation.materialization.metadata
    assert metadata["dataset"].value == DATASET
    assert _run_state_facts(metadata) == {}
    assert "partition metadata unavailable" in caplog.text


def test_failing_run_state_read_still_succeeds_and_warns(
    corpus, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setenv("FAKE_STATE_QUERY_FAIL", "1")

    result = dg.materialize(
        [public_contracts_silver], partition_key=DATE, resources={"corpus": corpus}
    )

    assert result.success
    (materialisation,) = result.get_asset_materialization_events()
    assert _run_state_facts(materialisation.materialization.metadata) == {}
    assert "partition metadata unavailable" in caplog.text


# --- the availability sensor ----------------------------------------------


def test_sensor_requests_newly_available_dates(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FAKE_EVEREF_DATES", "2024-01-15,2024-01-16")
    context = dg.build_sensor_context(resources={"corpus": corpus})

    result = public_contracts_availability_sensor(context)

    by_partition = {rr.partition_key: rr for rr in result.run_requests}
    assert sorted(by_partition) == ["2024-01-15", "2024-01-16"]
    assert by_partition["2024-01-15"].run_key.startswith(
        f"{DATASET}-silver-2024-01-15-"
    )


def test_sensor_excludes_dates_already_in_run_state(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Availability is the run-state diff, never a listing of the storage tree.

    Ingesting one date records it in run-state, so `missing-partitions` — and
    with it the sensor — drops it.
    """
    monkeypatch.setenv("FAKE_EVEREF_DATES", "2024-01-15,2024-01-16")
    assert dg.materialize(
        [public_contracts_silver],
        partition_key="2024-01-15",
        resources={"corpus": corpus},
    ).success

    result = public_contracts_availability_sensor(
        dg.build_sensor_context(resources={"corpus": corpus})
    )

    assert [rr.partition_key for rr in result.run_requests] == ["2024-01-16"]


def test_sensor_requests_nothing_when_nothing_is_missing(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FAKE_EVEREF_DATES", "")

    result = public_contracts_availability_sensor(
        dg.build_sensor_context(resources={"corpus": corpus})
    )

    assert result.run_requests == []


def test_sensor_caps_the_fan_out_and_carries_the_remainder(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The trailing edge only: a long backlog is capped, not drained in one tick.

    This sensor is not the backfill mechanism — the 1 892-day history is an
    operator action — so the remainder must still be reported missing next tick
    rather than dropped.
    """
    dates = [f"2024-01-{day:02d}" for day in range(5, 5 + MAX_PARTITIONS_PER_TICK + 1)]
    monkeypatch.setenv("FAKE_EVEREF_DATES", ",".join(dates))

    first = public_contracts_availability_sensor(
        dg.build_sensor_context(resources={"corpus": corpus})
    )

    # Oldest first, capped.
    assert [rr.partition_key for rr in first.run_requests] == (
        dates[:MAX_PARTITIONS_PER_TICK]
    )
    # Nothing was ingested, so the whole set is still missing on the next tick.
    second = public_contracts_availability_sensor(
        dg.build_sensor_context(resources={"corpus": corpus}, cursor=first.cursor)
    )
    assert [rr.partition_key for rr in second.run_requests] == (
        dates[:MAX_PARTITIONS_PER_TICK]
    )
    assert {rr.run_key for rr in first.run_requests}.isdisjoint(
        rr.run_key for rr in second.run_requests
    )


def test_sensor_ignores_a_date_below_the_coverage_floor(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The 2019 `.json.gz` days are below the floor and have no partition key."""
    monkeypatch.setenv("FAKE_EVEREF_DATES", "2019-04-10,2024-01-15")

    result = public_contracts_availability_sensor(
        dg.build_sensor_context(resources={"corpus": corpus})
    )

    assert [rr.partition_key for rr in result.run_requests] == ["2024-01-15"]


# --- independence from the live twin --------------------------------------


def test_the_history_tier_is_independent_of_the_live_twin() -> None:
    """Separate datasets, separate assets, and no dependency either way."""
    assert public_contracts_live_gold.partitions_def is None
    history = next(iter(public_contracts_silver.specs))
    live = next(iter(public_contracts_live_gold.specs))
    assert history.deps == []
    assert live.deps == []
    assert history.key != live.key
