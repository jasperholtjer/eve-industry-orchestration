"""Tests for the public-contracts history tier: Silver, its sensor, and Gold.

The dataset declares four Gold derivatives that each fold one day of Silver into
one day of Gold (ADR-0068 decision 5), so the Silver matrix still starts at the
``silver.served_start`` coverage floor — no derivative reaches back past it — and
each Gold tree carries its own partition matrix, its own build invocation and its
own run-state row. The four Gold assets are parametrised together because they
are deliberately identical in shape: only ``--derivative`` differs.

The live twin ``public-contracts-live`` is a separate dataset and is asserted
untouched here rather than assumed so.
"""

from __future__ import annotations

from pathlib import Path

import dagster as dg
import pytest
import yaml

from eve_industry_orchestration.defs.corpus_resource import CorpusResource
from eve_industry_orchestration.defs.public_contracts import (
    CONTRACT_FACTS_DERIVATIVE,
    CONTRACT_ITEM_FACTS_DERIVATIVE,
    CONTRACT_ITEM_PRICES_DERIVATIVE,
    COURIER_RATES_DERIVATIVE,
    DATASET,
    contract_facts_gold,
    contract_facts_gold_partitions,
    contract_item_facts_gold,
    contract_item_facts_gold_partitions,
    contract_item_prices_gold,
    contract_item_prices_gold_partitions,
    courier_rates_gold,
    courier_rates_gold_partitions,
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
# Any day other than DATE. Naming it as the only upstream-present day makes DATE
# a recorded upstream gap, which is how the fake reaches its "skipped" branch.
OTHER_DATE = "2024-01-14"

# The four Gold assets, each with the derivative it builds and the partitions
# definition its own configuration declares (corpus ADR-0068 decision 5).
GOLD_ASSETS = [
    pytest.param(
        contract_facts_gold,
        CONTRACT_FACTS_DERIVATIVE,
        contract_facts_gold_partitions,
        id="contract-facts",
    ),
    pytest.param(
        contract_item_facts_gold,
        CONTRACT_ITEM_FACTS_DERIVATIVE,
        contract_item_facts_gold_partitions,
        id="contract-item-facts",
    ),
    pytest.param(
        contract_item_prices_gold,
        CONTRACT_ITEM_PRICES_DERIVATIVE,
        contract_item_prices_gold_partitions,
        id="contract-item-prices",
    ),
    pytest.param(
        courier_rates_gold,
        COURIER_RATES_DERIVATIVE,
        courier_rates_gold_partitions,
        id="courier-rates",
    ),
]


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


def test_the_code_location_registers_every_asset() -> None:
    """`load_from_defs_folder` picks the module up; nothing is registered by hand."""
    from eve_industry_orchestration.definitions import defs

    keys = {spec.key for spec in defs().resolve_all_asset_specs()}
    assert dg.AssetKey("public_contracts_silver") in keys
    for name in (
        "contract_facts_gold",
        "contract_item_facts_gold",
        "contract_item_prices_gold",
        "courier_rates_gold",
    ):
        assert dg.AssetKey(name) in keys


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


# --- the Gold matrix ------------------------------------------------------


def _ingest(corpus, date: str) -> None:
    """Seals one day's Silver, which is all any of the four folds reads."""
    assert dg.materialize(
        [public_contracts_silver], partition_key=date, resources={"corpus": corpus}
    ).success


def _gold_partition_dir(corpus, derivative: str, date: str) -> Path:
    """Where corpus writes a Gold partition — under the *derivative* tree."""
    year, month, day = (int(part) for part in date.split("-"))
    return (
        Path(corpus.sink_path)
        / "gold"
        / derivative
        / f"year={year}"
        / f"month={month:02d}"
        / f"day={day:02d}"
    )


@pytest.mark.parametrize(("asset", "derivative", "partitions"), GOLD_ASSETS)
def test_each_derivative_starts_at_its_own_served_start(
    asset, derivative: str, partitions
) -> None:
    """Read from that derivative's own config entry, never from a literal."""
    config = yaml.safe_load(
        (DATASETS_DIR / f"{DATASET}.yaml").read_text(encoding="utf-8")
    )
    (entry,) = [d for d in config["gold"] if d["name"] == derivative]
    assert partitions.get_partition_keys()[0] == str(entry["served_start"])


@pytest.mark.parametrize(("asset", "derivative", "partitions"), GOLD_ASSETS)
def test_no_memory_bearing_pool_is_declared(asset, derivative: str, partitions) -> None:
    """Pool membership is by measured peak, and none of the four has one yet."""
    assert asset.op.pool is None


# --- build, then verify ---------------------------------------------------


@pytest.mark.parametrize(("asset", "derivative", "partitions"), GOLD_ASSETS)
def test_successful_build_is_followed_by_gold_verify(
    asset, derivative: str, partitions, corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    _ingest(corpus, DATE)
    calls = _record_runs(monkeypatch)

    result = dg.materialize([asset], partition_key=DATE, resources={"corpus": corpus})

    assert result.success
    # Exactly two calls: nothing inspects availability, coverage or completeness
    # before the build — whether a date can be built is the build's own answer.
    assert _subcommands(calls) == ["gold", "verify"]
    build, verify = calls
    assert build[:7] == (
        "gold",
        "build",
        "--dataset",
        DATASET,
        "--derivative",
        derivative,
        "--date",
    )
    # Gold verify keys on the *derivative*, because that is the tree corpus
    # wrote (`gold/<derivative>/...`); passing the dataset would 404.
    assert verify[:5] == ("verify", "--dataset", derivative, "--date", DATE)
    assert verify[5:7] == ("--tier", "gold")


@pytest.mark.parametrize(("asset", "derivative", "partitions"), GOLD_ASSETS)
def test_failing_build_fails_without_verifying(
    asset, derivative: str, partitions, corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The binary owns whatever gate it applies; a rejection fails the run here.

    The fake keys the gate rejection on ``dataset:derivative:date``, so only a
    build carrying exactly those three values can trip it.
    """
    _ingest(corpus, DATE)
    monkeypatch.setenv("FAKE_GOLD_GATE_FAIL_DATES", f"{DATASET}:{derivative}:{DATE}")
    calls = _record_runs(monkeypatch)

    result = dg.materialize(
        [asset],
        partition_key=DATE,
        resources={"corpus": corpus},
        raise_on_error=False,
    )

    assert not result.success
    assert _subcommands(calls) == ["gold"]
    assert result.get_asset_materialization_events() == []


@pytest.mark.parametrize(("asset", "derivative", "partitions"), GOLD_ASSETS)
def test_failing_gold_verify_fails_the_materialisation(
    asset, derivative: str, partitions, corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    _ingest(corpus, DATE)
    monkeypatch.setenv("FAKE_VERIFY_FAIL_DATES", f"{derivative}:gold:{DATE}")

    result = dg.materialize(
        [asset],
        partition_key=DATE,
        resources={"corpus": corpus},
        raise_on_error=False,
    )

    assert not result.success


@pytest.mark.parametrize(("asset", "derivative", "partitions"), GOLD_ASSETS)
def test_gold_materialisation_metadata_is_keyed_on_the_derivative(
    asset, derivative: str, partitions, corpus
) -> None:
    _ingest(corpus, DATE)

    result = dg.materialize([asset], partition_key=DATE, resources={"corpus": corpus})

    assert result.success
    (materialisation,) = result.get_asset_materialization_events()
    metadata = materialisation.materialization.metadata
    assert metadata["dataset"].value == DATASET
    assert metadata["derivative"].value == derivative
    assert metadata["tier"].value == "gold"
    assert metadata["partition"].value == DATE
    # The run-state row corpus wrote is registered under the derivative name, so
    # a lookup keyed on the dataset would silently enrich nothing.
    _assert_enriched(metadata)


@pytest.mark.parametrize(("asset", "derivative", "partitions"), GOLD_ASSETS)
def test_gold_skipped_day_is_observed_not_materialised(
    asset, derivative: str, partitions, corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A permanently-absent prerequisite leaves the partition Missing, run green."""
    # DATE is not in the upstream set, so the Silver run records it as a
    # permanent gap and the Gold build reports "skipped" for it.
    monkeypatch.setenv("FAKE_EVEREF_DATES", OTHER_DATE)
    assert dg.materialize(
        [public_contracts_silver], partition_key=DATE, resources={"corpus": corpus}
    ).success
    calls = _record_runs(monkeypatch)

    result = dg.materialize([asset], partition_key=DATE, resources={"corpus": corpus})

    assert result.success
    assert result.get_asset_materialization_events() == []
    # Verify is not invoked on a day that wrote no partition — it would 404.
    assert _subcommands(calls) == ["gold"]
    (observation,) = result.get_asset_observation_events()
    metadata = observation.event_specific_data.asset_observation.metadata
    assert metadata["skip_reason"].value == "upstream_gap"
    assert metadata["detail"].value


@pytest.mark.parametrize(("asset", "derivative", "partitions"), GOLD_ASSETS)
def test_gold_missing_run_state_row_still_succeeds_and_warns(
    asset,
    derivative: str,
    partitions,
    corpus,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The enrichment is advisory: no row is a warning, not a failed partition."""
    _ingest(corpus, DATE)
    monkeypatch.setattr(CorpusResource, "state_query", lambda self, sql, **kw: [])

    result = dg.materialize([asset], partition_key=DATE, resources={"corpus": corpus})

    assert result.success
    (materialisation,) = result.get_asset_materialization_events()
    assert _run_state_facts(materialisation.materialization.metadata) == {}
    assert "partition metadata unavailable" in caplog.text


# --- four derivatives of one dataset --------------------------------------


def test_each_derivative_writes_only_its_own_tree(corpus) -> None:
    """One Silver fold, four Gold trees, each built by an invocation naming
    only itself: materialising one must leave the other three absent."""
    _ingest(corpus, DATE)
    derivatives = [param.values[1] for param in GOLD_ASSETS]

    def _trees_on_disk() -> set[str]:
        return {
            name
            for name in derivatives
            if _gold_partition_dir(corpus, name, DATE).is_dir()
        }

    assert _trees_on_disk() == set()
    expected: set[str] = set()
    for param in GOLD_ASSETS:
        asset, derivative, _ = param.values
        assert dg.materialize(
            [asset], partition_key=DATE, resources={"corpus": corpus}
        ).success
        expected.add(derivative)
        # Only the derivatives materialised so far exist: no build produced
        # another derivative's partition.
        assert _trees_on_disk() == expected


def test_gold_assets_depend_on_the_silver_fold_only(corpus) -> None:
    """Lineage is the day's Silver. ``courier-rates``' cross-dataset reads are
    the builder's own, fingerprinted into ``_INDEX.json`` (ADR-0052), so they
    are deliberately not Dagster edges."""
    silver_key = next(iter(public_contracts_silver.specs)).key
    for param in GOLD_ASSETS:
        asset = param.values[0]
        spec = next(iter(asset.specs))
        assert [dep.asset_key for dep in spec.deps] == [silver_key]


# --- independence from the live twin --------------------------------------


def test_the_history_tier_is_independent_of_the_live_twin() -> None:
    """Separate datasets, separate assets, and no dependency either way."""
    assert public_contracts_live_gold.partitions_def is None
    history = next(iter(public_contracts_silver.specs))
    live = next(iter(public_contracts_live_gold.specs))
    assert history.deps == []
    assert live.deps == []
    assert history.key != live.key
