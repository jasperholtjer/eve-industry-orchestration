"""Tests for the sovereignty Gold derivatives and the fake binary that feeds them.

Two layers. The first drives the fake ``corpus`` binary directly over all five
sovereignty derivative names, because every asset and every readiness sensor in
this family is only as trustworthy as the fixture underneath it: a derivative
name the fake cannot resolve would make an asset test pass for the wrong reason.
The second exercises the two Gold assets this bundle adds.

One dataset per sink: the fake binary's Silver/skipped state is global rather
than per dataset, so a test that ingested two datasets into one sink would see
the second's dates in the first's results. The ``corpus`` fixture is
function-scoped, so each test owns a fresh sink and touches one dataset only.
"""

from __future__ import annotations

from pathlib import Path

import dagster as dg
import pytest

from eve_industry_orchestration.defs import sovereignty_campaigns as sc
from eve_industry_orchestration.defs import sovereignty_structures as ss
from eve_industry_orchestration.defs.corpus_resource import CorpusResource
from tests.conftest import _assert_enriched

DATE = "2024-01-15"
# Any day other than DATE. Naming it as the only upstream-present day makes DATE
# a recorded upstream gap, which is how the fake reaches its "skipped" branch.
OTHER_DATE = "2024-01-14"

# Every sovereignty derivative and the dataset that owns it (corpus ADR-0066).
# The panel is a `sovereignty-map` derivative even though it reads three trees.
DERIVATIVES = [
    pytest.param("sovereignty-map", "sovereignty-ownership", id="ownership"),
    pytest.param("sovereignty-map", "sovereignty-changes", id="changes"),
    pytest.param("sovereignty-map", "sovereignty-panel", id="panel"),
    pytest.param("sovereignty-structures", "sovereignty-adm", id="adm"),
    pytest.param("sovereignty-campaigns", "sovereignty-contests", id="contests"),
]

# The two assets this bundle adds, each with its dataset, derivative and the
# served start its own configuration declares. The pair is parametrised because
# the assets are deliberately identical in shape.
GOLD_ASSETS = [
    pytest.param(
        ss.sovereignty_adm_gold,
        "sovereignty-structures",
        "sovereignty-adm",
        ss.adm_gold_partitions,
        id="adm",
    ),
    pytest.param(
        sc.sovereignty_contests_gold,
        "sovereignty-campaigns",
        "sovereignty-contests",
        sc.contests_gold_partitions,
        id="contests",
    ),
]


def _context() -> dg.AssetExecutionContext:
    return dg.build_asset_context()  # type: ignore[return-value]


def _ingest(corpus, dataset: str, date: str) -> dict | None:
    return corpus.run(
        _context(),
        "ingest",
        "--dataset",
        dataset,
        "--date",
        date,
        "--sink-path",
        corpus.sink_path,
    )


def _gold_build(corpus, dataset: str, derivative: str, date: str) -> dict | None:
    return corpus.run(
        _context(),
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


def _gold_partition_dir(corpus, derivative: str, date: str) -> Path:
    """Where the fake writes a Gold partition — under the *derivative* tree."""
    year, month, day = (int(p) for p in date.split("-"))
    return (
        Path(corpus.sink_path)
        / "gold"
        / derivative
        / f"year={year}"
        / f"month={month:02d}"
        / f"day={day:02d}"
    )


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


# --- the fake binary answers for all five derivative names ----------------


@pytest.mark.parametrize(("dataset", "derivative"), DERIVATIVES)
def test_fake_binary_builds_every_sovereignty_derivative(
    dataset: str, derivative: str, corpus
) -> None:
    """Each of the five names resolves and writes its own Gold partition.

    A derivative the fake could not resolve exits 2 rather than writing, so the
    written contract files are what distinguishes "answered" from "tolerated".
    """
    _ingest(corpus, dataset, DATE)

    status = _gold_build(corpus, dataset, derivative, DATE)

    assert status is not None
    assert status["status"] == "written"
    assert status["derivative"] == derivative
    pdir = _gold_partition_dir(corpus, derivative, DATE)
    assert (pdir / "data.parquet").is_file()
    assert (pdir / "_DONE").is_file()


@pytest.mark.parametrize(("dataset", "derivative"), DERIVATIVES)
def test_fake_binary_reports_ready_dates_per_derivative(
    dataset: str, derivative: str, corpus
) -> None:
    """``gold ready-dates`` takes the selector and answers for that derivative.

    A day drops out of ``ready`` once that derivative's Gold is built, which is
    the state-level diff the readiness sensors will run on.
    """
    _ingest(corpus, dataset, DATE)

    report = corpus.gold_ready_dates(dataset, derivative=derivative)
    assert report["derivative"] == derivative
    assert DATE in report["ready"]

    _gold_build(corpus, dataset, derivative, DATE)

    after = corpus.gold_ready_dates(dataset, derivative=derivative)
    assert DATE not in after["ready"]


@pytest.mark.parametrize(("dataset", "derivative"), DERIVATIVES)
def test_fake_binary_reports_both_a_written_and_a_skipped_day(
    dataset: str, derivative: str, corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    """One derivative, both build outcomes, switched by the existing fixture.

    ``FAKE_EVEREF_DATES`` is the switch: a day it omits is recorded as a
    permanent upstream gap by ``ingest``, and ``gold build`` then reports
    ``status: "skipped"`` with a reason and writes nothing (ADR-0029/ADR-0065).
    No second skip mechanism is needed for the panel — the branch is keyed on
    the day, not on the derivative.
    """
    monkeypatch.setenv("FAKE_EVEREF_DATES", OTHER_DATE)
    _ingest(corpus, dataset, OTHER_DATE)
    _ingest(corpus, dataset, DATE)

    written = _gold_build(corpus, dataset, derivative, OTHER_DATE)
    assert written is not None
    assert written["status"] == "written"

    skipped = _gold_build(corpus, dataset, derivative, DATE)
    assert skipped is not None
    assert skipped["status"] == "skipped"
    assert skipped["derivative"] == derivative
    assert skipped["reason"]
    assert not _gold_partition_dir(corpus, derivative, DATE).exists()


# --- partition matrices come from the corpus config -----------------------


@pytest.mark.parametrize(("asset", "dataset", "derivative", "partitions"), GOLD_ASSETS)
def test_gold_partition_start_is_the_derivatives_served_start(
    asset, dataset: str, derivative: str, partitions
) -> None:
    """The start is the derivative's own ``served_start``, never a literal here."""
    from eve_industry_orchestration.defs.config import resolve_partition_starts

    expected = resolve_partition_starts(dataset, derivative).gold
    assert partitions.start.strftime("%Y-%m-%d") == expected


# --- the Gold assets ------------------------------------------------------


@pytest.mark.parametrize(("asset", "dataset", "derivative", "partitions"), GOLD_ASSETS)
def test_successful_build_is_followed_by_gold_verify(
    asset, dataset: str, derivative: str, partitions, corpus, monkeypatch
) -> None:
    _ingest(corpus, dataset, DATE)
    calls = _record_runs(monkeypatch)

    result = dg.materialize([asset], partition_key=DATE, resources={"corpus": corpus})

    assert result.success
    assert _subcommands(calls) == ["gold", "verify"]
    build, verify = calls
    assert build[:7] == (
        "gold",
        "build",
        "--dataset",
        dataset,
        "--derivative",
        derivative,
        "--date",
    )
    # Gold verify keys on the *derivative*, because that is the tree corpus
    # wrote (`gold/<derivative>/...`); passing the dataset would 404.
    assert verify[:5] == ("verify", "--dataset", derivative, "--date", DATE)
    assert verify[5:7] == ("--tier", "gold")


@pytest.mark.parametrize(("asset", "dataset", "derivative", "partitions"), GOLD_ASSETS)
def test_materialisation_metadata_is_keyed_on_the_derivative(
    asset, dataset: str, derivative: str, partitions, corpus
) -> None:
    _ingest(corpus, dataset, DATE)

    result = dg.materialize([asset], partition_key=DATE, resources={"corpus": corpus})

    assert result.success
    (materialization,) = result.get_asset_materialization_events()
    metadata = materialization.materialization.metadata
    assert metadata["dataset"].value == dataset
    assert metadata["derivative"].value == derivative
    assert metadata["tier"].value == "gold"
    assert metadata["partition"].value == DATE
    # The run-state row corpus wrote is registered under the derivative name, so
    # a lookup keyed on the dataset would silently enrich nothing.
    _assert_enriched(metadata)


@pytest.mark.parametrize(("asset", "dataset", "derivative", "partitions"), GOLD_ASSETS)
def test_skipped_day_is_observed_not_materialised(
    asset, dataset: str, derivative: str, partitions, corpus, monkeypatch
) -> None:
    """A permanently-absent prerequisite leaves the partition Missing, run green."""
    monkeypatch.setenv("FAKE_EVEREF_DATES", OTHER_DATE)
    _ingest(corpus, dataset, DATE)
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


@pytest.mark.parametrize(("asset", "dataset", "derivative", "partitions"), GOLD_ASSETS)
def test_failing_build_fails_without_verifying(
    asset, dataset: str, derivative: str, partitions, corpus, monkeypatch
) -> None:
    """The binary owns the coverage gate; a rejected window fails the run here.

    The fake keys the gate rejection on ``dataset:derivative:date``, so only a
    build carrying exactly those three values can trip it.
    """
    _ingest(corpus, dataset, DATE)
    monkeypatch.setenv("FAKE_GOLD_GATE_FAIL_DATES", f"{dataset}:{derivative}:{DATE}")
    calls = _record_runs(monkeypatch)

    result = dg.materialize(
        [asset],
        partition_key=DATE,
        resources={"corpus": corpus},
        raise_on_error=False,
    )

    assert not result.success
    assert _subcommands(calls) == ["gold"]


@pytest.mark.parametrize(("asset", "dataset", "derivative", "partitions"), GOLD_ASSETS)
def test_failing_gold_verify_fails_the_materialisation(
    asset, dataset: str, derivative: str, partitions, corpus, monkeypatch
) -> None:
    _ingest(corpus, dataset, DATE)
    monkeypatch.setenv("FAKE_VERIFY_FAIL_DATES", f"{derivative}:gold:{DATE}")

    result = dg.materialize(
        [asset],
        partition_key=DATE,
        resources={"corpus": corpus},
        raise_on_error=False,
    )

    assert not result.success


@pytest.mark.parametrize(("asset", "dataset", "derivative", "partitions"), GOLD_ASSETS)
def test_no_memory_bearing_pool_is_declared(
    asset, dataset: str, derivative: str, partitions
) -> None:
    """Pool membership is by measured peak, and neither build has one yet."""
    assert asset.op.pool is None
