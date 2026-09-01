"""Tests for the sovereignty Gold derivatives and the fake binary that feeds them.

Two layers. The first drives the fake ``corpus`` binary directly over all five
sovereignty derivative names, because every asset and every readiness sensor in
this family is only as trustworthy as the fixture underneath it: a derivative
name the fake cannot resolve would make an asset test pass for the wrong reason.
The second exercises the five Gold assets themselves, including the panel's
assembly edge over the other four and the SDE snapshot.

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
from eve_industry_orchestration.defs import sovereignty_map as sm
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

# The panel's same-day Gold prerequisites (corpus ADR-0066 decision 8): corpus
# reports a panel date ready only once these three trees hold that day. They
# span three datasets, but the fake's Silver state is keyed by date alone, so a
# sibling Gold build over an already-ingested day needs no second ingest.
PANEL_PREREQUISITES = [
    ("sovereignty-map", "sovereignty-ownership"),
    ("sovereignty-structures", "sovereignty-adm"),
    ("sovereignty-campaigns", "sovereignty-contests"),
]

# Every sovereignty Gold asset, each with its dataset, derivative and the
# partitions definition its own configuration declares. All five are
# parametrised together because they are deliberately identical in shape: the
# panel differs only in what it depends on, never in how it builds.
GOLD_ASSETS = [
    pytest.param(
        sm.sovereignty_ownership_gold,
        "sovereignty-map",
        "sovereignty-ownership",
        sm.ownership_gold_partitions,
        id="ownership",
    ),
    pytest.param(
        sm.sovereignty_changes_gold,
        "sovereignty-map",
        "sovereignty-changes",
        sm.changes_gold_partitions,
        id="changes",
    ),
    pytest.param(
        sm.sovereignty_panel_gold,
        "sovereignty-map",
        "sovereignty-panel",
        sm.panel_gold_partitions,
        id="panel",
    ),
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
    the state-level diff the readiness sensors will run on. The panel is also
    gated on its three same-day sibling trees, so this case builds them first —
    that gate is exercised in both directions in the sensor tests.
    """
    _ingest(corpus, dataset, DATE)
    if derivative == "sovereignty-panel":
        for prerequisite_dataset, prerequisite in PANEL_PREREQUISITES:
            _gold_build(corpus, prerequisite_dataset, prerequisite, DATE)

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


# --- sibling derivatives off one dataset ----------------------------------


def test_sibling_derivatives_write_only_their_own_partition(
    corpus, monkeypatch
) -> None:
    """Ownership and changes share a Silver fold but not a Gold tree.

    Both builds are invoked with their own ``--derivative``; corpus writes each
    tree under that name, so materialising one must leave the other's partition
    absent.
    """
    _ingest(corpus, "sovereignty-map", DATE)
    calls = _record_runs(monkeypatch)

    result = dg.materialize(
        [sm.sovereignty_ownership_gold],
        partition_key=DATE,
        resources={"corpus": corpus},
    )

    assert result.success
    assert "--derivative" in calls[0]
    assert calls[0][calls[0].index("--derivative") + 1] == "sovereignty-ownership"
    assert _gold_partition_dir(corpus, "sovereignty-ownership", DATE).is_dir()
    assert not _gold_partition_dir(corpus, "sovereignty-changes", DATE).exists()

    assert dg.materialize(
        [sm.sovereignty_changes_gold], partition_key=DATE, resources={"corpus": corpus}
    ).success
    assert _gold_partition_dir(corpus, "sovereignty-changes", DATE).is_dir()


def test_run_state_facts_are_read_per_derivative(corpus) -> None:
    """Each of the pair records its own run-state row, not its sibling's.

    The fake stamps ``parquet_sha256`` from the ``<dataset>|<tier>|<key>``
    identity, so the two derivatives have genuinely different facts registered
    for the same date; a lookup keyed on ``sovereignty-map`` would match neither.
    """
    _ingest(corpus, "sovereignty-map", DATE)

    facts = {}
    for asset in (sm.sovereignty_ownership_gold, sm.sovereignty_changes_gold):
        result = dg.materialize(
            [asset], partition_key=DATE, resources={"corpus": corpus}
        )
        assert result.success
        (materialization,) = result.get_asset_materialization_events()
        metadata = materialization.materialization.metadata
        _assert_enriched(metadata)
        facts[metadata["derivative"].value] = metadata["parquet_sha256"].value

    assert set(facts) == {"sovereignty-ownership", "sovereignty-changes"}
    assert len(set(facts.values())) == 2


# --- the panel's assembly edge --------------------------------------------


def test_panel_depends_on_its_four_siblings_and_the_sde_snapshot() -> None:
    """ADR-0066's build order is a real edge in the graph, not schedule timing."""
    # `deps=`, not parameters: every edge is Nothing-typed, so no IO manager
    # loads an upstream — the five carry lineage and ordering only. The
    # non-partitioned SDE snapshot is one of them and is no different.
    assert [
        in_def.dagster_type.is_nothing
        for in_def in sm.sovereignty_panel_gold.op.ins.values()
    ] == [True] * 5
    (spec,) = sm.sovereignty_panel_gold.specs
    assert {dep.asset_key for dep in spec.deps} == {
        dg.AssetKey("sovereignty_ownership_gold"),
        dg.AssetKey("sovereignty_changes_gold"),
        dg.AssetKey("sovereignty_adm_gold"),
        dg.AssetKey("sovereignty_contests_gold"),
        dg.AssetKey("sde_snapshot"),
    }


def test_the_code_location_loads_with_the_panel_edge() -> None:
    """No import cycle: sovereignty_map imports two sibling modules, neither back."""
    from eve_industry_orchestration.definitions import defs

    keys = {spec.key for spec in defs().resolve_all_asset_specs()}
    assert dg.AssetKey("sovereignty_panel_gold") in keys


def test_panel_materialises_with_no_sde_partition_provided(corpus, monkeypatch) -> None:
    """The non-partitioned SDE dep carries lineage only.

    Nothing about the SDE snapshot is fetched, mapped to a partition or passed
    in: the panel run is exactly the two-call build/verify of any other date.
    """
    _ingest(corpus, "sovereignty-map", DATE)
    calls = _record_runs(monkeypatch)

    result = dg.materialize(
        [sm.sovereignty_panel_gold], partition_key=DATE, resources={"corpus": corpus}
    )

    assert result.success
    assert _subcommands(calls) == ["gold", "verify"]


# --- the two starts, both from configuration ------------------------------


def test_panel_starts_one_flip_window_after_its_siblings() -> None:
    """2022-01-31 against 2022-01-01, both read out of the dataset YAML."""
    from eve_industry_orchestration.defs.config import resolve_partition_starts

    starts = {
        derivative: resolve_partition_starts("sovereignty-map", derivative).gold
        for derivative in (
            "sovereignty-ownership",
            "sovereignty-changes",
            "sovereignty-panel",
        )
    }
    assert starts["sovereignty-ownership"] == starts["sovereignty-changes"]
    assert starts["sovereignty-panel"] > starts["sovereignty-ownership"]
    assert (
        sm.panel_gold_partitions.start.strftime("%Y-%m-%d")
        == starts["sovereignty-panel"]
    )
    assert (
        sm.ownership_gold_partitions.start.strftime("%Y-%m-%d")
        == (starts["sovereignty-ownership"])
    )


SOVEREIGNTY_MODULES = (
    "sovereignty_map",
    "sovereignty_structures",
    "sovereignty_campaigns",
)


def _executable_source(module_name: str) -> str:
    """The module's code with comments and docstrings removed.

    Both are prose about what configuration yields and what the binary decides,
    and both legitimately name dates and coverage; only what actually runs is
    the subject of the two assertions below. ``ast.unparse`` drops comments, and
    the docstring statements are stripped explicitly.
    """
    import ast

    source = (Path(sm.__file__).parent / f"{module_name}.py").read_text(
        encoding="utf-8"
    )
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if not isinstance(
            node, ast.Module | ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef
        ):
            continue
        body = node.body
        if (
            body
            and isinstance(body[0], ast.Expr)
            and isinstance(body[0].value, ast.Constant)
            and isinstance(body[0].value.value, str)
        ):
            node.body = body[1:] or [ast.Pass()]
    return ast.unparse(tree)


@pytest.mark.parametrize("module_name", SOVEREIGNTY_MODULES)
def test_no_sovereignty_module_writes_a_date_literal(module_name: str) -> None:
    """Starts come from the corpus config; a literal here would silently drift."""
    import re

    assert not re.search(r"\d{4}-\d{2}-\d{2}", _executable_source(module_name))


# --- the two gates are both the binary's ----------------------------------


def test_an_incomplete_flip_window_is_not_a_skip(corpus, monkeypatch) -> None:
    """A short flip window nulls two columns; it does not skip the day.

    ``FAKE_SHORT_FLIP_WINDOW_DATES`` is the switch: the build reports a
    *written* partition whose ``constellation_flips_30d`` / ``region_flips_30d``
    are NULL, the way the real binary warns and publishes the counts as null
    when the trailing 30 days are not all there. That is the ordinary path —
    the partition materialises and Gold verify runs — and it produces none of
    what the skipped branch above produces: no observation, no missing
    partition, no failure.
    """
    monkeypatch.setenv(
        "FAKE_SHORT_FLIP_WINDOW_DATES", f"sovereignty-map:sovereignty-panel:{DATE}"
    )
    _ingest(corpus, "sovereignty-map", DATE)

    built = _gold_build(corpus, "sovereignty-map", "sovereignty-panel", DATE)
    assert built is not None
    assert built["status"] == "written"
    assert built["constellation_flips_30d"] is None
    assert built["region_flips_30d"] is None

    calls = _record_runs(monkeypatch)
    result = dg.materialize(
        [sm.sovereignty_panel_gold], partition_key=DATE, resources={"corpus": corpus}
    )

    assert result.success
    (materialization,) = result.get_asset_materialization_events()
    assert materialization.materialization.metadata["derivative"].value == (
        "sovereignty-panel"
    )
    assert _subcommands(calls) == ["gold", "verify"]
    assert result.get_asset_observation_events() == []


def test_a_complete_flip_window_reports_the_two_counts(corpus) -> None:
    """The switch above is a real fixture branch, not the only thing it does.

    Without it the same build reports both flip counts populated, so the NULLs
    in the test above come from the short window and nothing else.
    """
    _ingest(corpus, "sovereignty-map", DATE)

    built = _gold_build(corpus, "sovereignty-map", "sovereignty-panel", DATE)

    assert built is not None
    assert built["status"] == "written"
    assert built["constellation_flips_30d"] is not None
    assert built["region_flips_30d"] is not None


@pytest.mark.parametrize("module_name", SOVEREIGNTY_MODULES)
def test_no_asset_inspects_window_coverage(module_name: str) -> None:
    """The Gold gate is the binary's; a Python pre-check would duplicate it.

    Nothing that could read a window runs in these modules: no coverage
    arithmetic, no readiness poll, no run-state query beyond the advisory
    metadata read, and no tree walk.
    """
    code = _executable_source(module_name)
    for forbidden in ("coverage", "ready_dates", "state_query", "glob", "listdir"):
        assert forbidden not in code, f"{module_name} names {forbidden}"
