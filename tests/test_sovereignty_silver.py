"""Tests for the three sovereignty Silver assets.

Every case is parametrised over the family, because the three assets are
deliberately identical in shape: a divergence in any one of them is the
regression these tests exist to catch.

One dataset per sink: the fake binary's Silver/skipped state is global rather
than per dataset, so a test that ingested two datasets into one sink would see
the second's dates in the first's results. The ``corpus`` fixture is
function-scoped, so each test owns a fresh sink and touches one dataset only.
"""

from __future__ import annotations

import dagster as dg
import pytest

from eve_industry_orchestration.defs.corpus_resource import CorpusResource
from eve_industry_orchestration.defs.sovereignty_campaigns import (
    sovereignty_campaigns_silver,
)
from eve_industry_orchestration.defs.sovereignty_map import sovereignty_map_silver
from eve_industry_orchestration.defs.sovereignty_structures import (
    sovereignty_structures_silver,
)
from tests.conftest import _assert_enriched, _run_state_facts

# The dataset name is the `--dataset` value for ingest and Silver verify — never
# a Gold derivative name, which is a distinct namespace (ADR-0025).
SILVER_ASSETS = [
    pytest.param(sovereignty_map_silver, "sovereignty-map", id="map"),
    pytest.param(
        sovereignty_structures_silver, "sovereignty-structures", id="structures"
    ),
    pytest.param(sovereignty_campaigns_silver, "sovereignty-campaigns", id="campaigns"),
]

# The on-disk layout changes from per-date folders to yearly tarballs at
# 2022-12-16 (`source.folder_era_start`). Both dates are inside every one of the
# three datasets' partition ranges (the latest Silver start is 2022-01-01).
BEFORE_ERA_CHANGE = "2022-06-15"
AFTER_ERA_CHANGE = "2023-03-01"

DATE = "2024-01-15"


def _record_runs(
    monkeypatch: pytest.MonkeyPatch, *, fail_on: str | None = None
) -> list[tuple[str, ...]]:
    """Records every ``corpus`` subcommand the asset invokes, in order.

    Delegates to the real resource so the fake binary still runs, unless
    ``fail_on`` names a subcommand — then that one raises ``dg.Failure``, which
    is what the resource itself raises on a non-zero exit.
    """
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


# --- ingest, then verify --------------------------------------------------


@pytest.mark.parametrize(("asset", "dataset"), SILVER_ASSETS)
def test_successful_ingest_is_followed_by_verify(
    asset, dataset: str, corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = _record_runs(monkeypatch)

    result = dg.materialize([asset], partition_key=DATE, resources={"corpus": corpus})

    assert result.success
    assert _subcommands(calls) == ["ingest", "verify"]
    ingest, verify = calls
    assert ingest[:5] == ("ingest", "--dataset", dataset, "--date", DATE)
    # Verify keys on the same dataset and date, at the Silver tier.
    assert verify[:5] == ("verify", "--dataset", dataset, "--date", DATE)
    assert verify[5:7] == ("--tier", "silver")


@pytest.mark.parametrize(("asset", "dataset"), SILVER_ASSETS)
def test_failing_verify_fails_the_materialisation(
    asset, dataset: str, corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Proves the verify reaches the binary with this dataset and date.

    The fake keys its failure on ``dataset:tier:date``, so only a verify carrying
    exactly those three values can trip it.
    """
    monkeypatch.setenv("FAKE_VERIFY_FAIL_DATES", f"{dataset}:silver:{DATE}")

    result = dg.materialize(
        [asset],
        partition_key=DATE,
        resources={"corpus": corpus},
        raise_on_error=False,
    )

    assert not result.success


@pytest.mark.parametrize(("asset", "dataset"), SILVER_ASSETS)
def test_failing_ingest_fails_without_verifying(
    asset, dataset: str, corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = _record_runs(monkeypatch, fail_on="ingest")

    result = dg.materialize(
        [asset],
        partition_key=DATE,
        resources={"corpus": corpus},
        raise_on_error=False,
    )

    assert not result.success
    assert _subcommands(calls) == ["ingest"]
    assert result.get_asset_materialization_events() == []


# --- an absent upstream day (ADR-0028) ------------------------------------


@pytest.mark.parametrize(("asset", "dataset"), SILVER_ASSETS)
def test_absent_upstream_day_is_observed_not_materialised(
    asset, dataset: str, corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Upstream has DATE only; 2024-01-20 is an interior gap, so corpus exits 0
    # with status "skipped" and the partition is left Missing.
    monkeypatch.setenv("FAKE_EVEREF_DATES", DATE)
    calls = _record_runs(monkeypatch)

    result = dg.materialize(
        [asset], partition_key="2024-01-20", resources={"corpus": corpus}
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


@pytest.mark.parametrize(("asset", "dataset"), SILVER_ASSETS)
def test_an_absent_day_does_not_stop_neighbouring_days(
    asset, dataset: str, corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FAKE_EVEREF_DATES", "2024-01-15,2024-01-17")

    outcomes = {
        date: dg.materialize([asset], partition_key=date, resources={"corpus": corpus})
        for date in ("2024-01-15", "2024-01-16", "2024-01-17")
    }

    assert all(result.success for result in outcomes.values())
    assert outcomes["2024-01-16"].get_asset_materialization_events() == []
    for date in ("2024-01-15", "2024-01-17"):
        assert len(outcomes[date].get_asset_materialization_events()) == 1


# --- run-state enrichment -------------------------------------------------


@pytest.mark.parametrize(("asset", "dataset"), SILVER_ASSETS)
def test_materialisation_records_run_state_facts(asset, dataset: str, corpus) -> None:
    result = dg.materialize([asset], partition_key=DATE, resources={"corpus": corpus})

    assert result.success
    (materialisation,) = result.get_asset_materialization_events()
    metadata = materialisation.materialization.metadata
    assert metadata["dataset"].value == dataset
    assert metadata["tier"].value == "silver"
    assert metadata["partition"].value == DATE
    # Keyed on the run-state key (`date=<date>`), not the bare partition key.
    _assert_enriched(metadata)


@pytest.mark.parametrize(("asset", "dataset"), SILVER_ASSETS)
def test_failing_run_state_read_still_succeeds_and_warns(
    asset,
    dataset: str,
    corpus,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The enrichment is cosmetic; it must not fail a completed materialisation."""
    monkeypatch.setenv("FAKE_STATE_QUERY_FAIL", "1")

    result = dg.materialize([asset], partition_key=DATE, resources={"corpus": corpus})

    assert result.success
    (materialisation,) = result.get_asset_materialization_events()
    metadata = materialisation.materialization.metadata
    assert metadata["dataset"].value == dataset
    assert _run_state_facts(metadata) == {}
    assert "partition metadata unavailable" in caplog.text


# --- the folder/tar era boundary is the binary's ---------------------------


@pytest.mark.parametrize(("asset", "dataset"), SILVER_ASSETS)
def test_ingest_shape_is_identical_across_the_layout_era(
    asset, dataset: str, corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A date either side of ``folder_era_start`` is requested the same way.

    The layout change is resolved by the binary from the dataset config; a Python
    branch on the era would be a storage-boundary violation, and this asserts the
    argument vectors differ in the date alone.
    """
    calls = _record_runs(monkeypatch)

    for date in (BEFORE_ERA_CHANGE, AFTER_ERA_CHANGE):
        assert dg.materialize(
            [asset], partition_key=date, resources={"corpus": corpus}
        ).success

    ingests = [args for args in calls if args[0] == "ingest"]
    assert len(ingests) == 2
    before, after = ingests
    # Same flags, same order, same values — only --date moves.
    assert [a.replace(BEFORE_ERA_CHANGE, "<date>") for a in before] == [
        a.replace(AFTER_ERA_CHANGE, "<date>") for a in after
    ]
