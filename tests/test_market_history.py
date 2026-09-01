"""Tests for the market-history Silver completeness handling and the Gold path."""

from __future__ import annotations

import dagster as dg
import pytest

from eve_industry_orchestration.defs.config import resolve_partition_starts
from eve_industry_orchestration.defs.corpus_resource import CorpusResource
from eve_industry_orchestration.defs.market_history import (
    DATASET,
    gold_partitions,
    market_history_gold,
    market_history_silver,
)
from eve_industry_orchestration.defs.market_orders import (
    market_orders_changes_gold,
    market_orders_events_gold,
    market_orders_snapshot_gold,
)
from eve_industry_orchestration.defs.sensors import market_history_gold_sensor

# Well within the resolved Silver partition range (gold served_start minus one
# rolling window), and a real day in the polluted cohort the gate targets.
_DATE = "2026-06-24"


def test_silver_leaves_incomplete_day_missing(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An incomplete upstream publication leaves the partition Missing, retryable."""
    monkeypatch.setenv("FAKE_INCOMPLETE_DATES", _DATE)

    result = dg.materialize(
        [market_history_silver],
        partition_key=_DATE,
        resources={"corpus": corpus},
    )

    assert result.success
    # No materialisation — the partition stays Missing and is re-proposed.
    assert result.get_asset_materialization_events() == []
    observations = result.get_asset_observation_events()
    assert len(observations) == 1
    metadata = observations[0].event_specific_data.asset_observation.metadata
    assert metadata["skip_reason"].value == "upstream_incomplete"


def test_silver_materialises_settled_day(corpus) -> None:
    """A settled day (no incomplete flag) materialises normally."""
    result = dg.materialize(
        [market_history_silver],
        partition_key=_DATE,
        resources={"corpus": corpus},
    )

    assert result.success
    materializations = result.get_asset_materialization_events()
    assert len(materializations) == 1


# --- Gold: build then verify, with the gate owned by the binary --------------

# Well inside the resolved Gold range (served_start 2021-01-01).
_GOLD_DATE = "2024-01-15"


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


def _spy_on_corpus(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, ...]]:
    """Records every ``corpus`` invocation, in order, while still running it.

    The asset's contract is an ordering (build, then verify) and a flag set, and
    neither is visible in the materialisation result — only in what was invoked.
    """
    calls: list[tuple[str, ...]] = []
    original = CorpusResource.run

    def spy(self, context, *args: str):
        calls.append(args)
        return original(self, context, *args)

    monkeypatch.setattr(CorpusResource, "run", spy)
    return calls


def test_gold_builds_then_verifies_the_same_date(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Build runs first, Gold-tier verify follows for the same date, both green.

    The two invocations are also the *only* ones: the asset performs no coverage
    arithmetic and opens no partition of its own — the window decision is the
    binary's alone.
    """
    _ingest(corpus, _GOLD_DATE)
    calls = _spy_on_corpus(monkeypatch)

    result = dg.materialize(
        [market_history_gold],
        partition_key=_GOLD_DATE,
        selection=[market_history_gold],
        resources={"corpus": corpus},
    )

    assert result.success
    (materialization,) = result.get_asset_materialization_events()
    metadata = materialization.materialization.metadata
    assert metadata["dataset"].value == "market-history"
    assert metadata["tier"].value == "gold"
    assert metadata["partition"].value == _GOLD_DATE

    assert len(calls) == 2
    build, verify = calls
    assert build[:2] == ("gold", "build")
    assert verify[0] == "verify"
    assert "--tier" in verify and verify[verify.index("--tier") + 1] == "gold"
    for call in calls:
        assert call[call.index("--date") + 1] == _GOLD_DATE


def test_gold_does_not_verify_a_failed_build(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A non-zero build fails the run before verification is attempted."""
    # No Silver ingest for the target day, so the build exits non-zero.
    calls = _spy_on_corpus(monkeypatch)

    result = dg.materialize(
        [market_history_gold],
        partition_key=_GOLD_DATE,
        selection=[market_history_gold],
        resources={"corpus": corpus},
        raise_on_error=False,
    )

    assert not result.success
    assert result.get_asset_materialization_events() == []
    assert [call[:2] for call in calls] == [("gold", "build")]


def test_gold_incomplete_window_fails_without_materialising(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The coverage gate is the binary's: its non-zero exit is never suppressed."""
    _ingest(corpus, _GOLD_DATE)
    monkeypatch.setenv("FAKE_GOLD_GATE_FAIL_DATES", _GOLD_DATE)

    result = dg.materialize(
        [market_history_gold],
        partition_key=_GOLD_DATE,
        selection=[market_history_gold],
        resources={"corpus": corpus},
        raise_on_error=False,
    )

    assert not result.success
    assert result.get_asset_materialization_events() == []


def test_gold_fails_when_verification_fails(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A green build with a failed contract check is a failed run, not a success."""
    _ingest(corpus, _GOLD_DATE)
    monkeypatch.setenv("FAKE_VERIFY_FAIL_DATES", _GOLD_DATE)
    calls = _spy_on_corpus(monkeypatch)

    result = dg.materialize(
        [market_history_gold],
        partition_key=_GOLD_DATE,
        selection=[market_history_gold],
        resources={"corpus": corpus},
        raise_on_error=False,
    )

    assert not result.success
    assert result.get_asset_materialization_events() == []
    # The build did run; only the verification refused it.
    assert [call[0] for call in calls] == ["gold", "verify"]


def test_gold_stale_readiness_still_fails_at_the_gate(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The sensor pre-check is an optimisation; the binary remains the gate.

    The date is reported ready at tick time, then the window degrades before the
    build runs — the run fails and the orchestrator claims nothing.
    """
    _ingest(corpus, _GOLD_DATE)
    tick = market_history_gold_sensor(
        dg.build_sensor_context(resources={"corpus": corpus})
    )
    assert [rr.partition_key for rr in tick.run_requests] == [_GOLD_DATE]

    monkeypatch.setenv("FAKE_GOLD_GATE_FAIL_DATES", _GOLD_DATE)
    result = dg.materialize(
        [market_history_gold],
        partition_key=_GOLD_DATE,
        selection=[market_history_gold],
        resources={"corpus": corpus},
        raise_on_error=False,
    )

    assert not result.success
    assert result.get_asset_materialization_events() == []


def test_gold_passes_the_root_as_a_flag_and_assembles_no_path(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Both operations get the configured root; neither gets a date-derived path."""
    _ingest(corpus, _GOLD_DATE)
    calls = _spy_on_corpus(monkeypatch)

    dg.materialize(
        [market_history_gold],
        partition_key=_GOLD_DATE,
        selection=[market_history_gold],
        resources={"corpus": corpus},
    )

    for call in calls:
        assert call[call.index("--sink-path") + 1] == corpus.sink_path
        # No layout built in Python: the only date the binary is given is the
        # partition key itself, never a year=/month=/day= path or a tier root.
        assert not any(arg.startswith("year=") for arg in call)
        assert not any(_GOLD_DATE in arg and arg != _GOLD_DATE for arg in call), (
            "a path assembled from the partition date reached the binary"
        )


def test_gold_declares_the_heavy_pool() -> None:
    """Config-level assertion: pool arbitration is not observable in this suite.

    The fake-binary tests run no scheduler and no run coordinator, so "the excess
    runs queue" cannot be exercised here. What orchestration owns is the
    *declaration* — a pool on the asset bounds every launch path (sensor,
    backfill, manual), where a sensor-set run tag would bound only one — so the
    declaration is what this asserts. The limit itself lives in
    deploy/dagster.yaml.
    """
    assert market_history_gold.op.pool == "heavy"

    # `heavy` is one SHARED memory budget, not a per-asset limit: every wide-window
    # ~3-4 GB Gold build names the same pool so at most `default_limit` of them run
    # at once, whatever mix the coordinator picks. Pools are created implicitly by
    # `pool=`, so a drifted literal in one module (typo, half-done rename) does not
    # error — that asset silently gets its own pool with its own limit, and the real
    # ceiling doubles while every other test stays green. Pin the names to each
    # other. market-orders Silver is deliberately absent: it holds its own limit-1
    # `market_orders` pool. killmails Gold is absent too — it joins `heavy`
    # PROVISIONALLY (deploy/dagster.yaml), pending measurement, so dropping it is a
    # legitimate change that must not fail here.
    for shared in (
        market_orders_snapshot_gold,
        market_orders_changes_gold,
        market_orders_events_gold,
    ):
        assert shared.op.pool == market_history_gold.op.pool, (
            f"{shared.op.name} left the shared heavy memory budget"
        )


def test_gold_partition_start_comes_from_config() -> None:
    """The valid Gold keys derive from the dataset YAML's served_start.

    Asserted against the resolver's own output, not a literal: the test must
    fail if ``market_history.py`` stops routing through
    :func:`resolve_partition_starts` and hardcodes a start date instead.
    """
    resolved = resolve_partition_starts(DATASET)
    assert resolved.gold is not None
    assert gold_partitions.start.strftime("%Y-%m-%d") == resolved.gold
