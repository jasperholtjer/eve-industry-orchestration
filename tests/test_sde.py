"""Tests for the build-versioned unified SDE assets and sensors (ADR-0032)."""

from __future__ import annotations

import dagster as dg
import pytest

from eve_industry_orchestration.defs import sde, sensor_util
from eve_industry_orchestration.defs.config import sde_entities, sde_gold_derivatives
from eve_industry_orchestration.defs.sde import (
    sde_changelog_gold,
    sde_industry_facilities_gold,
    sde_industry_hubs_gold,
    sde_industry_products_gold,
    sde_silver,
    sde_snapshot_gold,
)
from eve_industry_orchestration.defs.sensors import (
    sde_build_discovery_sensor,
    sde_gold_sensor,
)

DATASET = "sde"
BUILDS = "100:2025-09-18,200:2025-10-01"


def _ingest_build(corpus, build: int) -> None:
    corpus.run(
        dg.build_asset_context(),
        "ingest",
        "--dataset",
        DATASET,
        "--build",
        str(build),
        "--sink-path",
        corpus.sink_path,
    )


def _build_changelog(corpus, build: int) -> None:
    corpus.run(
        dg.build_asset_context(),
        "gold",
        "build",
        "--dataset",
        DATASET,
        "--derivative",
        sde.CHANGELOG_DERIVATIVE,
        "--build",
        str(build),
        "--sink-path",
        corpus.sink_path,
    )


def _instance_with_builds(*builds: int) -> dg.DagsterInstance:
    instance = dg.DagsterInstance.ephemeral()
    instance.add_dynamic_partitions(sde.build_partitions.name, [str(b) for b in builds])
    return instance


# --- config ----------------------------------------------------------------


def test_sde_entities_from_config() -> None:
    assert sde_entities(DATASET) == ["types", "groups", "mapRegions"]


def test_sde_gold_derivatives_from_config() -> None:
    derivatives = sde_gold_derivatives(DATASET)
    assert [(d.name, d.shape) for d in derivatives] == [
        ("sde-changelog", "entity-changelog"),
        ("sde-snapshot", "entity-snapshot"),
        ("sde-industry-products", "industry-products"),
        ("sde-industry-facilities", "industry-facilities"),
        ("sde-industry-hubs", "industry-hubs"),
    ]


# --- Silver asset (one atomic partition per build) -------------------------


def test_silver_materialises_one_partition_for_a_build(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FAKE_SDE_BUILDS", BUILDS)
    instance = _instance_with_builds(100)

    result = dg.materialize(
        [sde_silver],
        partition_key="100",
        instance=instance,
        resources={"corpus": corpus},
    )

    assert result.success
    materialised = {
        e.asset_key.to_user_string() for e in result.get_asset_materialization_events()
    }
    assert materialised == {"sde_silver"}


# --- snapshot Gold (latest-only, non-partitioned) --------------------------


def test_snapshot_materialises_against_latest(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FAKE_SDE_BUILDS", BUILDS)
    instance = _instance_with_builds(100)
    _ingest_build(corpus, 100)

    result = dg.materialize(
        [sde_snapshot_gold],
        instance=instance,
        resources={"corpus": corpus},
    )

    assert result.success
    events = result.get_asset_materialization_events()
    assert len(events) == 1
    assert events[0].asset_key.to_user_string() == "sde_snapshot"


def test_snapshot_skips_when_no_silver_committed(corpus) -> None:
    # The schedule may fire on a cold corpus: no committed Silver → built False,
    # but the asset still materialises (a clean skip, not a failure).
    instance = dg.DagsterInstance.ephemeral()

    result = dg.materialize(
        [sde_snapshot_gold],
        instance=instance,
        resources={"corpus": corpus},
    )

    assert result.success
    events = result.get_asset_materialization_events()
    assert len(events) == 1
    assert events[0].materialization.metadata["built"].value is False


# --- industry-products Gold (latest-only, non-partitioned; ADR-0044) --------


def test_industry_products_materialises_against_latest(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FAKE_SDE_BUILDS", BUILDS)
    instance = _instance_with_builds(100)
    _ingest_build(corpus, 100)

    result = dg.materialize(
        [sde_industry_products_gold],
        instance=instance,
        resources={"corpus": corpus},
    )

    assert result.success
    events = result.get_asset_materialization_events()
    assert len(events) == 1
    assert events[0].asset_key.to_user_string() == "sde_industry_products"


def test_industry_products_skips_when_no_silver_committed(corpus) -> None:
    instance = dg.DagsterInstance.ephemeral()

    result = dg.materialize(
        [sde_industry_products_gold],
        instance=instance,
        resources={"corpus": corpus},
    )

    assert result.success
    events = result.get_asset_materialization_events()
    assert len(events) == 1
    assert events[0].materialization.metadata["built"].value is False


# --- industry-facilities + industry-hubs Gold (latest-only; ADR-0056) -------


@pytest.mark.parametrize(
    ("asset", "asset_key"),
    [
        (sde_industry_facilities_gold, "sde_industry_facilities"),
        (sde_industry_hubs_gold, "sde_industry_hubs"),
    ],
)
def test_industry_facility_assets_materialise_against_latest(
    corpus,
    monkeypatch: pytest.MonkeyPatch,
    asset: dg.AssetsDefinition,
    asset_key: str,
) -> None:
    monkeypatch.setenv("FAKE_SDE_BUILDS", BUILDS)
    instance = _instance_with_builds(100)
    _ingest_build(corpus, 100)

    result = dg.materialize(
        [asset],
        instance=instance,
        resources={"corpus": corpus},
    )

    assert result.success
    events = result.get_asset_materialization_events()
    assert len(events) == 1
    assert events[0].asset_key.to_user_string() == asset_key
    assert events[0].materialization.metadata["row_count"].value == 1


@pytest.mark.parametrize(
    "asset",
    [sde_industry_facilities_gold, sde_industry_hubs_gold],
)
def test_industry_facility_assets_skip_when_no_silver_committed(
    corpus, asset: dg.AssetsDefinition
) -> None:
    instance = dg.DagsterInstance.ephemeral()

    result = dg.materialize(
        [asset],
        instance=instance,
        resources={"corpus": corpus},
    )

    assert result.success
    events = result.get_asset_materialization_events()
    assert len(events) == 1
    assert events[0].materialization.metadata["built"].value is False


# --- changelog Gold (baseline skip) ----------------------------------------


def test_changelog_baseline_build_writes_nothing(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The first build has no committed predecessor Silver → no changelog
    # partition; the output is left Missing (ADR-0032).
    monkeypatch.setenv("FAKE_SDE_BUILDS", BUILDS)
    instance = _instance_with_builds(100)
    _ingest_build(corpus, 100)

    result = dg.materialize(
        [sde_changelog_gold],
        partition_key="100",
        instance=instance,
        resources={"corpus": corpus},
    )

    assert result.success
    assert result.get_asset_materialization_events() == []


def test_changelog_with_predecessor_materialises_one_partition(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FAKE_SDE_BUILDS", BUILDS)
    instance = _instance_with_builds(100, 200)
    _ingest_build(corpus, 100)
    _ingest_build(corpus, 200)

    result = dg.materialize(
        [sde_changelog_gold],
        partition_key="200",
        instance=instance,
        resources={"corpus": corpus},
    )

    assert result.success
    events = result.get_asset_materialization_events()
    assert len(events) == 1
    assert events[0].asset_key.to_user_string() == "sde_changelog"


# --- build-discovery sensor ------------------------------------------------


def test_build_discovery_sensor_registers_and_requests(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A build discovered on this tick is registered and requested on the same
    # tick: Dagster evaluates run requests with the additions applied.
    monkeypatch.setenv("FAKE_SDE_BUILDS", BUILDS)
    instance = dg.DagsterInstance.ephemeral()
    context = dg.build_sensor_context(resources={"corpus": corpus}, instance=instance)

    result = sde_build_discovery_sensor(context)

    by_partition = {rr.partition_key: rr for rr in result.run_requests}
    assert sorted(by_partition) == ["100", "200"]
    assert by_partition["100"].run_key.startswith("sde-silver-100-")
    # New build keys are registered as dynamic partitions in the same tick.
    added = {
        key for req in result.dynamic_partitions_requests for key in req.partition_keys
    }
    assert added == {"100", "200"}


def test_build_discovery_sensor_skips_builds_with_committed_silver(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Readiness is the run-state, not the partition store: an ingested build
    # drops out, a registered-but-never-ingested one does not.
    monkeypatch.setenv("FAKE_SDE_BUILDS", BUILDS)
    _ingest_build(corpus, 100)
    instance = _instance_with_builds(100, 200)
    context = dg.build_sensor_context(resources={"corpus": corpus}, instance=instance)

    result = sde_build_discovery_sensor(context)

    assert [rr.partition_key for rr in result.run_requests] == ["200"]
    assert result.dynamic_partitions_requests == []


def test_build_discovery_sensor_retries_a_registered_build_that_never_ingested(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The hole this row exists for. Keyed on "not yet registered", a failed
    # ingest was never asked for again and the gap became permanent.
    monkeypatch.setenv("FAKE_SDE_BUILDS", BUILDS)
    instance = _instance_with_builds(100, 200)

    first = sde_build_discovery_sensor(
        dg.build_sensor_context(resources={"corpus": corpus}, instance=instance)
    )
    second = sde_build_discovery_sensor(
        dg.build_sensor_context(
            resources={"corpus": corpus}, instance=instance, cursor=first.cursor
        )
    )

    assert [rr.partition_key for rr in first.run_requests] == ["100", "200"]
    assert [rr.partition_key for rr in second.run_requests] == ["100", "200"]
    first_keys = {rr.run_key for rr in first.run_requests}
    assert first_keys.isdisjoint({rr.run_key for rr in second.run_requests})


def test_build_discovery_sensor_orders_builds_numerically(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Build keys are numbers; lexically "100" sorts before "99" and the per-tick
    # cap would then take the wrong build.
    monkeypatch.setenv("FAKE_SDE_BUILDS", "99:2025-09-02,100:2025-09-03")
    monkeypatch.setattr(sensor_util, "MAX_PARTITIONS_PER_TICK", 1)
    instance = dg.DagsterInstance.ephemeral()
    context = dg.build_sensor_context(resources={"corpus": corpus}, instance=instance)

    result = sde_build_discovery_sensor(context)

    assert [rr.partition_key for rr in result.run_requests] == ["99"]


def test_build_discovery_sensor_logs_only_the_builds_it_requests(
    corpus, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    # After a run-state reset the eligible set is the whole build history; logging
    # it would emit hundreds of lines an hour for builds this tick never asks for.
    monkeypatch.setenv("FAKE_SDE_BUILDS", "100:2025-09-18,200:2025-10-01")
    monkeypatch.setattr(sensor_util, "MAX_PARTITIONS_PER_TICK", 1)
    instance = dg.DagsterInstance.ephemeral()
    context = dg.build_sensor_context(resources={"corpus": corpus}, instance=instance)

    result = sde_build_discovery_sensor(context)

    assert [rr.partition_key for rr in result.run_requests] == ["100"]
    logged = capsys.readouterr().err
    assert "build 100 (released 2025-09-18)" in logged
    assert "build 200" not in logged


def test_build_discovery_sensor_survives_a_junk_partition_key(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The dynamic-partitions store is hand-editable; every ordering path calls
    # int() on its keys, so one junk key must be skipped, not raise.
    monkeypatch.setenv("FAKE_SDE_BUILDS", BUILDS)
    instance = dg.DagsterInstance.ephemeral()
    instance.add_dynamic_partitions(sde.build_partitions.name, ["oops"])
    context = dg.build_sensor_context(resources={"corpus": corpus}, instance=instance)

    result = sde_build_discovery_sensor(context)

    assert sorted(rr.partition_key for rr in result.run_requests) == ["100", "200"]


def test_gold_sensor_survives_a_junk_partition_key(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FAKE_SDE_BUILDS", BUILDS)
    for build in (100, 200):
        _ingest_build(corpus, build)
    instance = _instance_with_builds(100, 200)
    instance.add_dynamic_partitions(sde.build_partitions.name, ["oops"])
    context = dg.build_sensor_context(resources={"corpus": corpus}, instance=instance)

    result = sde_gold_sensor(context)

    assert [rr.partition_key for rr in result.run_requests] == ["200"]


def test_build_discovery_sensor_tags_the_release_date(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FAKE_SDE_BUILDS", BUILDS)
    instance = dg.DagsterInstance.ephemeral()
    context = dg.build_sensor_context(resources={"corpus": corpus}, instance=instance)

    result = sde_build_discovery_sensor(context)

    tags = {
        rr.partition_key: rr.tags.get(sde.RELEASE_DATE_TAG)
        for rr in result.run_requests
    }
    assert tags == {"100": "2025-09-18", "200": "2025-10-01"}


def test_silver_records_the_release_date_tag_as_metadata(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FAKE_SDE_BUILDS", BUILDS)
    instance = _instance_with_builds(100)

    result = dg.materialize(
        [sde_silver],
        partition_key="100",
        instance=instance,
        resources={"corpus": corpus},
        tags={sde.RELEASE_DATE_TAG: "2025-09-18"},
    )

    (event,) = result.get_asset_materialization_events()
    metadata = event.step_materialization_data.materialization.metadata
    assert metadata["listed_release_date"].value == "2025-09-18"


def test_silver_omits_the_release_date_metadata_without_the_tag(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A manual backfill carries no tag; it must materialise without a placeholder.
    monkeypatch.setenv("FAKE_SDE_BUILDS", BUILDS)
    instance = _instance_with_builds(100)

    result = dg.materialize(
        [sde_silver],
        partition_key="100",
        instance=instance,
        resources={"corpus": corpus},
    )

    (event,) = result.get_asset_materialization_events()
    metadata = event.step_materialization_data.materialization.metadata
    assert "listed_release_date" not in metadata


# --- Gold readiness sensor -------------------------------------------------


def test_gold_sensor_requests_outstanding_builds_only(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The snapshot is non-partitioned + schedule-driven (ADR-0032), so the gold
    # sensor only requests the build-partitioned changelog — and not for build
    # 100, which is the baseline the binary skips.
    monkeypatch.setenv("FAKE_SDE_BUILDS", BUILDS)
    _ingest_build(corpus, 100)
    _ingest_build(corpus, 200)
    instance = _instance_with_builds(100, 200)
    context = dg.build_sensor_context(resources={"corpus": corpus}, instance=instance)

    result = sde_gold_sensor(context)

    assert [rr.partition_key for rr in result.run_requests] == ["200"]
    assert result.run_requests[0].run_key.startswith("sde-changelog-200-")


def test_gold_sensor_skips_builds_whose_changelog_is_committed(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Readiness is the run-state difference, not the sensor's memory: a build
    # whose changelog Gold is committed drops out of the outstanding set.
    monkeypatch.setenv("FAKE_SDE_BUILDS", BUILDS)
    _ingest_build(corpus, 100)
    _ingest_build(corpus, 200)
    _build_changelog(corpus, 200)
    instance = _instance_with_builds(100, 200)
    context = dg.build_sensor_context(resources={"corpus": corpus}, instance=instance)

    result = sde_gold_sensor(context)

    assert result.run_requests == []


def test_gold_sensor_no_silver_yields_no_requests(corpus) -> None:
    instance = dg.DagsterInstance.ephemeral()
    context = dg.build_sensor_context(resources={"corpus": corpus}, instance=instance)

    result = sde_gold_sensor(context)

    assert result.run_requests == []


def test_gold_sensor_single_build_is_baseline(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A cold corpus with one committed build has nothing to diff against, and the
    # binary would report `skipped`. Do not queue that run.
    monkeypatch.setenv("FAKE_SDE_BUILDS", BUILDS)
    _ingest_build(corpus, 100)
    instance = _instance_with_builds(100)
    context = dg.build_sensor_context(resources={"corpus": corpus}, instance=instance)

    result = sde_gold_sensor(context)

    assert result.run_requests == []


def test_gold_sensor_backfilled_build_unblocks_the_old_baseline(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Ingesting below the current baseline gives the old baseline a predecessor,
    # so it becomes outstanding — the rule tracks run-state, not first sighting.
    monkeypatch.setenv("FAKE_SDE_BUILDS", "50:2025-09-01," + BUILDS)
    _ingest_build(corpus, 200)
    instance = _instance_with_builds(50, 100, 200)
    context = dg.build_sensor_context(resources={"corpus": corpus}, instance=instance)
    assert sde_gold_sensor(context).run_requests == []

    _ingest_build(corpus, 50)
    context = dg.build_sensor_context(resources={"corpus": corpus}, instance=instance)

    result = sde_gold_sensor(context)

    assert [rr.partition_key for rr in result.run_requests] == ["200"]


def test_gold_sensor_orders_builds_numerically(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Build keys are numbers; a lexical sort would put "100" before "99" and the
    # per-tick cap would then take the wrong builds.
    monkeypatch.setenv("FAKE_SDE_BUILDS", "98:2025-09-01,99:2025-09-02,100:2025-09-03")
    monkeypatch.setattr(sensor_util, "MAX_PARTITIONS_PER_TICK", 1)
    for build in (98, 99, 100):
        _ingest_build(corpus, build)
    instance = _instance_with_builds(98, 99, 100)
    context = dg.build_sensor_context(resources={"corpus": corpus}, instance=instance)

    result = sde_gold_sensor(context)

    assert [rr.partition_key for rr in result.run_requests] == ["99"]


def test_gold_sensor_drains_a_backlog_larger_than_the_cap(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The regression this row exists for. The cap must bound the work *left*: a
    # sensor that caps every committed build and keys on a static run_key stalls
    # on the first cap-many builds and never reaches the rest.
    cap = sensor_util.MAX_PARTITIONS_PER_TICK
    builds = list(range(100, 100 + cap + 2))
    monkeypatch.setenv(
        "FAKE_SDE_BUILDS",
        ",".join(f"{b}:2025-09-{(i % 28) + 1:02d}" for i, b in enumerate(builds)),
    )
    for build in builds:
        _ingest_build(corpus, build)
    instance = _instance_with_builds(*builds)

    first = sde_gold_sensor(
        dg.build_sensor_context(resources={"corpus": corpus}, instance=instance)
    )
    # Every build but the baseline is outstanding; the cap takes the oldest.
    assert [rr.partition_key for rr in first.run_requests] == [
        str(b) for b in builds[1 : 1 + cap]
    ]

    for build in builds[1 : 1 + cap]:
        _build_changelog(corpus, build)

    second = sde_gold_sensor(
        dg.build_sensor_context(
            resources={"corpus": corpus}, instance=instance, cursor=first.cursor
        )
    )

    assert [rr.partition_key for rr in second.run_requests] == [str(builds[-1])]


def test_gold_sensor_retries_a_build_that_did_not_commit(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A changelog run can fail, or finish green without materialising
    # (output_required=False). The rotating run_key is what lets the next tick
    # ask again; a static one is deduped by Dagster for good.
    monkeypatch.setenv("FAKE_SDE_BUILDS", BUILDS)
    _ingest_build(corpus, 100)
    _ingest_build(corpus, 200)
    instance = _instance_with_builds(100, 200)

    first = sde_gold_sensor(
        dg.build_sensor_context(resources={"corpus": corpus}, instance=instance)
    )
    second = sde_gold_sensor(
        dg.build_sensor_context(
            resources={"corpus": corpus}, instance=instance, cursor=first.cursor
        )
    )

    (first_rr,) = first.run_requests
    (second_rr,) = second.run_requests
    assert first_rr.partition_key == second_rr.partition_key == "200"
    assert first_rr.run_key != second_rr.run_key


def test_gold_sensor_skips_a_build_already_in_flight(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The rotating run_key must not put a second corpus writer on a build whose
    # prior run has not settled.
    monkeypatch.setenv("FAKE_SDE_BUILDS", BUILDS)
    monkeypatch.setattr(
        sensor_util, "_in_flight_partitions", lambda context, asset_key: {"200"}
    )
    _ingest_build(corpus, 100)
    _ingest_build(corpus, 200)
    instance = _instance_with_builds(100, 200)
    context = dg.build_sensor_context(resources={"corpus": corpus}, instance=instance)

    result = sde_gold_sensor(context)

    assert result.run_requests == []


# --- Gold sensor: the stale term (a changelog diffed across a hole) --------


def test_gold_sensor_reproposes_a_changelog_diffed_across_a_hole(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    # 300 committed while 200 was still ingesting, so its changelog was diffed
    # against 100 and the 200->300 link never existed. Committing 200 afterwards
    # makes 300 stale: the binary would pick a different predecessor now.
    monkeypatch.setenv(
        "FAKE_SDE_BUILDS", "100:2025-09-01,200:2025-09-02,300:2025-09-03"
    )
    _ingest_build(corpus, 100)
    _ingest_build(corpus, 300)
    _build_changelog(corpus, 300)
    instance = _instance_with_builds(100, 200, 300)
    assert (
        sde_gold_sensor(
            dg.build_sensor_context(resources={"corpus": corpus}, instance=instance)
        ).run_requests
        == []
    )

    _ingest_build(corpus, 200)
    context = dg.build_sensor_context(resources={"corpus": corpus}, instance=instance)

    result = sde_gold_sensor(context)

    assert [rr.partition_key for rr in result.run_requests] == ["200", "300"]


def test_gold_sensor_leaves_a_changelog_whose_predecessor_is_unchanged(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Every lower Silver predates the changelog, so it is not stale and the
    # outstanding set stays empty — the stale term must not rebuild everything.
    monkeypatch.setenv(
        "FAKE_SDE_BUILDS", "100:2025-09-01,200:2025-09-02,300:2025-09-03"
    )
    for build in (100, 200, 300):
        _ingest_build(corpus, build)
    _build_changelog(corpus, 200)
    _build_changelog(corpus, 300)
    instance = _instance_with_builds(100, 200, 300)
    context = dg.build_sensor_context(resources={"corpus": corpus}, instance=instance)

    assert sde_gold_sensor(context).run_requests == []


def test_gold_sensor_never_reports_the_baseline_stale(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A build below the baseline arriving late gives the old baseline (100) a
    # predecessor, so 100 becomes outstanding by the ordinary term. 50 is the new
    # baseline and has no lower Silver, so it can never be stale; 200's nearest
    # lower Silver (100) is unchanged, so it is not stale either.
    monkeypatch.setenv("FAKE_SDE_BUILDS", "50:2025-08-01,100:2025-09-01,200:2025-09-02")
    _ingest_build(corpus, 100)
    _ingest_build(corpus, 200)
    _build_changelog(corpus, 200)
    instance = _instance_with_builds(50, 100, 200)

    _ingest_build(corpus, 50)
    context = dg.build_sensor_context(resources={"corpus": corpus}, instance=instance)

    result = sde_gold_sensor(context)

    assert [rr.partition_key for rr in result.run_requests] == ["100"]


def test_gold_sensor_stale_term_does_not_cascade(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Re-ingesting a build far below flags only the changelog whose *nearest*
    # lower Silver actually changed. A superset ("any lower Silver committed
    # after this Gold") would rebuild 300 and 400 as well.
    monkeypatch.setenv(
        "FAKE_SDE_BUILDS",
        "100:2025-09-01,200:2025-09-02,300:2025-09-03,400:2025-09-04",
    )
    for build in (100, 200, 300, 400):
        _ingest_build(corpus, build)
    for build in (200, 300, 400):
        _build_changelog(corpus, build)
    instance = _instance_with_builds(100, 200, 300, 400)

    _ingest_build(corpus, 100)
    context = dg.build_sensor_context(resources={"corpus": corpus}, instance=instance)

    result = sde_gold_sensor(context)

    assert [rr.partition_key for rr in result.run_requests] == ["200"]


# --- Gold sensor: deferral on a lower Silver run in flight -----------------


def _defer_silver(monkeypatch: pytest.MonkeyPatch, *in_flight: str) -> None:
    """Reports ``in_flight`` for `sde_silver` and nothing for the changelog."""
    monkeypatch.setattr(
        sensor_util,
        "_in_flight_partitions",
        lambda context, asset_key: (
            set(in_flight) if asset_key == sde.sde_silver.key else set()
        ),
    )


def test_gold_sensor_defers_a_build_with_a_lower_silver_in_flight(
    corpus, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    # 250 is ingesting; building 300 now would diff it against 200 and produce
    # exactly the hole this row exists to prevent.
    monkeypatch.setenv(
        "FAKE_SDE_BUILDS",
        "100:2025-09-01,200:2025-09-02,250:2025-09-03,300:2025-09-04",
    )
    for build in (100, 200, 300):
        _ingest_build(corpus, build)
    instance = _instance_with_builds(100, 200, 250, 300)
    _defer_silver(monkeypatch, "250")
    context = dg.build_sensor_context(resources={"corpus": corpus}, instance=instance)

    result = sde_gold_sensor(context)

    assert [rr.partition_key for rr in result.run_requests] == ["200"]
    assert "deferring 300" in capsys.readouterr().err


def test_gold_sensor_requests_a_deferred_build_once_the_run_settles(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Deferral is bounded by a run's lifetime: the build stays outstanding.
    monkeypatch.setenv(
        "FAKE_SDE_BUILDS",
        "100:2025-09-01,200:2025-09-02,250:2025-09-03,300:2025-09-04",
    )
    for build in (100, 200, 300):
        _ingest_build(corpus, build)
    instance = _instance_with_builds(100, 200, 250, 300)
    context = dg.build_sensor_context(resources={"corpus": corpus}, instance=instance)

    result = sde_gold_sensor(context)

    assert [rr.partition_key for rr in result.run_requests] == ["200", "300"]


def test_gold_sensor_defers_a_build_whose_own_silver_is_in_flight(
    corpus, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    # A re-ingest of 300 rewrites the very Silver partition 300's changelog diffs
    # from. The per-asset in-flight guard only covers `sde_changelog_gold`, so the
    # deferral has to close it: pred(B) < S <= B, not S < B.
    monkeypatch.setenv(
        "FAKE_SDE_BUILDS", "100:2025-09-01,200:2025-09-02,300:2025-09-03"
    )
    for build in (100, 200, 300):
        _ingest_build(corpus, build)
    instance = _instance_with_builds(100, 200, 300)
    _defer_silver(monkeypatch, "300")
    context = dg.build_sensor_context(resources={"corpus": corpus}, instance=instance)

    result = sde_gold_sensor(context)

    assert [rr.partition_key for rr in result.run_requests] == ["200"]
    assert "deferring 300" in capsys.readouterr().err


def test_gold_sensor_ignores_a_higher_silver_in_flight(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Only a *lower* build can change the predecessor; a higher one is irrelevant.
    monkeypatch.setenv(
        "FAKE_SDE_BUILDS",
        "100:2025-09-01,200:2025-09-02,300:2025-09-03,400:2025-09-04",
    )
    for build in (100, 200, 300):
        _ingest_build(corpus, build)
    instance = _instance_with_builds(100, 200, 300, 400)
    _defer_silver(monkeypatch, "400")
    context = dg.build_sensor_context(resources={"corpus": corpus}, instance=instance)

    result = sde_gold_sensor(context)

    assert [rr.partition_key for rr in result.run_requests] == ["200", "300"]


def test_gold_sensor_is_not_stalled_by_a_build_that_never_commits(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The honest shape of a build that never commits: discovery re-requests 200
    # every tick, so it is *also* in flight (often QUEUED behind the
    # `everef_download` pool) on a large share of Gold ticks. Under the broad rule
    # that tick requested nothing at all. Narrowed, only 300 — whose predecessor
    # 200 would actually change — waits; 400's predecessor (300) is unaffected, so
    # the changelog stream keeps moving.
    monkeypatch.setenv(
        "FAKE_SDE_BUILDS",
        "100:2025-09-01,200:2025-09-02,300:2025-09-03,400:2025-09-04",
    )
    for build in (100, 300, 400):
        _ingest_build(corpus, build)
    instance = _instance_with_builds(100, 200, 300, 400)
    _defer_silver(monkeypatch, "200")
    context = dg.build_sensor_context(resources={"corpus": corpus}, instance=instance)

    result = sde_gold_sensor(context)

    assert [rr.partition_key for rr in result.run_requests] == ["400"]


def test_gold_sensor_defers_only_across_the_current_predecessor(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The narrowed rule: an in-flight run defers a build only when it lands
    # strictly between that build and its current nearest lower committed Silver.
    # 150 sits between 100 and 200, so 200 waits; it is at-or-below 300's
    # predecessor (200), so it cannot become 300's predecessor and 300 is asked
    # for on the same tick.
    monkeypatch.setenv(
        "FAKE_SDE_BUILDS",
        "100:2025-09-01,150:2025-09-02,200:2025-09-03,300:2025-09-04",
    )
    for build in (100, 200, 300):
        _ingest_build(corpus, build)
    instance = _instance_with_builds(100, 150, 200, 300)
    _defer_silver(monkeypatch, "150")
    context = dg.build_sensor_context(resources={"corpus": corpus}, instance=instance)

    result = sde_gold_sensor(context)

    assert [rr.partition_key for rr in result.run_requests] == ["300"]


# --- multi-derivative ambiguity (matches the binary) -----------------------


def test_sde_gold_build_without_derivative_fails(
    corpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FAKE_SDE_BUILDS", BUILDS)
    _ingest_build(corpus, 100)
    with pytest.raises(dg.Failure):
        corpus.run(
            dg.build_asset_context(),
            "gold",
            "build",
            "--dataset",
            DATASET,
            "--build",
            "100",
            "--sink-path",
            corpus.sink_path,
        )
