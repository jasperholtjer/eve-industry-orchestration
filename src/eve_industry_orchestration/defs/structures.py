"""structures: one daily Silver source, two Gold derivatives (corpus ADR-0057/0062).

EVE Ref publishes ~4 full-population snapshots per day of the public structure
universe under per-date subdirectories (the ``hourly-folder`` layout, like
market-orders); a partition is one DAY, folded last-write-wins per
``structure_id`` into source-faithful Silver.

Both Gold derivatives are **backfillable historical matrices**, so both get a
daily-partitioned asset driven by a ``ready-dates`` sensor:

- ``structures-snapshot`` — the map's per-structure dimension as-of the date. A
  pure function of the target day's Silver (no window), so every day is
  independently reproducible.
- ``structure-population-history`` — the predict region/system covariate: class
  counts plus presence-diff churn over a 30-day ``closed="left"`` look-back, with
  the coverage gate the binary owns.

Both builds read the ``sde-types`` / ``sde-groups`` Gold snapshots to resolve
``type_id → facility_class`` and stamp that build into ``_INDEX.json``'s
``dependency_fingerprint``, so the SDE snapshot Gold is a real upstream (like
``news-entity-mentions``), not lineage decoration. The binary fails loud when the
SDE Gold is not ``_DONE``-sealed.

Each asset is a thin shim over the ``corpus`` binary; the binary owns the compute,
the coverage gate, the SDE resolution, and the ``parquet + _INDEX.json + _DONE``
contract. Partition starts come from the corpus dataset config (see
:mod:`config`), never hardcoded — Silver from 2024-03-31 (the first v2 archive,
ADR-0062), the covariate from 2024-04-30 (the first day with its 30-day
reference day inside the served window).

**Gold verify keys on the derivative name, not the dataset.** ``corpus gold
build`` writes under ``gold/<derivative>/...`` and ``corpus verify --tier gold``
resolves ``gold/<--dataset>/...``, so Gold verify passes the *derivative* name as
``--dataset``. Silver verify still uses the dataset name.
"""

from collections.abc import Iterator

import dagster as dg

from eve_industry_orchestration.defs import sde
from eve_industry_orchestration.defs.config import resolve_partition_starts
from eve_industry_orchestration.defs.corpus_resource import CorpusResource, date_key

DATASET = "structures"

SNAPSHOT_DERIVATIVE = "structures-snapshot"
POPULATION_DERIVATIVE = "structure-population-history"

# Silver is shared by both derivatives; its derived start is the earliest preload
# (the covariate's 30d window before served_start 2024-04-30 → 2024-03-31), which
# coincides with the dataset YAML's silver.served_start floor (ADR-0027): the
# first archive carrying the v2 payload at all. Each derivative's Gold start is
# resolved separately — the dimension serves from 2024-03-31, the covariate from
# 2024-04-30.
_snapshot_starts = resolve_partition_starts(DATASET, SNAPSHOT_DERIVATIVE)
_population_starts = resolve_partition_starts(DATASET, POPULATION_DERIVATIVE)
if _snapshot_starts.gold is None or _population_starts.gold is None:
    raise ValueError(
        f"{DATASET} resolved no Gold served_start; both structures derivatives "
        "are windowed historical matrices and must declare one"
    )
silver_partitions = dg.DailyPartitionsDefinition(start_date=_snapshot_starts.silver)
snapshot_gold_partitions = dg.DailyPartitionsDefinition(
    start_date=_snapshot_starts.gold
)
population_gold_partitions = dg.DailyPartitionsDefinition(
    start_date=_population_starts.gold
)

_SILVER_POOL = "everef_download"


@dg.asset(
    partitions_def=silver_partitions,
    group_name="structures",
    kinds={"corpus"},
    pool=_SILVER_POOL,
    # An interior upstream-gap day (EVE Ref published nothing, ADR-0028) skips:
    # corpus exits 0 with status "skipped" and writes no partition, so the asset
    # must complete without materialising — the partition stays Missing.
    #
    # A day at the publication frontier — the date folder lists, but none of its
    # members carries the declared `member_suffix: .v2.json.bz2` yet — reports
    # status "incomplete" instead, on exit 0 and again without a partition.
    # Unlike "skipped" this is not permanent: the day is expected to settle.
    output_required=False,
)
def structures_silver(
    context: dg.AssetExecutionContext, corpus: CorpusResource
) -> Iterator[dg.MaterializeResult | dg.AssetObservation]:
    """Silver partition: fold one day's ~4 population snapshots, then verify.

    Two distinct non-materialising outcomes are left Missing, each with its own
    ``AssetObservation`` reason, instead of falling through to a verify that
    would 404 on the partition corpus deliberately did not write:

    - ``status: skipped`` — a genuinely-absent upstream day (permanent,
      ADR-0028): EVE Ref never published it and never will.
    - ``status: incomplete`` — a day at the publication frontier (retryable).
      This dataset declares ``member_suffix: .v2.json.bz2``, so a listed date
      folder holding no member with that suffix reaches corpus's
      ``FolderEmptiedByDeclaredSuffix`` → ``DeclaredSuffixVerdict::
      PublicationFrontier`` path (ADR-0064) rather than a permanent skip. The
      availability sensor rotates its ``run_key`` per tick (see
      :mod:`sensor_util`), so the date is re-proposed and materialises once
      upstream settles, rather than red-looping the run every tick.
    """
    date = context.partition_key
    status = corpus.run(
        context,
        "ingest",
        "--dataset",
        DATASET,
        "--date",
        date,
        "--sink-path",
        corpus.sink_path,
    )
    if status is not None and status.get("status") == "skipped":
        context.log.info(
            "structures %s: upstream absent, leaving partition missing", date
        )
        yield dg.AssetObservation(
            asset_key=context.asset_key,
            partition=date,
            metadata={
                "skip_reason": "upstream_absent",
                "detail": str(status.get("reason", "")),
            },
        )
        return
    if status is not None and status.get("status") == "incomplete":
        context.log.info(
            "structures %s: upstream publication incomplete, leaving partition "
            "missing (retryable)",
            date,
        )
        yield dg.AssetObservation(
            asset_key=context.asset_key,
            partition=date,
            metadata={
                "skip_reason": "upstream_incomplete",
                "detail": str(status.get("reason", "")),
            },
        )
        return
    corpus.run(
        context,
        "verify",
        "--dataset",
        DATASET,
        "--date",
        date,
        "--tier",
        "silver",
        "--sink-path",
        corpus.sink_path,
    )
    # The run-state facts corpus recorded for the partition it just wrote (rows,
    # retention_class, parquet_sha256) merge over the identifying fields; the read
    # is advisory and yields {} rather than failing a completed materialisation.
    yield dg.MaterializeResult(
        metadata={"dataset": DATASET, "tier": "silver", "partition": date}
        | corpus.partition_metadata(DATASET, "silver", date_key(date))
    )


def _build_gold_asset(
    derivative: str, partitions: dg.DailyPartitionsDefinition
) -> dg.AssetsDefinition:
    """Builds a daily-partitioned Gold asset for one structures derivative.

    ``deps=`` is lineage only; the readiness sensor drives this. The SDE snapshot
    Gold is a genuine upstream — both builders resolve ``type_id →
    facility_class`` from it — so it joins the deps alongside Silver. No ``heavy``
    pool: the population is ~2–20k rows/day, so even the 30-day window is
    trivially small next to the market-history / market-orders builds that the
    pool exists to bound.
    """

    @dg.asset(
        name=f"{derivative.replace('-', '_')}_gold",
        partitions_def=partitions,
        deps=[structures_silver, sde.sde_snapshot_gold],
        group_name="structures",
        kinds={"corpus"},
        # A target day whose Silver is an upstream gap can never build a Gold row
        # (ADR-0029); corpus reports "skipped", so the asset completes without
        # materialising — the partition stays Missing rather than failing.
        output_required=False,
    )
    def _gold(
        context: dg.AssetExecutionContext, corpus: CorpusResource
    ) -> Iterator[dg.MaterializeResult | dg.AssetObservation]:
        date = context.partition_key
        status = corpus.run(
            context,
            "gold",
            "build",
            "--dataset",
            DATASET,
            "--derivative",
            derivative,
            "--date",
            date,
            "--sink-path",
            corpus.sink_path,
        )
        if status is not None and status.get("status") == "skipped":
            context.log.info(
                "%s %s: target silver is an upstream gap, leaving partition missing",
                derivative,
                date,
            )
            yield dg.AssetObservation(
                asset_key=context.asset_key,
                partition=date,
                metadata={
                    "skip_reason": "upstream_gap",
                    "detail": str(status.get("reason", "")),
                },
            )
            return
        corpus.run(
            context,
            "verify",
            "--dataset",
            derivative,
            "--date",
            date,
            "--tier",
            "gold",
            "--sink-path",
            corpus.sink_path,
        )
        # A multi-derivative Gold row is keyed in run-state on the derivative
        # (its own gold/<derivative>/ tree), not on the dataset name.
        yield dg.MaterializeResult(
            metadata={
                "dataset": DATASET,
                "derivative": derivative,
                "tier": "gold",
                "partition": date,
            }
            | corpus.partition_metadata(derivative, "gold", date_key(date))
        )

    return _gold


# One module-level asset per derivative so auto-discovery registers both.
structures_snapshot_gold = _build_gold_asset(
    SNAPSHOT_DERIVATIVE, snapshot_gold_partitions
)
structure_population_history_gold = _build_gold_asset(
    POPULATION_DERIVATIVE, population_gold_partitions
)
