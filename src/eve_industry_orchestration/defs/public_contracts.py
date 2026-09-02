"""public-contracts: the history tier over EVE Ref's public-contract archives.

One day-partitioned Silver asset and nothing else. The upstream publishes ~47
snapshots a day in per-date folders (``.v2.tar.bz2``, corpus ADR-0068); a
partition is one DAY, whose snapshot archives the binary merges into one
source-faithful Silver stream at snapshot grain (ADR-0068 decision 5 — one
table, one ``data.parquet`` per day). No Bronze is written: the archives are
streamed and discarded (ADR-0067), so there is no Bronze tier to wire.

**Why the Silver start comes from the coverage floor, not from a derivative.**
All four Gold derivatives fold one day of Silver into one day of Gold and hold
no cross-day state (ADR-0068 decision 5), so none reaches back past its own
``served_start`` and the derived preload lands on the floor itself. The
``silver.served_start`` (ADR-0027) is therefore the binding anchor: the first
day of the ``.v2.tar.bz2`` era, below which the eight 2019 ``.json.gz`` days are
a different payload entirely. :func:`config.resolve_silver_start` reads that
floor, so no date literal appears here.

**Four Gold derivatives, four assets.** The 43x fold from snapshot grain to a
served shape is a Gold concern (ADR-0068 decision 5), and
``datasets/public-contracts.yaml`` now declares it as four day-partitioned
trees — ``contracts-facts``, ``contracts-item-facts``,
``contracts-item-prices`` and ``contracts-courier-rates`` — each with its own
builder, its own ``_DONE`` and its own run-state row. Each builds here under
its own ``--derivative`` and its own partitions definition; no tree's seal
stands for another's. ``contracts-courier-rates`` resolves its
``end_region_id`` against sealed ``structures-snapshot`` and SDE trees, but
those are builder-pinned reads fingerprinted into ``_INDEX.json`` (ADR-0052),
never a Dagster dependency edge.

The asset name is the derivative name, which is also the tree under ``gold/``
and the run-state key — so an asset in the Dagster list names the bytes it
produces. It is deliberately *not* the ``shape:`` beside it in the YAML
(``contract-facts``, ``courier-rates``): a shape is corpus's builder-dispatch
key, is never written to disk, and is what :func:`config._lookback_for_shape`
keys the Silver preload on — which is why that mapping is untouched by a tree
rename.

The live twin ``public-contracts-live`` (:mod:`public_contracts_live`) is a
separate dataset with a separate YAML, a current-overwrite ``current/``
partition and its own schedule. Neither tier depends on the other.
"""

from collections.abc import Iterator

import dagster as dg

from eve_industry_orchestration.defs.config import (
    resolve_partition_starts,
    resolve_silver_start,
)
from eve_industry_orchestration.defs.corpus_resource import CorpusResource, date_key

DATASET = "public-contracts"
CONTRACTS_FACTS_DERIVATIVE = "contracts-facts"
CONTRACTS_ITEM_FACTS_DERIVATIVE = "contracts-item-facts"
CONTRACTS_ITEM_PRICES_DERIVATIVE = "contracts-item-prices"
CONTRACTS_COURIER_RATES_DERIVATIVE = "contracts-courier-rates"

silver_partitions = dg.DailyPartitionsDefinition(
    start_date=resolve_silver_start(DATASET)
)

# One EVE Ref archive-set fetch per partition: the same politeness cap every
# other Silver ingest joins. Deliberately not a memory-bearing pool — no
# `/usr/bin/time -v` peak has been measured for this ingest, and CLAUDE.md
# makes membership of such a pool a measurement, never a guess. See
# deploy/dagster.yaml for the budget this does and does not count against.
_SILVER_POOL = "everef_download"


@dg.asset(
    partitions_def=silver_partitions,
    group_name="public_contracts",
    kinds={"corpus"},
    pool=_SILVER_POOL,
    # An interior day EVE Ref never published (ADR-0028) skips: corpus exits 0
    # with status "skipped" and writes no partition, so the asset must be
    # allowed to complete without materialising — the partition stays Missing
    # rather than failing the run or materialising empty.
    #
    # A day at the publication frontier — the date folder exists but the
    # `.v2.tar.bz2` has not landed yet — reports status "incomplete" instead
    # (the `PublicationFrontier` verdict corpus's declared-suffix classifier
    # reaches because this dataset declares both `member_suffix` and
    # `ignorable_member_suffixes`). Unlike "skipped", this is not permanent:
    # the day is expected to settle and is re-proposed on the next sensor tick.
    output_required=False,
)
def public_contracts_silver(
    context: dg.AssetExecutionContext, corpus: CorpusResource
) -> Iterator[dg.MaterializeResult | dg.AssetObservation]:
    """Silver partition: ingest one day's contract snapshots, then verify.

    Every date is requested the same way — the binary owns which archives a day
    holds and how they are merged, so the asset never branches on the packaging
    era or the snapshot count.

    Two distinct non-materialising outcomes are left Missing, each with its own
    ``AssetObservation`` reason, instead of letting the fall-through verify 404
    on an absent partition:

    - ``status: skipped`` — a genuinely-absent upstream day (permanent,
      ADR-0028/0029): EVE Ref never published it and never will.
    - ``status: incomplete`` — a day at the publication frontier (retryable):
      the date folder exists but the day's ``.v2.tar.bz2`` has not landed yet.
      The availability sensor rotates its ``run_key`` per tick (see
      :mod:`sensor_util`), so the date is re-proposed and picked up once
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
            "public-contracts %s: upstream absent, leaving partition missing", date
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
            "public-contracts %s: upstream publication incomplete, leaving "
            "partition missing (retryable)",
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


def _build_gold(
    context: dg.AssetExecutionContext, corpus: CorpusResource, derivative: str
) -> Iterator[dg.MaterializeResult | dg.AssetObservation]:
    """The body all four Gold assets share, differing only in ``--derivative``.

    Build, then — unless corpus reported a skipped day — Gold-tier verify and a
    ``MaterializeResult``. Nothing here inspects the day's Silver, a coverage
    ratio or a sibling tree: whether a date can be built is the binary's answer
    (ADR-0068), and this asset only reports the status it was given.
    """
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
            "%s %s: prerequisite permanently absent, leaving partition missing",
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
    # `corpus gold build` writes both the partition tree and the run-state row
    # under the *derivative* name, not the dataset, so Gold verify and the
    # run-state read both key on the derivative. Four derivatives of one dataset
    # therefore record their own facts, never each other's.
    yield dg.MaterializeResult(
        metadata={
            "dataset": DATASET,
            "derivative": derivative,
            "tier": "gold",
            "partition": date,
        }
        | corpus.partition_metadata(derivative, "gold", date_key(date))
    )


def _gold_start(derivative: str) -> str:
    """The derivative's own configured ``served_start``.

    Resolved by name rather than derived from the Silver matrix: the four trees
    share a start today, but each is free to move its own in corpus, so each
    asset resolves the one its own configuration declares.
    """
    start = resolve_partition_starts(DATASET, derivative).gold
    if start is None:
        raise ValueError(
            f"{DATASET} resolved no Gold served_start for {derivative}; every "
            "public-contracts derivative declares one"
        )
    return start


contracts_facts_gold_partitions = dg.DailyPartitionsDefinition(
    start_date=_gold_start(CONTRACTS_FACTS_DERIVATIVE)
)
contracts_item_facts_gold_partitions = dg.DailyPartitionsDefinition(
    start_date=_gold_start(CONTRACTS_ITEM_FACTS_DERIVATIVE)
)
contracts_item_prices_gold_partitions = dg.DailyPartitionsDefinition(
    start_date=_gold_start(CONTRACTS_ITEM_PRICES_DERIVATIVE)
)
contracts_courier_rates_gold_partitions = dg.DailyPartitionsDefinition(
    start_date=_gold_start(CONTRACTS_COURIER_RATES_DERIVATIVE)
)


@dg.asset(
    name="contracts_facts_gold",
    partitions_def=contracts_facts_gold_partitions,
    deps=[public_contracts_silver],
    group_name="public_contracts",
    kinds={"corpus"},
    # No `pool=`: membership of a memory-bearing pool is by measured peak and
    # this build has none yet (see deploy/dagster.yaml). The global cap applies.
    #
    # A day whose Silver can never arrive is reported by corpus as "skipped"
    # with exit 0 and no partition written (ADR-0029), so the asset must be
    # allowed to complete without materialising.
    output_required=False,
)
def contracts_facts_gold(
    context: dg.AssetExecutionContext, corpus: CorpusResource
) -> Iterator[dg.MaterializeResult | dg.AssetObservation]:
    """Gold partition: one row per contract that is new on the day.

    ``deps=`` is lineage only; the readiness sensor drives this. The build reads
    that day's Silver alone — no cross-day state (ADR-0068) — and owns whatever
    gate it applies to it; there is no Python pre-check.
    """
    yield from _build_gold(context, corpus, CONTRACTS_FACTS_DERIVATIVE)


@dg.asset(
    name="contracts_item_facts_gold",
    partitions_def=contracts_item_facts_gold_partitions,
    deps=[public_contracts_silver],
    group_name="public_contracts",
    kinds={"corpus"},
    # No `pool=`: see contracts_facts_gold.
    output_required=False,
)
def contracts_item_facts_gold(
    context: dg.AssetExecutionContext, corpus: CorpusResource
) -> Iterator[dg.MaterializeResult | dg.AssetObservation]:
    """Gold partition: the item side of the same fold.

    Shares the day's Silver with ``contracts_facts_gold`` on purpose, but is a
    separate build under its own ``--derivative``: the two trees are written and
    registered separately, and neither run produces the other's partition.
    """
    yield from _build_gold(context, corpus, CONTRACTS_ITEM_FACTS_DERIVATIVE)


@dg.asset(
    name="contracts_item_prices_gold",
    partitions_def=contracts_item_prices_gold_partitions,
    deps=[public_contracts_silver],
    group_name="public_contracts",
    kinds={"corpus"},
    # No `pool=`: see contracts_facts_gold.
    output_required=False,
)
def contracts_item_prices_gold(
    context: dg.AssetExecutionContext, corpus: CorpusResource
) -> Iterator[dg.MaterializeResult | dg.AssetObservation]:
    """Gold partition: the per-type item price distribution for the day.

    Which contracts qualify — unmutated single-item exchanges — is the builder's
    rule (ADR-0068 section 7), never filtered here.
    """
    yield from _build_gold(context, corpus, CONTRACTS_ITEM_PRICES_DERIVATIVE)


@dg.asset(
    name="contracts_courier_rates_gold",
    partitions_def=contracts_courier_rates_gold_partitions,
    deps=[public_contracts_silver],
    group_name="public_contracts",
    kinds={"corpus"},
    # No `pool=`: see contracts_facts_gold.
    output_required=False,
)
def contracts_courier_rates_gold(
    context: dg.AssetExecutionContext, corpus: CorpusResource
) -> Iterator[dg.MaterializeResult | dg.AssetObservation]:
    """Gold partition: the volume-weighted freight rate per route for the day.

    The builder resolves ``end_region_id`` against sealed ``structures-snapshot``
    and SDE trees and fingerprints them into ``_INDEX.json`` (ADR-0052). Those
    are its own reads, so they are deliberately not ``deps=`` here: a Dagster
    edge would claim an ordering the binary already owns.
    """
    yield from _build_gold(context, corpus, CONTRACTS_COURIER_RATES_DERIVATIVE)
