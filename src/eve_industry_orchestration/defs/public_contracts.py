"""public-contracts: the history tier over EVE Ref's public-contract archives.

One day-partitioned Silver asset and nothing else. The upstream publishes ~47
snapshots a day in per-date folders (``.v2.tar.bz2``, corpus ADR-0068); a
partition is one DAY, whose snapshot archives the binary merges into one
source-faithful Silver stream at snapshot grain (ADR-0068 decision 5 — one
table, one ``data.parquet`` per day). No Bronze is written: the archives are
streamed and discarded (ADR-0067), so there is no Bronze tier to wire.

**Why the start comes from the coverage floor, not from a derivative.**
``datasets/public-contracts.yaml`` declares no ``gold:`` block at all, so there
is no windowed derivative to reach back from — the usual
``gold.served_start`` minus the look-back has nothing to anchor to. Its
``silver.served_start`` (ADR-0027) is the only anchor the config declares: the
first day of the ``.v2.tar.bz2`` era, below which the eight 2019 ``.json.gz``
days are a different payload entirely. :func:`config.resolve_silver_start` reads
that floor, so no date literal appears here.

**The Gold derivatives are corpus's ``public-contracts-gold`` row, not this
one.** The 43x fold from snapshot grain to a served shape is a Gold concern
(ADR-0068 decision 5) and the derivatives are that row's to declare; until the
YAML carries them there is no Gold asset and no ``ready-dates`` sensor here.

The live twin ``public-contracts-live`` (:mod:`public_contracts_live`) is a
separate dataset with a separate YAML, a current-overwrite ``current/``
partition and its own schedule. Neither tier depends on the other.
"""

from collections.abc import Iterator

import dagster as dg

from eve_industry_orchestration.defs.config import resolve_silver_start
from eve_industry_orchestration.defs.corpus_resource import CorpusResource, date_key

DATASET = "public-contracts"

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
    output_required=False,
)
def public_contracts_silver(
    context: dg.AssetExecutionContext, corpus: CorpusResource
) -> Iterator[dg.MaterializeResult | dg.AssetObservation]:
    """Silver partition: ingest one day's contract snapshots, then verify.

    Every date is requested the same way — the binary owns which archives a day
    holds and how they are merged, so the asset never branches on the packaging
    era or the snapshot count. A genuinely-absent upstream day (corpus reports
    ``status: skipped``) is left Missing: the verify (which would 404 on the
    absent partition) is skipped and an ``AssetObservation`` records why,
    instead of a misleading materialisation.
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
