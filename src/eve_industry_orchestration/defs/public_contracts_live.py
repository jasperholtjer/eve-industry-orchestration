"""public-contracts-live: newest open-contract snapshot (corpus ADR-0068).

The same current-overwrite lifecycle as ``market-orders-live`` (ADR-0039) and
``market-prices-live`` (ADR-0040): ``corpus live build`` lists the newest
per-date EVE Ref folder, fetches one ``.v2.tar.bz2``, and collapses it to one row
per open contract in a fixed ``gold/public-contracts-live/current/`` partition.
There is no Silver tier, no ``year=/month=/day=`` matrix, and no history — last
write wins, so ``defs/config.py`` resolves no start date here.

Orchestration therefore departs from the repo's "sensor over cron" rule
deliberately: there is no per-date availability to diff, only "grab whatever is
newest right now". A single non-partitioned asset, driven by a fixed-cadence
``dg.ScheduleDefinition`` (see :mod:`sensors`), is the correct construct — the
binary picks the snapshot itself, so there is no ``ready-dates`` resolution to
schedule around.

The asset is a thin shim: it shells ``corpus live build`` and records the run. The
binary owns the fetch, the collapse, and the atomic ``parquet + _INDEX.json +
_DONE`` overwrite. No ``corpus verify`` call — that resolves a day-partitioned
``year=/month=/day=`` path and the live tree is the non-partitioned ``current/``
partition; the binary raises a non-zero exit on failure, which the resource turns
into a run failure regardless.
"""

import dagster as dg

from eve_industry_orchestration.defs.corpus_resource import CorpusResource

DATASET = "public-contracts-live"

# One EVE Ref fetch per run; join the network politeness pool so the live cadence
# never pushes total EVE Ref parallel transfers past the courtesy cap alongside
# the Silver fetches. Lightweight on memory (one snapshot), so not the `heavy`
# pool — it must not compete with the windowed Gold backfills. See
# deploy/dagster.yaml.
_LIVE_POOL = "everef_download"


@dg.asset(
    group_name="public_contracts",
    kinds={"corpus"},
    pool=_LIVE_POOL,
)
def public_contracts_live_gold(
    context: dg.AssetExecutionContext, corpus: CorpusResource
) -> dg.MaterializeResult:
    """Overwrite the live ``current/`` partition with the newest snapshot.

    Non-partitioned: every run targets the same
    ``gold/public-contracts-live/current/`` partition (ADR-0068). The schedule
    drives it; there is no partition matrix and no ``deps=`` chain (the live
    dataset fetches its own snapshot and has no day-partitioned Silver upstream).
    """
    status = corpus.run(
        context,
        "live",
        "build",
        "--dataset",
        DATASET,
        "--sink-path",
        corpus.sink_path,
    )
    # No `partition_metadata` enrichment here, deliberately: `corpus live build`
    # writes no run-state row at all. `crates/corpus-cli/src/live.rs` is never
    # handed the state DB, imports no `corpus_state`, and says so at the point of
    # the `_INDEX.json` it stamps: "No run-state row for the throwaway live tree;
    # the snapshot file is the traceable provenance." A `state query` against
    # `partitions` would therefore match nothing and log a warning every run. The
    # facts the enrichment would carry are already here: the binary prints `rows`
    # on the status line, and the live tree's retention is fixed (last write
    # wins, no retention class to vary).
    metadata: dict[str, object] = {
        "dataset": DATASET,
        "tier": "gold",
        "partition": "current",
    }
    if status is not None:
        # `snapshot_at` is the payload's own `scrape_start` — the freshness the
        # filename cannot give, because its seconds field drifts. Copy each key
        # only when the binary reported it; a missing key stays absent rather
        # than being defaulted, so metadata never claims a freshness the run did
        # not observe.
        for key in ("snapshot_at", "snapshot_file", "date", "rows"):
            if key in status:
                metadata[key] = status[key]
    return dg.MaterializeResult(metadata=metadata)
