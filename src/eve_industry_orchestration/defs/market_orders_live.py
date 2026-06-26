"""market-orders-live: near-real-time orderbook aggregate (corpus ADR-0039).

A separate corpus dataset from ``market-orders``, with a different lifecycle. The
``corpus live build`` subcommand fetches the single newest EVE Ref orderbook
snapshot, collapses it to the ``orderbook-aggregate`` shape, and **overwrites** a
fixed ``gold/market-orders-live/current/`` partition. There is no Silver tier, no
``year=/month=/day=`` matrix, and no history — last write wins.

Orchestration therefore departs from the repo's "sensor over cron" rule
deliberately: there is no per-date availability to diff, only "grab whatever is
newest right now". A single non-partitioned asset, driven by a fixed-cadence
``dg.schedule`` (see :mod:`sensors`), is the correct construct — the same
point-in-time pattern as the EWMA "recent" assets, minus the ``ready-dates`` date
resolution (the binary picks the snapshot itself).

The asset is a thin shim: it shells ``corpus live build`` and records the run. The
binary owns the fetch, the collapse, and the atomic ``parquet + _INDEX.json +
_DONE`` overwrite. No ``corpus verify`` call — that resolves a day-partitioned
``year=/month=/day=`` path and the live tree is the non-partitioned ``current/``
partition; the binary raises a non-zero exit on failure, which the resource turns
into a run failure regardless.
"""

import dagster as dg

from eve_industry_orchestration.defs.corpus_resource import CorpusResource

DATASET = "market-orders-live"

# One EVE Ref fetch per run; join the network politeness pool so the live cadence
# never pushes total EVE Ref parallel transfers past the courtesy cap alongside
# the Silver fetches. Lightweight on memory (one snapshot), so not the `heavy`
# pool — it must not compete with the windowed Gold backfills. See
# deploy/dagster.yaml.
_LIVE_POOL = "everef_download"


@dg.asset(
    group_name="market_orders",
    kinds={"corpus"},
    pool=_LIVE_POOL,
)
def market_orders_live_gold(
    context: dg.AssetExecutionContext, corpus: CorpusResource
) -> dg.MaterializeResult:
    """Overwrite the live ``current/`` partition with the newest snapshot.

    Non-partitioned: every run targets the same ``gold/market-orders-live/current/``
    partition (ADR-0039). The schedule drives it; there is no partition matrix and
    no ``deps=`` chain (the live dataset is independent of the day-partitioned
    ``market-orders`` Silver — it fetches its own snapshot).
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
    metadata: dict[str, object] = {
        "dataset": DATASET,
        "tier": "gold",
        "partition": "current",
    }
    if status is not None:
        for key in ("snapshot_file", "date", "rows"):
            if key in status:
                metadata[key] = status[key]
    return dg.MaterializeResult(metadata=metadata)
