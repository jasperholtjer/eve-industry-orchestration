"""industry-cost-indices-live: current cost-index level (corpus ADR-0043).

A separate corpus dataset from ``industry-cost-indices``, with a different
lifecycle. The ``corpus live build`` subcommand fetches the single newest EVE Ref
cost-index snapshot, pivots it to the ``daily-snapshot`` shape, and **overwrites**
a fixed ``gold/industry-cost-indices-live/current/`` partition. There is no Silver
tier, no ``year=/month=/day=`` matrix, and no history — last write wins. A cost
index is the currently-valid administered level, so the now-value is simply the
latest snapshot (no EWMA or rolling — that is the ``industry-cost-indices-history``
derivative).

Orchestration therefore departs from the repo's "sensor over cron" rule
deliberately: there is no per-date availability to diff, only "grab whatever is
newest right now". A single non-partitioned asset, driven by a fixed-cadence
``dg.schedule`` (see :mod:`sensors`), is the correct construct — the same
point-in-time pattern as ``market-orders-live``.

The asset is a thin shim: it shells ``corpus live build`` and records the run. The
binary owns the fetch, the pivot, and the atomic ``parquet + _INDEX.json + _DONE``
overwrite. No ``corpus verify`` call — that resolves a day-partitioned
``year=/month=/day=`` path and the live tree is the non-partitioned ``current/``
partition; the binary raises a non-zero exit on failure, which the resource turns
into a run failure regardless.
"""

import dagster as dg

from eve_industry_orchestration.defs.corpus_resource import CorpusResource

DATASET = "industry-cost-indices-live"

# The source is EVE Ref (not ESI), so this joins the network politeness pool — one
# fetch per run, alongside the Silver fetches — unlike market-prices-live (ESI).
# Lightweight on memory (one snapshot), so not the `heavy` pool: it must not
# compete with the windowed Gold backfills. See deploy/dagster.yaml.
_LIVE_POOL = "everef_download"


@dg.asset(
    group_name="industry_cost_indices",
    kinds={"corpus"},
    pool=_LIVE_POOL,
)
def industry_cost_indices_live_gold(
    context: dg.AssetExecutionContext, corpus: CorpusResource
) -> dg.MaterializeResult:
    """Overwrite the live ``current/`` partition with the newest snapshot.

    Non-partitioned: every run targets the same
    ``gold/industry-cost-indices-live/current/`` partition (ADR-0043). The
    schedule drives it; there is no partition matrix and no ``deps=`` chain (the
    live dataset is independent of the day-partitioned ``industry-cost-indices``
    Silver — it fetches its own snapshot).
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
        for key in ("snapshot_file", "date", "rows", "source"):
            if key in status:
                metadata[key] = status[key]
    return dg.MaterializeResult(metadata=metadata)
