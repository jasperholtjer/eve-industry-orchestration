"""lp-store-offers-live: the LP store offers, current-overwrite (corpus ADR-0070).

The fifth member of the live current-overwrite family (ADR-0039/0040/0043/0068)
and the first that writes **two** Gold trees from one build: LP store offers
exist only on ESI — they are not in the SDE — and the endpoint serves the
current state only, so there is no day-partitioned ingest, no Bronze cache, no
Silver tier and no history. Last write wins.

**One asset, not one per tree.** `corpus live build` fans out over the 283 NPC
corporations `/corporations/npccorps/` names, and writes
``gold/lp-store-offers/current/`` and ``gold/lp-store-offer-items/current/``
from that single fetch, both built before either is committed. A second asset
would re-run the whole 284-request fan-out for the same payload, and could leave
one tree fresh against a stale other — a torn join for the consumer. So: one
asset, one invocation, two partitions recorded off one status object.

That status object is **multi-partition**, the only one in this repository that
is: the row counts arrive in a ``partitions`` list keyed on ``derivative``,
not as a top-level ``rows`` the sibling live assets read (ADR-0070 §3).

Cadence is a daily `dg.schedule` (see :mod:`sensors`), not a sensor and not
hourly. There is no per-date availability to diff — only "overwrite ``current/``
with what ESI serves now" — and every store's response carries the same
``Expires``, so the caches roll together once a day; one run past that instant
fetches one clean generation, and an hourly poll would repeat 284 requests
against a payload that only moves on deployments.

The fetch is ESI rather than EVE Ref, so the asset joins no ``everef_download``
politeness pool: it shares no endpoint with the Silver transfers and cannot
starve them. ~6 MB of JSON is not memory-bearing either, so not ``heavy``; it
obeys only the global ``max_concurrent_runs`` cap.

The asset is a thin shim: it shells ``corpus live build`` and records the run.
The binary owns the fan-out, its retries and its four-in-flight bound, the
decode and the atomic ``parquet + _INDEX.json + _DONE`` overwrite of both trees
— including the rule that a corporation whose GET exhausts its retries fails the
whole run while a ``200 []`` is a real empty store (102 of 283), so a short
table can never be published as a success. No ``corpus verify`` call: that
resolves a day-partitioned path and these are the non-partitioned ``current/``
trees; a non-zero exit is the failure signal the resource turns into a run
failure.
"""

import dagster as dg

from eve_industry_orchestration.defs.corpus_resource import CorpusResource

DATASET = "lp-store-offers-live"


@dg.asset(
    group_name="lp_store_offers",
    kinds={"corpus"},
)
def lp_store_offers_live_gold(
    context: dg.AssetExecutionContext, corpus: CorpusResource
) -> dg.MaterializeResult:
    """Overwrite both live ``current/`` partitions from one ESI fan-out.

    Non-partitioned: every run targets the same two Gold trees (ADR-0070). The
    schedule drives it; there is no partition matrix and no ``deps=`` chain (the
    dataset fetches its own ESI payloads, independent of any Silver tier).
    """
    status = corpus.run(
        context,
        "live",
        "build",
        "--dataset",
        DATASET,
        # An option of `live build`, not a global flag.
        "--sink-path",
        corpus.sink_path,
    )
    # No `partition_metadata` enrichment, for the reason `market_prices_live.py`
    # states at length: `corpus live build` writes no run-state row on any
    # branch, so a `state query` against `partitions` would match nothing and
    # warn on every scheduled run. What the enrichment would carry is on the
    # status line already, and the live trees' retention is fixed.
    metadata: dict[str, object] = {
        "dataset": DATASET,
        "tier": "gold",
        "partition": "current",
    }
    if status is not None:
        # Run-level facts: the snapshot instant, the source, and the fan-out's
        # own shape — `corporations` polled and how many were empty stores,
        # which is the one number that says whether a thin result is real.
        for key in ("snapshot_at", "source", "corporations", "empty_stores"):
            if key in status:
                metadata[key] = status[key]
        # Per-tree row counts. The status object is multi-partition here, so
        # there is no top-level `rows` to copy: one entry per Gold tree written.
        partitions = status.get("partitions")
        if isinstance(partitions, list):
            for part in partitions:
                if isinstance(part, dict) and "derivative" in part and "rows" in part:
                    metadata[f"rows.{part['derivative']}"] = part["rows"]
    return dg.MaterializeResult(metadata=metadata)
