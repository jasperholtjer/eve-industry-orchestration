"""market-prices-live: CCP adjusted/average prices, current-overwrite (corpus ADR-0040).

A separate corpus dataset with the same current-overwrite lifecycle as
``market-orders-live`` (ADR-0039), but a different source: CCP's per-type
``adjusted_price`` / ``average_price`` are published only at the live ESI
endpoint ``/markets/prices/`` (corpus ``SourceConfig::Esi``, the first non-everef
source). ``corpus live build`` GETs the endpoint, stamps the response
``Last-Modified`` as ``snapshot_at``, and **overwrites** a fixed
``gold/market-prices-live/current/`` partition. There is no Silver tier, no
``year=/month=/day=`` matrix, and no history — last write wins.

Orchestration mirrors ``market-orders-live``: a single non-partitioned asset
driven by a fixed-cadence ``dg.schedule`` (see :mod:`sensors`), not a sensor.
There is no per-date availability to diff — only "overwrite ``current/`` with
whatever ESI serves right now".

Unlike ``market-orders-live`` the fetch hits **ESI, not EVE Ref**, so the asset
does **not** join the ``everef_download`` politeness pool — a single small hourly
GET cannot starve the EVE Ref Silver transfers and shares no endpoint with them.
It is lightweight on memory (one ~16k-row payload), so not the ``heavy`` pool
either; it obeys only the global ``max_concurrent_runs`` cap.

The asset is a thin shim: it shells ``corpus live build`` and records the run.
The binary owns the fetch, the decode, and the atomic ``parquet + _INDEX.json +
_DONE`` overwrite. No ``corpus verify`` call — that resolves a day-partitioned
path and the live tree is the non-partitioned ``current/`` partition; the binary
raises a non-zero exit on failure, which the resource turns into a run failure.
"""

import dagster as dg

from eve_industry_orchestration.defs.corpus_resource import CorpusResource

DATASET = "market-prices-live"


@dg.asset(
    group_name="market_prices",
    kinds={"corpus"},
)
def market_prices_live_gold(
    context: dg.AssetExecutionContext, corpus: CorpusResource
) -> dg.MaterializeResult:
    """Overwrite the live ``current/`` partition with the latest ESI snapshot.

    Non-partitioned: every run targets the same ``gold/market-prices-live/current/``
    partition (ADR-0040). The schedule drives it; there is no partition matrix and
    no ``deps=`` chain (the dataset fetches its own ESI payload, independent of any
    Silver tier).
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
        for key in ("snapshot_at", "rows", "source"):
            if key in status:
                metadata[key] = status[key]
    return dg.MaterializeResult(metadata=metadata)
