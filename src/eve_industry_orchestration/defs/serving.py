"""Serving-tier load assets: trigger ``eve-serving load`` after Gold is produced.

The serving tier (Postgres ``eve`` + Neo4j on the DB-VM) is already deployed and
verified; this repo owns only the *when*. Each asset is a thin shim that shells
the idempotent ``eve-serving load`` CLI over SSH (see :class:`ServingResource`)
and records the run — it reimplements no load logic and never touches the
databases directly.

**Ordering (the SDE→fact edge).** A new SDE build does a full-state rewrite that
``TRUNCATE``s the ``market.*`` + ``industry.*`` fact tables and clears their
serving load-state, so the fact datasets must be (re)loaded *after* the SDE load.
The asset graph models this: :func:`serving_load_sde` is an upstream of all four
fact loads, so a single :data:`serving_load_job` run executes them in topological
order — SDE first, then the facts — and the loader's ``parquet_sha256`` idempotency
makes an unchanged run a no-op. After an SDE rebuild the SDE load truncates and
repopulates, which clears the fact load-state, so the downstream fact loads re-run
and repopulate their tables in the same pass.

Each load also depends on its source Gold dataset being present (``_DONE``): the
``deps=`` carry that lineage (the SDE load off Gold ``sde-snapshot``, each fact
load off its Gold dataset). ``deps=`` is lineage only — the schedule on
:data:`serving_load_job` drives the actual materialisations, mirroring how the rest
of this repo separates lineage from triggering.

All five loads are latest-only / ``current``-snapshot operations, so the assets are
**non-partitioned** (the loader resolves the newest Gold partition itself), like
the ``*-live`` Gold assets.
"""

import dagster as dg

from eve_industry_orchestration.defs import industry_cost_indices_live as icil
from eve_industry_orchestration.defs import market_history, sde
from eve_industry_orchestration.defs import market_orders_live as mol
from eve_industry_orchestration.defs import market_prices_live as mpl
from eve_industry_orchestration.defs.serving_resource import ServingResource

_GROUP = "serving"
# Each load writes both serving stores; the kinds surface that in the asset graph.
_KINDS = {"postgres", "neo4j"}


def _result(
    dataset: str, serving: ServingResource, status: dict
) -> dg.MaterializeResult:
    return dg.MaterializeResult(
        metadata={
            "dataset": dataset,
            "host": f"{serving.user}@{serving.host}",
            "action": status.get("action"),
            "rows": status.get("rows"),
        }
    )


@dg.asset(
    key="serving_load_sde",
    deps=[sde.sde_snapshot_gold],
    group_name=_GROUP,
    kinds=_KINDS,
)
def serving_load_sde(
    context: dg.AssetExecutionContext, serving: ServingResource
) -> dg.MaterializeResult:
    """Load the latest SDE catalogue into the serving tier (full-state rewrite).

    Non-partitioned, latest-only: ``eve-serving load --dataset sde --latest`` reads
    the newest Gold ``sde-snapshot`` and rewrites the static-reference tables,
    TRUNCATEing ``market.*`` and clearing their load-state — which is why the three
    market loads sit downstream of this asset.
    """
    status = serving.load(context, "sde", "--latest")
    return _result("sde", serving, status)


@dg.asset(
    key="serving_load_market_history",
    deps=[market_history.market_history_gold, serving_load_sde],
    group_name=_GROUP,
    kinds=_KINDS,
)
def serving_load_market_history(
    context: dg.AssetExecutionContext, serving: ServingResource
) -> dg.MaterializeResult:
    """Load the latest market-history Gold into the serving tier.

    ``eve-serving load --dataset market-history`` defaults to the latest Gold
    partition. Downstream of :func:`serving_load_sde` (its FK target — types and
    regions — and TRUNCATE source) and of the Gold ``market-history`` availability.
    """
    status = serving.load(context, "market-history")
    return _result("market-history", serving, status)


@dg.asset(
    key="serving_load_market_orders_live",
    deps=[mol.market_orders_live_gold, serving_load_sde],
    group_name=_GROUP,
    kinds=_KINDS,
)
def serving_load_market_orders_live(
    context: dg.AssetExecutionContext, serving: ServingResource
) -> dg.MaterializeResult:
    """Load the live orderbook ``current`` snapshot into the serving tier."""
    status = serving.load(context, "market-orders-live")
    return _result("market-orders-live", serving, status)


@dg.asset(
    key="serving_load_market_prices_live",
    deps=[mpl.market_prices_live_gold, serving_load_sde],
    group_name=_GROUP,
    kinds=_KINDS,
)
def serving_load_market_prices_live(
    context: dg.AssetExecutionContext, serving: ServingResource
) -> dg.MaterializeResult:
    """Load the live CCP prices ``current`` snapshot into the serving tier."""
    status = serving.load(context, "market-prices-live")
    return _result("market-prices-live", serving, status)


@dg.asset(
    key="serving_load_industry_cost_indices_live",
    deps=[icil.industry_cost_indices_live_gold, serving_load_sde],
    group_name=_GROUP,
    kinds=_KINDS,
)
def serving_load_industry_cost_indices_live(
    context: dg.AssetExecutionContext, serving: ServingResource
) -> dg.MaterializeResult:
    """Load the live cost-index ``current`` snapshot into the serving tier.

    Downstream of :func:`serving_load_sde`: the ``industry.cost_indices_live`` rows
    carry a ``system_id`` FK into the map dimension, so the SDE full-state rewrite
    TRUNCATEs the table and clears its load-state, forcing a reload in the same pass.
    """
    status = serving.load(context, "industry-cost-indices-live")
    return _result("industry-cost-indices-live", serving, status)


# One job over the five loads. Selecting them together makes a single run execute
# in dependency order — SDE first, then the market + industry facts — so an SDE
# rebuild's TRUNCATE is always followed by a fact reload within the same pass.
serving_load_job = dg.define_asset_job(
    name="serving_load_job",
    selection=[
        serving_load_sde,
        serving_load_market_history,
        serving_load_market_orders_live,
        serving_load_market_prices_live,
        serving_load_industry_cost_indices_live,
    ],
)

# Hourly trigger. Every load is idempotent on the Gold partition's parquet_sha256,
# so an unchanged hour is a cheap no-op (``skipped: 0 rows``); the hourly cadence
# bounds how long the serving tier lags a fresh Gold build or an SDE rebuild. A
# schedule, not a sensor: there is no per-date matrix to diff — the loader resolves
# the newest Gold partition itself — only "reload the latest", the same construct
# as the ``*-live`` Gold schedules.
serving_load_schedule = dg.ScheduleDefinition(
    name="serving_load_schedule",
    job=serving_load_job,
    cron_schedule="0 * * * *",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)
