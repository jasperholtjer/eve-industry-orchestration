"""mer: the Monthly Economic Report monthly-archive dataset (corpus ADR-0058).

Like ``sde``, MER is NOT daily and NOT date-windowed: a partition is a *report
month*, published ~5th–10th of the following month and discovered by
``corpus everef list`` (not date enumeration). The matrix is therefore a
``DynamicPartitionsDefinition`` keyed on the report-month (``YYYY-MM-01``),
populated by the report-discovery sensor — there is no ``DailyPartitionsDefinition``
and no :func:`config.resolve_partition_starts` window.

One report-month ZIP feeds **two** source-faithful Silver streams via two
``corpus ingest`` calls over the same archive (corpus ADR-0058 §2):

- :func:`mer_silver` — the macro blob ``corpus ingest --dataset mer --month
  <YYYY-MM>`` commits ``silver/mer/`` `(concept, report_month, row_index, era,
  source_file, row_json)`. Report-month-partitioned. Carries ``everef_download``.
- :func:`mer_killdump_silver` — the per-kill ``corpus ingest --dataset
  mer-killdump --month <YYYY-MM>`` commits the typed era-superset
  ``silver/mer-killdump/``. Report-month-partitioned, **Silver only** (no Gold —
  kept for a future killmails cross-check). Carries ``everef_download``.

The five kern-series Gold trees are each a **full cross-month point-in-time
merge** over *every* committed ``mer`` Silver partition (corpus ADR-0058 §5), so
they are non-partitioned assets a schedule rematerialises — there is no per-date
Gold matrix and ``corpus gold build --dataset mer --derivative <name>`` takes no
date/build selector. Each self-skips when no Silver is committed yet. The Gold
trees are year-partitioned by ``history_date`` on disk (the binary owns that),
not date-verifiable as a single partition, so the build status is the success
signal (mirroring ``sde_snapshot_gold``).

All Gold builds merge small macro-aggregate CSVs (money supply, indices, sinks,
production), not a wide window — no ``heavy`` pool.
"""

import dagster as dg

from eve_industry_orchestration.defs.corpus_resource import CorpusResource

# The macro-aggregate blob dataset. Gold reads across all its Silver partitions.
DATASET = "mer"
# The per-kill dataset (Silver only). Shares the report-month partition set.
KILLDUMP_DATASET = "mer-killdump"

# The five kern-series Gold trees (corpus ADR-0058 §5 / datasets/mer.yaml). Each
# is one ``mer-history`` derivative over a kern concept; hardcoded like the SDE
# derivative constants (they are the Gold-tree path components + state keys).
HISTORY_DERIVATIVES = (
    "mer-money-supply",
    "mer-economy-indices",
    "mer-sinks-faucets",
    "mer-commodity-sinks-faucets",
    "mer-production-destruction",
)

# A report-month is the partition identity. Report-months are discovered, not
# enumerable from a start date, so the matrix is dynamic and the report-discovery
# sensor registers each new month's key (``YYYY-MM-01``).
report_partitions = dg.DynamicPartitionsDefinition(name="mer_report_months")

_SILVER_POOL = "everef_download"
_GROUP = "mer"


def _month_arg(partition_key: str) -> str:
    """``YYYY-MM-01`` partition key → the ``--month YYYY-MM`` ingest selector."""
    return partition_key[:7]


@dg.asset(
    key="mer_silver",
    partitions_def=report_partitions,
    group_name=_GROUP,
    kinds={"corpus"},
    pool=_SILVER_POOL,
)
def mer_silver(
    context: dg.AssetExecutionContext, corpus: CorpusResource
) -> dg.MaterializeResult:
    """Silver blob for one report-month: ingest, then verify.

    ``corpus ingest --dataset mer --month <YYYY-MM>`` commits the one atomic blob
    Silver partition (corpus ADR-0058 §4a); the asset only shells out and records
    the materialisation. Verify addresses the ``mer`` tree at the report-month
    (``YYYY-MM-01`` = the partition key).
    """
    month = _month_arg(context.partition_key)
    status = corpus.run(
        context,
        "ingest",
        "--dataset",
        DATASET,
        "--month",
        month,
        "--sink-path",
        corpus.sink_path,
    )
    report_month = (status or {}).get("report_month", context.partition_key)
    corpus.run(
        context,
        "verify",
        "--dataset",
        DATASET,
        "--date",
        report_month,
        "--tier",
        "silver",
        "--sink-path",
        corpus.sink_path,
    )
    return dg.MaterializeResult(
        metadata={
            "dataset": DATASET,
            "tier": "silver",
            "report_month": report_month,
            "rows": (status or {}).get("rows"),
        }
    )


@dg.asset(
    key="mer_killdump_silver",
    partitions_def=report_partitions,
    group_name=_GROUP,
    kinds={"corpus"},
    pool=_SILVER_POOL,
)
def mer_killdump_silver(
    context: dg.AssetExecutionContext, corpus: CorpusResource
) -> dg.MaterializeResult:
    """Silver typed era-superset for one report-month: ingest, then verify.

    ``corpus ingest --dataset mer-killdump --month <YYYY-MM>`` commits the
    per-kill Silver over the same ZIP as :func:`mer_silver` (corpus ADR-0058
    §4b). Silver only — no Gold derivative. Shares the report-month partition set.
    """
    month = _month_arg(context.partition_key)
    status = corpus.run(
        context,
        "ingest",
        "--dataset",
        KILLDUMP_DATASET,
        "--month",
        month,
        "--sink-path",
        corpus.sink_path,
    )
    report_month = (status or {}).get("report_month", context.partition_key)
    corpus.run(
        context,
        "verify",
        "--dataset",
        KILLDUMP_DATASET,
        "--date",
        report_month,
        "--tier",
        "silver",
        "--sink-path",
        corpus.sink_path,
    )
    return dg.MaterializeResult(
        metadata={
            "dataset": KILLDUMP_DATASET,
            "tier": "silver",
            "report_month": report_month,
            "rows": (status or {}).get("rows"),
        }
    )


def _history_gold(
    context: dg.AssetExecutionContext, corpus: CorpusResource, derivative: str
) -> dg.MaterializeResult:
    """Rematerialises one kern-series Gold tree from all committed Silver.

    ``corpus gold build --dataset mer --derivative <name>`` takes **no** date /
    build selector (corpus ADR-0058 §5): it merges the concept's rows across
    every committed ``mer`` Silver partition and year-partitions the result by
    ``history_date``. Skips cleanly when no Silver is committed yet (the schedule
    may fire on a cold corpus); the year-partitioned tree is not date-verifiable
    as one partition, so the build status is the success signal.
    """
    committed = corpus.state_query(
        "SELECT 1 FROM partitions WHERE tier = 'silver' AND dataset = 'mer' LIMIT 1"
    )
    if not committed:
        context.log.info(
            "mer %s: no committed Silver report-month yet; skipping", derivative
        )
        return dg.MaterializeResult(
            metadata={"dataset": derivative, "tier": "gold", "built": False}
        )

    status = corpus.run(
        context,
        "gold",
        "build",
        "--dataset",
        DATASET,
        "--derivative",
        derivative,
        "--sink-path",
        corpus.sink_path,
    )
    return dg.MaterializeResult(
        metadata={
            "dataset": derivative,
            "tier": "gold",
            "built": True,
            "concept": (status or {}).get("concept"),
            "years": (status or {}).get("years"),
            "row_count": (status or {}).get("row_count"),
        }
    )


@dg.asset(
    key="mer_money_supply", deps=[mer_silver], group_name=_GROUP, kinds={"corpus"}
)
def mer_money_supply_gold(
    context: dg.AssetExecutionContext, corpus: CorpusResource
) -> dg.MaterializeResult:
    """Point-in-time money-supply history (corpus ADR-0058 §5)."""
    return _history_gold(context, corpus, "mer-money-supply")


@dg.asset(
    key="mer_economy_indices", deps=[mer_silver], group_name=_GROUP, kinds={"corpus"}
)
def mer_economy_indices_gold(
    context: dg.AssetExecutionContext, corpus: CorpusResource
) -> dg.MaterializeResult:
    """Point-in-time economy-indices history (corpus ADR-0058 §5)."""
    return _history_gold(context, corpus, "mer-economy-indices")


@dg.asset(
    key="mer_sinks_faucets", deps=[mer_silver], group_name=_GROUP, kinds={"corpus"}
)
def mer_sinks_faucets_gold(
    context: dg.AssetExecutionContext, corpus: CorpusResource
) -> dg.MaterializeResult:
    """Point-in-time sinks/faucets history (corpus ADR-0058 §5)."""
    return _history_gold(context, corpus, "mer-sinks-faucets")


@dg.asset(
    key="mer_commodity_sinks_faucets",
    deps=[mer_silver],
    group_name=_GROUP,
    kinds={"corpus"},
)
def mer_commodity_sinks_faucets_gold(
    context: dg.AssetExecutionContext, corpus: CorpusResource
) -> dg.MaterializeResult:
    """Point-in-time commodity sinks/faucets history (corpus ADR-0058 §5)."""
    return _history_gold(context, corpus, "mer-commodity-sinks-faucets")


@dg.asset(
    key="mer_production_destruction",
    deps=[mer_silver],
    group_name=_GROUP,
    kinds={"corpus"},
)
def mer_production_destruction_gold(
    context: dg.AssetExecutionContext, corpus: CorpusResource
) -> dg.MaterializeResult:
    """Point-in-time production/destruction/mining history (corpus ADR-0058 §5)."""
    return _history_gold(context, corpus, "mer-production-destruction")


# The five Gold assets in declaration order, for the rematerialise schedule.
HISTORY_GOLD_ASSETS = (
    mer_money_supply_gold,
    mer_economy_indices_gold,
    mer_sinks_faucets_gold,
    mer_commodity_sinks_faucets_gold,
    mer_production_destruction_gold,
)
