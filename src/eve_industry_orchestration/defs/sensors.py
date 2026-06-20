"""Availability-driven sensor for the market-history Silver tier.

EVE Ref publishes one file per calendar day with a variable, ~1-day lag, so a
fixed cron would structurally miss "yesterday" (ROADMAP §Decisions). This sensor
polls ``corpus everef missing-partitions`` instead, which diffs upstream
availability against the SQLite ``partitions`` table, and requests a Silver
materialisation for each newly available date.

Status is keyed on the corpus run-state, never on globbing the NAS tree: the
``missing`` set already excludes locally ingested dates, and Dagster ``run_key``
dedup prevents re-queuing a date that is already requested or in flight. Gold
follows via the ``deps=`` chain once its builder is unblocked (ROADMAP item 2).
"""

import dagster as dg

from eve_industry_orchestration.defs.corpus_resource import CorpusResource
from eve_industry_orchestration.defs.market_history import (
    DATASET,
    market_history_silver,
    silver_partitions,
)

# deploy/dagster.yaml pins max_concurrent_runs:1, so the coordinator serialises
# execution. Still cap how many partitions enter the queue per tick (oldest
# first) so a cold start does not enqueue the whole backlog at once; later ticks
# drain the remainder. The tag mirrors the corpus `everef-download` lease so a
# tag_concurrency_limit can throttle this lane independently if needed.
MAX_PARTITIONS_PER_TICK = 10
_EVEREF_TAG = "corpus/everef-download"


@dg.sensor(
    target=market_history_silver,
    minimum_interval_seconds=300,
    default_status=dg.DefaultSensorStatus.STOPPED,
)
def market_history_availability_sensor(
    context: dg.SensorEvaluationContext, corpus: CorpusResource
) -> dg.SensorResult:
    """Requests Silver runs for market-history dates newly available upstream."""
    report = corpus.everef_missing_partitions(DATASET)
    missing = report.get("missing", [])

    valid = set(silver_partitions.get_partition_keys())
    eligible = sorted(date for date in missing if date in valid)
    selected = eligible[:MAX_PARTITIONS_PER_TICK]

    deferred = len(eligible) - len(selected)
    if deferred > 0:
        context.log.info(
            "availability: %d eligible, requesting %d this tick, %d deferred",
            len(eligible),
            len(selected),
            deferred,
        )

    run_requests = [
        dg.RunRequest(
            run_key=f"{DATASET}-silver-{date}",
            partition_key=date,
            tags={_EVEREF_TAG: "1"},
        )
        for date in selected
    ]
    return dg.SensorResult(run_requests=run_requests)
