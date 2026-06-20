"""Availability-driven sensor for the market-history Silver tier.

EVE Ref publishes one file per calendar day with a variable, ~1-day lag, so a
fixed cron would structurally miss "yesterday" (ROADMAP §Decisions). This sensor
polls ``corpus everef missing-partitions`` instead, which diffs upstream
availability against the SQLite ``partitions`` table, and requests a Silver
materialisation for each newly available date.

Status is keyed on the corpus run-state, never on globbing the NAS tree: the
``missing`` set already excludes locally ingested dates, and Dagster ``run_key``
dedup prevents re-queuing a date that is already requested or in flight.

Gold has its own availability sensor (``market_history_gold_sensor``): ``deps=``
only expresses lineage, it does not trigger downstream materialisations, so Gold
is driven by polling ``corpus gold ready-dates`` rather than by the Silver run.
"""

import dagster as dg

from eve_industry_orchestration.defs.corpus_resource import CorpusResource
from eve_industry_orchestration.defs.market_history import (
    DATASET,
    gold_partitions,
    market_history_gold,
    market_history_silver,
    silver_partitions,
)

# deploy/dagster.yaml runs up to max_concurrent_runs:4, with a
# tag_concurrency_limit of 2 on the everef-download tag below. Still cap how many
# partitions enter the queue per tick (oldest first) so a cold start does not
# enqueue the whole backlog at once; later ticks drain the remainder. The tag
# mirrors the corpus `everef-download` lease and is what the coordinator throttles
# to keep at most 2 Silver downloads (the `ingest` step) hitting EVE Ref at once.
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


@dg.sensor(
    target=market_history_gold,
    minimum_interval_seconds=300,
    default_status=dg.DefaultSensorStatus.STOPPED,
)
def market_history_gold_sensor(
    context: dg.SensorEvaluationContext, corpus: CorpusResource
) -> dg.SensorResult:
    """Requests Gold runs for market-history dates whose Silver window is complete.

    ``deps=`` only expresses lineage, so Gold needs its own availability trigger.
    The readiness decision (target-day Silver present, rolling window at
    ``coverage_min_ratio``, Gold not yet built) lives in ``corpus gold
    ready-dates`` — never recomputed in Python — so this sensor stays a thin
    cap-and-dedup loop, mirroring the Silver sensor.
    """
    report = corpus.gold_ready_dates(DATASET)
    ready = report.get("ready", [])

    valid = set(gold_partitions.get_partition_keys())
    eligible = sorted(date for date in ready if date in valid)
    selected = eligible[:MAX_PARTITIONS_PER_TICK]

    deferred = len(eligible) - len(selected)
    if deferred > 0:
        context.log.info(
            "gold-readiness: %d eligible, requesting %d this tick, %d deferred",
            len(eligible),
            len(selected),
            deferred,
        )

    run_requests = [
        dg.RunRequest(run_key=f"{DATASET}-gold-{date}", partition_key=date)
        for date in selected
    ]
    return dg.SensorResult(run_requests=run_requests)
