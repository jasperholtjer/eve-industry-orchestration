"""Shared tail for the availability / readiness sensors.

Every dataset's Silver and Gold sensor is the same thin loop: diff the corpus
run-state report (``missing`` for Silver, ``ready`` for Gold) against the valid
partition matrix, cap the fan-out per tick, and request one run per eligible
date. This module owns the two non-obvious parts of that loop so they stay
identical across datasets:

- **Retry-safe run keys.** Silver's ``output_required=False`` (ADR-0041) means an
  upstream-incomplete day finishes as a green no-op *without* materialising, and
  corpus keeps reporting it ``missing``. A *static* ``run_key`` per date would be
  deduped by Dagster after that first no-op run, permanently blocking the retry
  the incomplete-skip promises — the failure mode that stalled market-history
  from 2026-06-27. The key therefore carries a per-tick token drawn from the
  sensor cursor, so each tick's request for a still-missing date is a distinct,
  non-deduped run. Once corpus commits the partition it drops out of the report,
  so the retries are self-limiting.
- **In-flight guard.** Because the key rotates, a slow run still in flight when
  the next tick fires would otherwise be launched a second time — two writers
  racing on the same contract dir on the single-HDD NAS (Gold overwrites in
  place). Dates that already have a non-terminal run for the target asset are
  skipped until that run settles.
"""

from __future__ import annotations

from collections.abc import Iterable

import dagster as dg

# Cap how many partitions enter the queue per tick (oldest first) so a cold start
# does not enqueue the whole backlog at once; later ticks drain the remainder.
# Concurrency itself is governed by deploy/dagster.yaml (global max_concurrent_runs
# plus the `everef_download` / `heavy` pools on the assets).
MAX_PARTITIONS_PER_TICK = 10

_ACTIVE_STATUSES = [
    dg.DagsterRunStatus.QUEUED,
    dg.DagsterRunStatus.NOT_STARTED,
    dg.DagsterRunStatus.STARTING,
    dg.DagsterRunStatus.STARTED,
    dg.DagsterRunStatus.CANCELING,
]


def _next_token(cursor: str | None) -> int:
    """Returns the next per-tick token, monotonic across ticks via the cursor."""
    if not cursor:
        return 1
    try:
        return int(cursor) + 1
    except ValueError:
        return 1


def _in_flight_partitions(
    context: dg.SensorEvaluationContext, asset_key: dg.AssetKey
) -> set[str]:
    """Partition keys with a non-terminal run for ``asset_key``.

    Guards the rotating ``run_key`` against launching a second run for a date
    whose prior run has not yet settled — which would put two ``corpus`` writers
    on the same contract directory. Best-effort: without run storage (a unit-test
    context built with no instance) it reports nothing in flight, since such a
    context launches no real runs to race with.
    """
    try:
        instance = context.instance
    except dg.DagsterInvariantViolationError:
        return set()
    records = instance.get_run_records(filters=dg.RunsFilter(statuses=_ACTIVE_STATUSES))
    in_flight: set[str] = set()
    for record in records:
        run = record.dagster_run
        selection = run.asset_selection or set()
        if asset_key not in selection:
            continue
        partition = run.tags.get("dagster/partition")
        if partition:
            in_flight.add(partition)
    return in_flight


def request_partitions(
    context: dg.SensorEvaluationContext,
    *,
    reported: Iterable[str],
    valid: set[str],
    run_key_prefix: str,
    asset_key: dg.AssetKey,
    label: str,
) -> dg.SensorResult:
    """Builds retry-safe run requests for the dates corpus reports actionable.

    Args:
        context: The sensor evaluation context (for the cursor, log, instance).
        reported: Dates corpus reports actionable (``missing`` or ``ready``).
        valid: The dataset's valid partition keys for this tier.
        run_key_prefix: Stable per-partition ``run_key`` stem, e.g.
            ``market-history-silver``; the date and a rotating token are appended.
        asset_key: The sensor's target asset, used by the in-flight guard.
        label: Log prefix (``availability`` / ``gold-readiness``).

    Returns:
        A :class:`dagster.SensorResult` carrying the run requests and the advanced
        cursor token.
    """
    eligible = sorted(date for date in reported if date in valid)
    selected = eligible[:MAX_PARTITIONS_PER_TICK]

    deferred = len(eligible) - len(selected)
    if deferred > 0:
        context.log.info(
            "%s: %d eligible, requesting %d this tick, %d deferred",
            label,
            len(eligible),
            len(selected),
            deferred,
        )

    token = _next_token(context.cursor)
    in_flight = _in_flight_partitions(context, asset_key)
    run_requests = [
        dg.RunRequest(run_key=f"{run_key_prefix}-{date}-{token}", partition_key=date)
        for date in selected
        if date not in in_flight
    ]
    return dg.SensorResult(run_requests=run_requests, cursor=str(token))
