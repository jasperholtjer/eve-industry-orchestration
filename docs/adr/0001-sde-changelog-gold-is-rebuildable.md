# ADR-0001: `sde-changelog` Gold is rebuildable, not write-once

## Status

Accepted (2026-09-01). Decided by the `sde-build-sequence-integrity` row, which
resolved the 2026-09-01 question on holes in the SDE build sequence. That
question file is deleted once its answer lands here: this record replaces it,
and git holds what it said.

## Context

The binary diffs a changelog build against the largest **committed** Silver build
below it. The Gold sensor asks for a build as soon as *any* smaller build has
committed Silver. While a lower `sde_silver` run is still in flight those two
questions differ, and the changelog is then built against the wrong predecessor —
a diff over too wide an interval, with the intermediate link missing.

That is repairable, because Gold overwrites in place and the binary recomputes
the predecessor from currently committed Silver. So `sde_gold_sensor` now carries
a stale term: the nearest lower Silver committed *after* this Gold was built,
read through `corpus state query`, folded into the outstanding set. A wrong diff
is corrected on the next tick by a plain rematerialise — no new corpus surface,
no bespoke repair path.

The consequence is what needs recording: a changelog partition can now change
after it was first written.

## Decision

`sde-changelog` Gold is **not write-once**. A changelog partition whose
predecessor changed underneath it is rebuilt in place, which changes its
`parquet_sha256`. A consumer that recorded that hash for an already-loaded
partition will see a different one later, and that is correct behaviour rather
than corruption or drift.

The build number stays the key throughout — ordering, predecessor, deferral and
detection all turn on it. `release_date` is carried as a log and metadata label
only.

## Consequences

Mutable Gold is precedent here, not novelty. `killmails` already rebuilds days
that grow upstream, and corpus ADR-0060 (`killmails` mutable partitions and the
`totals.json` freshness token) is the record for that. What is new is only that
this dataset joins them.

The mitigation is already in place and needs no work from this row: every
serving load is idempotent on the partition's `parquet_sha256` — an unchanged
partition prints `skipped` and is a no-op, a changed one re-loads
([`docs/serving-seam.md`](../serving-seam.md)). A repaired changelog therefore
propagates by itself on the next scheduled load.

Anything downstream that treats a recorded `parquet_sha256` as permanent for
this dataset is wrong and must re-read it. Nothing in this platform does today.

## Operator note: the one-off run-state check

Any changelog partitions built across a hole *before* this row deployed are
already stale. The stale term repairs them on the first tick after deploy, so
this check is **confirmation, not a prerequisite** — it is not a deploy gate and
nothing waits on it. The session that wrote this ADR could not run it: outbound
SSH to the Dagster LXC is blocked in its sandbox.

Run once against the NAS run-state:

```sql
SELECT CAST(substr(g.partition_key, 7) AS INTEGER) AS build
FROM partitions g
WHERE g.dataset = 'sde-changelog' AND g.tier = 'gold'
  AND g.last_seen_at < (
    SELECT s.last_seen_at FROM partitions s
    WHERE s.dataset = 'sde' AND s.tier = 'silver'
      AND CAST(substr(s.partition_key, 7) AS INTEGER)
          < CAST(substr(g.partition_key, 7) AS INTEGER)
    ORDER BY CAST(substr(s.partition_key, 7) AS INTEGER) DESC
    LIMIT 1
  )
```

Any rows it returns are changelog partitions built against the wrong
predecessor: rematerialise those `sde_changelog_gold` partitions. A baseline
build yields NULL and drops out on its own. An empty result confirms the sensor
already healed them.
