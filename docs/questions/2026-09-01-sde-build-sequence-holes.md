---
status: answered
row: sde-gold-sensor-stall
---

# Should a hole in the SDE build sequence block the next changelog, or diff across it?

## Why this is blocked

`sde-gold-sensor-stall` fixed the SDE Gold sensor's stall: it now subtracts the
builds whose changelog Gold is committed before applying the per-tick cap, and
requests the rest through `request_partitions`. Reviewing that change surfaced a
second, older defect underneath it, which the row deliberately did not widen to
cover because the answer is a policy choice rather than a bug fix.

The binary's predecessor is "the largest **committed** Silver build below the
target" (`corpus-cli/src/sde.rs::build_changelog`). The sensor asks for a build
as soon as *any* smaller build has committed Silver. Those two are not the same
question whenever the build sequence has a hole, and it can:

- `sde_silver` carries `pool="everef_download"` at `default_limit: 2`, so two
  builds ingest at once and 300 can commit before 200. The Gold tick in between
  sees `{100, 300}`, asks for 300, and the binary diffs 300 against **100**.
  When 200 lands it is diffed against 100 as well. Gold then holds an overlapping
  100→300 and 100→200, and the 200→300 diff never exists.
- `sde_build_discovery_sensor` still has exactly the bug this row fixed for Gold:
  a static `run_key` per build, filtered against the dynamic partitions it
  registered in the same tick. From the next tick the build counts as known, so a
  failed `sde_silver` run is never re-proposed and the hole is permanent.

Subtracting committed Gold makes the first one terminal — once 300's changelog is
committed it leaves the outstanding set for good. The old static `run_key` made
it terminal too, by dedup, so the row did not make this worse; it did make it the
explicit stop condition, which is why it is worth settling now.

## The options

- **Block on the sequence.** A build is outstanding only when the largest
  *registered* build below it has committed Silver. Cheap — the sensor already
  holds both sets — and it makes the sensor's readiness rule the binary's rule.
  The cost: a build whose Silver never commits blocks every later changelog until
  someone intervenes, and there is no suppression path (`skipped_partitions`,
  corpus ADR-0028, answers "this Silver will never exist", which is a different
  question and is asked only of a Silver dataset).
- **Diff across the hole, and repair later.** Keep today's rule and add a repair
  sensor, the way killmails already does for drifted Gold
  (`killmails_consumption_gold_repair_sensor` asks run-state which Gold predates
  its own Silver). Here it would ask which changelog partitions were built
  against a predecessor that is no longer the largest committed one below them,
  and rebuild those. Needs a corpus query or subcommand that can answer it — the
  changelog's recorded predecessor is not something this repo may compute.
- **Remove the hole instead.** Give `sde_build_discovery_sensor` the same
  treatment this row gave the Gold sensor: key Silver readiness on run-state
  (registered builds minus committed Silver) rather than on "not yet registered",
  and let `request_partitions` rotate the key. Then a failed ingest is retried,
  holes close on their own, and the ordering race narrows to the window where two
  pooled runs are genuinely in flight — which the in-flight guard already covers
  per partition, but not across partitions.

## What I would do

The third and then the first, as one follow-up row in that order. Fixing the
discovery sensor is the same fix this row just made, in the sibling sensor, and
it removes most of the exposure on its own — a hole that heals cannot become a
permanent wrong diff. Blocking on the sequence then closes the concurrency
window, and its "a stuck build stalls the rest" cost is acceptable precisely
because holes now heal: the stall becomes temporary rather than terminal. The
repair-sensor option is more machinery than the problem earns, and it needs
corpus work this repository cannot do for itself.

I would not fold either into `sde-gold-sensor-stall`. That row's spec is
`sde-gold-readiness` and its goal was the stall; blocking rules and a second
sensor are a second capability, and the row is already a strict improvement on
what `develop` had.

## Answer

Neither option as written. Take the **third** and the **second**, and replace
the blocking rule of the first with a bounded deferral. One follow-up row.

### Why not "block on the sequence"

The rule is correct — "the largest *registered* build below the target has
committed Silver" does give the binary the predecessor it would pick — but it is
too expensive for what it buys, on three counts:

- **Its only maintenance path defeats it.** The rule reads the Dagster
  dynamic-partitions store. Clearing a stuck build's key — the obvious operator
  action, and the one this document's own escape hatch implies — silently
  releases every build above it to diff across the resulting hole. A correctness
  rule whose repair procedure produces the defect it prevents is not a rule.
- **It compounds with the third option instead of cancelling it.** "The stall
  becomes temporary because holes now heal" assumes a failed Silver is transient.
  The one documented SDE failure is not: `skip_builds: [2960198]` is a
  structurally incomplete archive, permanently excluded. Under the third option
  that build is re-proposed every tick; under the first it also stops every later
  changelog, for good.
- **It moves the "sensor rule ≠ binary rule" defect rather than removing it.**
  The binary reads committed Silver from run-state; the blocking rule would read
  Dagster's partition store. Two stores joined by an unenforced assumption about
  discovery order — the same class of defect, one step along.

Against that, the damage it prevents is a diff over a wider interval that
correctly labels itself (`prev_build_id`), plus one overlapping partition and one
absent link, reachable only while two pooled Silver runs are genuinely in flight.
A silent, dataset-wide stall is the worse failure.

### Why the repair option is cheaper than this document assumed

It does not need the recorded predecessor, and therefore needs no corpus work.
The question "did a lower build's Silver commit after this Gold was built" is
already answerable through the sanctioned `state query` seam, in the pattern
`killmails_consumption_gold_repair_sensor` established. Only the **nearest**
lower Silver matters, which makes the query exact rather than a superset — no
rebuild storm when an old build is re-ingested, and no cascade:

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

A baseline build yields NULL and drops out on its own. Repair itself is already
free: Gold overwrites in place and the binary recomputes the predecessor from
currently committed Silver, so a flagged partition needs a rematerialise, nothing
more.

### The follow-up row

Three parts, one row, one spec:

1. **Fix `sde_build_discovery_sensor`** exactly as `sde-gold-sensor-stall` fixed
   the Gold sensor: readiness from run-state (registered minus committed Silver),
   `request_partitions` for the rotating key and the in-flight guard. Log the
   build, its `release_date` and the attempt, so a permanently failing build is
   visible rather than silent. Note the helper returns a `SensorResult` without
   `dynamic_partitions_requests`, so discovery has to reassemble it, with `valid`
   = registered ∪ newly discovered and `sort_key=int`.
2. **Fold the stale term into `sde_gold_sensor`**, not a second sensor. Killmails
   needs two because its terms target different assets; both terms here target
   `sde_changelog_gold`, so it is a union in the existing outstanding set:
   `(committed_silver − committed_gold − baseline) ∪ stale_gold`.
3. **Defer, do not block.** Hold back a Gold build while a *lower* `sde_silver`
   run is queued or in flight, reusing `_in_flight_partitions`. That closes the
   real race window, and its worst case is bounded by a run's duration rather
   than by human intervention — without run storage the helper reports nothing in
   flight and the repair term covers it.

Together: (3) makes a hole non-permanent, the deferral makes a wrong diff rare,
and the repair term makes it temporary. None of the three can stop the changelog
stream.

### Keys and labels

The build number stays the key everywhere — ordering, predecessor, deferral,
detection. There are three release dates for a build (the index `last_modified`
that `everef list` reports, the archive's internal `releaseDate`, and the
`year=/month=/day=` in `done_path`) and they demonstrably disagree: that
divergence is exactly why 2960198 is excluded by number rather than by an era
date. A date is also not unique per build and describes upstream publication,
not commit order, which is the axis both the race and the detection query turn
on.

Carry the date as a **label**: `sde_build_discovery_sensor` already fetches
`release_date` and discards it — put it in the log lines and the materialisation
metadata, and report build plus date in the stale-Gold log. Take it from
`everef list` or the ingest status, never by parsing `done_path`; that layout is
corpus's to own.

### For the ADR

State plainly that `sde-changelog` Gold is no longer write-once. That is
precedent (killmails, corpus ADR-0060) but it changes the `parquet_sha256` a
consumer may have recorded, so it belongs in the record rather than arriving as a
surprise.

Before the row lands, run the query above once against the NAS run-state: any
rows are changelog partitions already built across a hole, and they need a
rematerialise, because after the fix they are no longer proposed on their own.
