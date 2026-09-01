## Context

See proposal.md — Why, and `docs/questions/2026-09-01-sde-build-sequence-holes.md`,
whose Answer section this change implements without reopening it. The constraints
that shape the approach:

- Run-state is reachable only through `corpus state query`, which takes an
  arbitrary SQL string over the `partitions` table. Both facts this change needs
  are already rows there: `sde` Silver commits `dataset = 'sde'`,
  `partition_key = 'build=<n>'`; the changelog commits `dataset = 'sde-changelog'`,
  `tier = 'gold'`, the same key. No new corpus surface is required, and none may
  be invented here.
- `sensor_util` owns the retry-safe `run_key` and the in-flight guard so they stay
  identical across datasets. A second copy in `sensors.py` defeats the module.
- `sensor_util.request_partitions` returns a `SensorResult` without
  `dynamic_partitions_requests`. The discovery sensor is the only writer of
  `sde.build_partitions`, so it must reassemble the result rather than return the
  helper's directly.
- `sde_changelog_gold` is `output_required=False`; a baseline build finishes green
  without materialising, by design (corpus ADR-0032).

## Goals / Non-Goals

**Goals:**

- A hole in the build sequence heals on its own rather than becoming permanent.
- A changelog built across a hole is repaired on a later tick.
- The race that produces one is narrowed at its source, without any rule that can
  stall the changelog stream.

**Non-Goals:**

- A second sensor. Both Gold terms target `sde_changelog_gold`, so they are a
  union in one outstanding set.
- Any new corpus subcommand, JSON shape or Gold tree.
- A suppression path for a build that will never ingest. `skipped_partitions`
  (corpus ADR-0028) answers "this Silver will never exist" and is asked of a
  Silver dataset; nothing here needs it, and the deferral rule is deliberately
  built so that such a build costs a retry per tick rather than a stall.
- The non-partitioned SDE Gold assets (snapshot, industry-products, facilities,
  hubs). They are schedule-driven and unaffected.

## Decisions

**Defer on a lower run in flight; do not block on the registered sequence.** The
rejected alternative — a build is outstanding only when the largest *registered*
build below it has committed Silver — is correct as a rule and still wrong to
adopt, for three reasons taken from the answered question:

- Its only maintenance path defeats it. The rule reads the Dagster
  dynamic-partitions store, so clearing a stuck build's key — the obvious operator
  action — silently releases every build above it to diff across the resulting
  hole. A correctness rule whose repair procedure produces the defect it prevents
  is not a rule.
- It compounds with the discovery fix instead of cancelling it. "The stall becomes
  temporary because holes now heal" assumes a failed Silver is transient; the one
  documented SDE failure, `skip_builds: [2960198]`, is a structurally incomplete
  archive and permanent. That build would stop every later changelog for good.
- It moves the "sensor rule is not the binary rule" defect rather than removing
  it. The binary reads committed Silver from run-state; the blocking rule would
  read Dagster's partition store — two stores joined by an unenforced assumption
  about discovery order.

Against that, the damage it prevents is a diff over a wider interval that
correctly labels itself, one overlapping partition and one absent link, reachable
only while two pooled Silver runs are genuinely in flight. A silent, dataset-wide
stall is the worse failure. The deferral buys most of the same protection with a
worst case bounded by a run's duration.

**Detect staleness on the nearest lower Silver only.** A superset — "any lower
Silver committed after this Gold" — would flag every changelog above a re-ingested
old build and produce a rebuild storm with a cascade behind it. The nearest lower
build is the one the binary would actually pick, which makes the query exact. A
baseline changelog has no lower Silver, so the correlated subquery yields NULL and
the row drops out without a special case.

**Fold the stale term into `sde_gold_sensor` rather than add a repair sensor.**
`killmails_consumption_gold_repair_sensor` is a second sensor because its two
terms target different assets. Both terms here target `sde_changelog_gold`, so a
second sensor would duplicate the cap, the in-flight guard and the deferral, and
the two would race each other for the same partition. One union in the existing
outstanding set has none of that.

**Detection lives in `corpus_resource.py`, not in the sensor.** `stale_gold_dates`
already establishes that the SQL for a run-state question belongs to the resource
and the sensor gets a typed list back. Following it keeps the only SQL in this
repository in one module and keeps the sensor readable as a rule.

**Repair is a plain rematerialise, and therefore the changelog is no longer
write-once.** Gold overwrites in place and the binary recomputes the predecessor
from currently committed Silver, so a flagged partition needs nothing but the
normal run. That changes a `parquet_sha256` a consumer may have recorded, which is
why it is written down as ADR-0001 rather than left implicit. It is precedent
rather than novelty — killmails does the same, and corpus ADR-0060 covers drift
repair — but the consumer-visible half is new for this dataset.
`docs/serving-seam.md` records that every serving load is idempotent on
`parquet_sha256`, so a repaired partition re-loads rather than corrupts.

**The build number is the key; the release date is a label.** There are three
release dates for a build — the index `last_modified` from `everef list`, the
archive's internal `releaseDate`, and the `year=/month=/day=` in `done_path` — and
they demonstrably disagree; that divergence is why 2960198 is excluded by number
rather than by date. A date is also not unique per build and describes upstream
publication rather than commit order, which is the axis both the race and the
detection query turn on. The date is carried on log lines and the run request so a
permanently failing build is visible rather than silent, taken from the listing
the sensor already fetches. The run request carries it as a tag and `sde_silver`
surfaces that tag as materialisation metadata, which is what "carried as a label"
has to mean if a reader is to find the date on the partition rather than only in
a log. That is the one asset-side line this row adds; it moves no compute, reads
nothing from the contract, and is dropped silently when the tag is absent.

The stale-Gold log line names the build number and not the date, which narrows
the answered question's "report build plus date". `sde_gold_sensor` never calls
`everef list`, so a date there would have to come from the run-state row or from
`done_path`, and the second is corpus's layout to own. The build number is the
key the operator needs; adding an upstream fetch to a Gold tick to decorate a log
line is not proportionate.

**Discovery reassembles the `SensorResult`.** `request_partitions` gives the run
requests and the cursor; the discovery sensor adds its own
`dynamic_partitions_requests` for the keys it has just discovered. The eligible
set is registered together with newly discovered, so a build registered on an
earlier tick whose ingest failed is reconsidered — which is the whole point of
the part.

## Risks / Trade-offs

- **A permanently failing build is now retried every tick.** → Self-limiting in
  the same way as every other dataset: the moment corpus commits the partition it
  leaves the outstanding set. A visible, repeating run failure is strictly better
  than today's silent, permanent hole, and the per-tick cap bounds the cost. The
  log line naming the build and its release date is what makes it visible.
- **The deferral can starve a build while Silver ingests churn.** → Bounded by a
  run's lifetime, not by intervention, and the build stays in the outstanding set
  throughout. Where run storage reports nothing in flight — unit contexts — the
  deferral is a no-op and the stale term covers the case it would have prevented.
- **The stale query is correlated and runs every Gold tick.** → It is one query
  over the `partitions` table, the same table the sensor already reads twice, and
  SDE build counts are in the hundreds. `stale_gold_dates` runs the same shape per
  tick for killmails.
- **A repaired changelog changes bytes a consumer recorded.** → ADR-0001, and
  serving's load is idempotent on `parquet_sha256` by design.
- **Existing partitions already built across a hole are not repaired by deploying
  this.** → They are, by the stale term, on the first tick after deploy — that is
  what the term is for. The question file's one-off check against the NAS
  run-state remains worth running to know how many there are; it is an operator
  action, not a code path.
