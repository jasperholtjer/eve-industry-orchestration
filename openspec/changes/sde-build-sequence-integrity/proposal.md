## Why

Implements roadmap row `sde-build-sequence-integrity`. The design is settled in
`docs/questions/2026-09-01-sde-build-sequence-holes.md` (status: answered); its
Answer section is binding.

`sde-gold-sensor-stall` fixed which builds `sde_gold_sensor` asks for. It left
standing the older defect underneath: the sensor asks for a build as soon as
*any* smaller build has committed Silver, while the binary diffs against the
**largest committed** Silver build below it (`corpus-cli/src/sde.rs`). Those two
questions differ exactly when the build sequence has a hole, and it can have one:

- `sde_silver` sits in the `everef_download` pool at `default_limit: 2`, so two
  builds ingest at once and 300 can commit before 200. A Gold tick in between
  sees `{100, 300}`, asks for 300, and the binary diffs 300 against **100**. Gold
  then holds an overlapping 100→300 and 100→200, and the 200→300 link never
  exists.
- `sde_build_discovery_sensor` still has the bug the previous row fixed for Gold:
  a static `run_key` per build, filtered against the dynamic partitions it
  registered in the same tick. From the next tick the build counts as known, so a
  failed `sde_silver` run is never re-proposed and the hole is permanent.

Subtracting committed Gold made the wrong diff terminal — once 300's changelog is
committed it leaves the outstanding set for good. Nothing in the repository can
notice it afterwards.

## What Changes

Three parts, one capability. None of them can stop the changelog stream.

- **`sde_build_discovery_sensor` reads readiness from run-state**, the way
  `sde_gold_sensor` now does: registered builds minus builds with committed `sde`
  Silver, handed to `sensor_util.request_partitions` with `sort_key=int` for its
  rotating `run_key` and in-flight guard. A failed ingest is retried on the next
  tick and a hole heals on its own. The helper returns a `SensorResult` without
  `dynamic_partitions_requests`, so discovery reassembles one, with the eligible
  set taken as registered ∪ newly discovered.
- **`sde_gold_sensor` gains a stale term** rather than a second sensor. Both
  terms target `sde_changelog_gold`, so the outstanding set becomes
  `(committed_silver − committed_gold − baseline) ∪ stale_gold`, where
  `stale_gold` is the changelog partitions whose nearest lower committed `sde`
  Silver was committed *after* the changelog itself. That is one read through the
  existing `corpus state query` seam, in the shape
  `killmails_consumption_gold_repair_sensor` already established. Repair needs no
  more than a rematerialise: Gold overwrites in place and the binary recomputes
  the predecessor from currently committed Silver.
- **A Gold build is deferred, not blocked**, while a *lower* build's `sde_silver`
  run is queued or in flight, reusing `sensor_util._in_flight_partitions`. That
  closes the race window at its source and its worst case is bounded by a run's
  duration, never by human intervention. The rejected alternative — requiring the
  largest *registered* build below the target to have committed — is recorded in
  design.md with the three reasons it was rejected.

Build number stays the key for ordering, predecessor, deferral and detection.
`release_date` is carried only as a label: in the discovery sensor's log lines, on
the run request as a tag, and from there onto the `sde_silver` materialisation's
metadata, taken from the `everef list` payload the sensor already fetches and
discards — never parsed from `done_path`, which is corpus's layout to own.

No compute, parsing or validation moves into Python. Every fact is a row in the
run-state `partitions` table read through `corpus state query`, and no new corpus
subcommand or JSON shape is needed.

## Capabilities

### Modified Capabilities

- `sde-gold-readiness`: widened from "which builds get a changelog run" to "which
  builds get a changelog run, against the predecessor the binary will actually
  pick". Gains discovery readiness, the stale-Gold term and the deferral rule;
  its existing in-flight requirement is generalised to both sensors.

## Impact

- `src/eve_industry_orchestration/defs/sensors.py` — `sde_build_discovery_sensor`
  and `sde_gold_sensor`.
- `src/eve_industry_orchestration/defs/corpus_resource.py` — one read-only query
  method for stale changelog builds, mirroring `stale_gold_dates`.
- `src/eve_industry_orchestration/defs/sde.py` — `sde_silver` surfaces the
  release-date tag as materialisation metadata. One label, no compute.
- `tests/fake_corpus.py` — `state query` answers the stale-Gold detection SQL
  from the state it already keeps.
- `tests/test_sde.py` — regression tests for all three parts.
- `docs/adr/0001-sde-changelog-gold-is-rebuildable.md` — new, and the first ADR in
  this repository: `sde-changelog` Gold is no longer write-once, which changes a
  `parquet_sha256` a consumer may have recorded. `ROADMAP.md` is corrected where
  it says ADRs live only in `eve-industry-corpus`.
- No asset changes what it shells out to, no asset joins a pool, and no new
  partition definition appears. `sde_changelog_gold` still runs
  `corpus gold build --derivative sde-changelog --build <n>`; only which builds
  are asked for, and when, changes.
