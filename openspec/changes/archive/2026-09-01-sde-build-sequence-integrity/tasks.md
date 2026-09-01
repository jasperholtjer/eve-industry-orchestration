## 1. The run-state seam

- [x] 1.1 Add a read-only `stale_changelog_builds()` to
  `CorpusResource` in `src/eve_industry_orchestration/defs/corpus_resource.py`,
  mirroring `stale_gold_dates` in shape: it issues the correlated detection SQL
  from `docs/questions/2026-09-01-sde-build-sequence-holes.md` through
  `state_query`, and returns the build numbers as a sorted `list[int]`. The SQL
  compares the changelog partition's `last_seen_at` against the `last_seen_at` of
  the **nearest** lower committed `sde` Silver build; a baseline changelog yields
  NULL and drops out. No path is constructed and no parquet is opened. Verify by
  reading the file and by task 1.2's test.
- [x] 1.2 Teach `tests/fake_corpus.py` to answer that query. It dispatches on
  substrings of the incoming `--sql`, so add a branch alongside the existing
  `dataset = 'sde-changelog'` one that computes the same predicate over the state
  it already keeps, returning rows shaped `{"build": <int>}` (or whatever column
  name 1.1 selects). The fake must record a commit ordering it can compare —
  extend the state it keeps for `sde` Silver and `sde_gold` with the tick or
  sequence number at which each committed, if it does not already have one.
  Verify with a test that ingests 100, builds 300's changelog, then ingests 200,
  and asserts the query reports exactly `[300]`.

## 2. The discovery sensor

- [x] 2.1 Consult the `dagster-expert` skill before touching either sensor
  definition; this group and group 3 change `@dg.sensor` bodies and a
  `SensorResult`. Verify by naming, in the report, what it said about the
  installed Dagster version's `SensorResult` and `dynamic_partitions_requests`.
- [x] 2.2 Rewrite `sde_build_discovery_sensor` in
  `src/eve_industry_orchestration/defs/sensors.py` to take readiness from
  run-state instead of from "not yet registered": the eligible set is the
  registered build keys together with those discovered on this tick, minus the
  builds whose `sde` Silver is committed. Hand the remainder to
  `sensor_util.request_partitions` with `sort_key=int` and the `sde_silver` asset
  key, then reassemble the returned `SensorResult` with a
  `dynamic_partitions_requests` carrying the newly discovered keys — the helper
  does not set that field and the discovery sensor is the only writer of
  `sde.build_partitions`. Verify with `uv run pytest -q tests/test_sde.py`.
- [x] 2.3 Carry `release_date` as a label rather than discarding it: name the
  build and its release date in the sensor's log lines, and attach the date to
  the run request as a tag. Take it from the `everef list` payload the sensor
  already fetches — never from `done_path`. Where a build has no date in the
  payload, log the build alone rather than a placeholder. Verify with a test that
  asserts the tag on the emitted run request.
- [x] 2.4 Surface that tag as materialisation metadata on `sde_silver` in
  `src/eve_industry_orchestration/defs/sde.py`, so the date is readable on the
  partition and not only in a log. Read it from the run's tags, add it to the
  metadata the asset already emits, and omit the key entirely when the tag is
  absent — a manual backfill has no tag and must not fail or emit a placeholder.
  This is a label only: no contract byte is read and nothing is parsed. Verify
  with `uv run pytest -q tests/test_sde.py`.
- [x] 2.5 Rewrite the sensor's docstring to state the readiness rule it now
  implements, why the eligible set includes already-registered builds, and that a
  failed ingest is retried. Verify by reading the file.

## 3. The Gold sensor

- [x] 3.1 Fold the stale term into `sde_gold_sensor`'s outstanding set: it becomes
  `(committed_silver − committed_gold − baseline) ∪ stale_changelog_builds()`.
  One sensor, one union, no second sensor — both terms target
  `sde_changelog_gold`. Verify with `uv run pytest -q tests/test_sde.py`.
- [x] 3.2 Defer, in the same sensor, any outstanding build that has a *lower*
  build's `sde_silver` run queued or in flight, using
  `sensor_util._in_flight_partitions(context, sde.sde_silver.key)`. A deferred
  build stays in the outstanding set and is logged as deferred; readiness must
  not be made to depend on the registered build sequence. Apply the deferral
  before the per-tick cap so a deferred build does not consume a slot. Verify
  with `uv run pytest -q tests/test_sde.py`.
- [x] 3.3 Update the sensor's docstring to state all three terms and the deferral,
  and to say that the changelog is rebuilt in place when its predecessor changed.
  Verify by reading the file.

## 4. Tests

- [x] 4.1 Discovery: a registered build whose Silver never committed is proposed
  again on a later tick with a distinct run key; a build with committed Silver is
  not proposed; a newly discovered build is registered and requested on the same
  tick; build 99 precedes build 100 under a cap of 1; the release date reaches
  the `sde_silver` materialisation metadata, and a run without the tag
  materialises without it. Verify with `uv run pytest -q tests/test_sde.py`.
- [x] 4.2 Stale term: a changelog committed before a lower Silver becomes
  outstanding again; one whose lower Silver all predate it does not; a baseline
  changelog is never reported stale; re-ingesting a build far below flags only the
  changelog whose nearest lower Silver actually changed. Verify with
  `uv run pytest -q tests/test_sde.py`.
- [x] 4.3 Deferral: an outstanding build with a lower `sde_silver` run in flight is
  not requested and the deferral is logged; it is requested on the next tick once
  that run is terminal; an in-flight run for a *higher* build does not defer it; a
  build whose Silver never commits does not stop builds above it. Verify with
  `uv run pytest -q tests/test_sde.py`.
- [x] 4.4 Confirm the existing Gold-sensor tests from `sde-gold-sensor-stall` still
  pass unchanged, or say in the report which one changed and why. Verify with
  `uv run pytest -q tests/test_sde.py`.

## 5. The record

- [x] 5.1 Write `docs/adr/0001-sde-changelog-gold-is-rebuildable.md`, this
  repository's first ADR. It states plainly that `sde-changelog` Gold is no longer
  write-once: a changelog whose predecessor changed is rebuilt in place, which
  changes the `parquet_sha256` a consumer may have recorded. Name the precedent
  (killmails, corpus ADR-0060) and the mitigation already in place
  (`docs/serving-seam.md`: every serving load is idempotent on `parquet_sha256`).
  Keep it to one screen; ADRs here are pruned and rewritten, not append-only.
- [x] 5.2 Record the one-off run-state check in the ADR as an operator note, with
  the detection SQL from the question file and what to do with any rows it
  returns (rematerialise those changelog partitions). The stale term repairs them
  on the first tick after deploy, so the check is confirmation rather than a
  prerequisite — say that, so nobody treats it as a gate. This session could not
  run it: outbound SSH to the Dagster LXC is blocked in its sandbox.
- [x] 5.3 Correct `ROADMAP.md`, which says ADRs live in `eve-industry-corpus`, to
  say that compute and data config do while this repo now records its own
  orchestration decisions under `docs/adr/`. One sentence; do not restructure the
  file.

- [x] 5.4 Update the **State of the repository** paragraph in
  `openspec/config.yaml` where this row changes what it claims — the SDE sensors'
  readiness rule and the arrival of `docs/adr/`. Two sentences at most.

## 6. Verification

- [x] 6.1 `uv run ruff check .`, `uv run ruff format --check .` and
  `uv run pytest -q` are all green in the worktree.
- [x] 6.2 `openspec validate sde-build-sequence-integrity --strict` passes.
