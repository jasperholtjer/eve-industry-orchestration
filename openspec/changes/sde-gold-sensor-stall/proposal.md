## Why

Implements roadmap row `sde-gold-sensor-stall`.

`sde_gold_sensor` is the only readiness sensor in this repository that does not
go through `sensor_util.request_partitions`, and it stalls. It builds its
eligible set from *every* build with committed Silver, never subtracting the
builds whose `sde-changelog` Gold is already committed, then truncates that
ever-growing list to `MAX_PARTITIONS_PER_TICK` (10) — and keys each request on a
static `run_key`. Dagster dedups a used `run_key` permanently, so from the second
tick the sensor requests nothing that can launch: build 11 onward is never
reached, and a build whose Gold run failed is never retried. That second half is
the failure mode `sensor_util`'s own docstring records as having stalled
market-history from 2026-06-27; SDE simply never got the fix.

## What Changes

- `sde_gold_sensor` reads a second run-state query — committed `sde-changelog`
  Gold partitions — and subtracts them from the committed-Silver builds *before*
  the per-tick cap, so the cap bounds the work left rather than the work ever
  done. This mirrors `sde_build_discovery_sensor`, which already subtracts what
  it has handled before capping.
- The baseline build — the lowest committed Silver build, the one with no
  committed predecessor — is left out of the eligible set. The binary skips it
  and writes nothing (ADR-0032), so it can never leave the pending set on its
  own and would otherwise be re-requested on every tick once run keys rotate.
  The binary still owns the decision; the sensor only declines to queue a run it
  knows is a no-op, which is the pre-check pattern the Gold gate already uses.
- The remaining builds are requested through `request_partitions`, which the SDE
  sensor previously bypassed. That gives it the rotating, retry-safe `run_key`
  and the in-flight guard every other Gold sensor has, in one implementation
  rather than a second copy.
- `request_partitions` gains an optional `sort_key`. Its `sorted()` is lexical,
  which is wrong for build numbers (`"100" < "200" < "99"`); SDE passes `int`.
  Every existing caller keys on dates, where the default is already correct, and
  is untouched.
- No compute, parsing or validation moves into Python: both facts come from
  `corpus state query` over the run-state `partitions` table, which the sensor
  already uses.

## Capabilities

### New Capabilities

- `sde-gold-readiness`: which SDE builds the orchestrator requests an
  `sde-changelog` Gold run for, how it decides a build is still outstanding, and
  how those requests survive a failed or skipped run.

### Modified Capabilities

<!-- None: openspec/specs/ is empty, this is the repository's first spec. -->

## Impact

- `src/eve_industry_orchestration/defs/sensors.py` — `sde_gold_sensor`.
- `src/eve_industry_orchestration/defs/sensor_util.py` — optional `sort_key` on
  `request_partitions`.
- `tests/fake_corpus.py` — `state query` answers the committed-Gold query for
  `dataset = 'sde-changelog'` from the `sde_gold` state it already keeps.
- `tests/test_sde.py` — the two Gold-sensor tests assert the old static run keys
  and are rewritten; the stall itself gets regression tests.
- No asset joins the `heavy` pool and no asset changes what it shells out to.
  `sde_changelog_gold` still runs `corpus gold build --derivative sde-changelog
  --build <n>` and records that run; only which builds are asked for changes.
