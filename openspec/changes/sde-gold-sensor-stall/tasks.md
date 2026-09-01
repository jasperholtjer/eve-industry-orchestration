## 1. The shared request loop

- [ ] 1.1 Add an optional `sort_key` parameter to
  `sensor_util.request_partitions`, defaulting to `None` so the existing
  date-keyed callers keep the current lexical order, and pass it to the internal
  `sorted()`. Verify by reading
  `src/eve_industry_orchestration/defs/sensor_util.py` — no call site outside
  `sensors.py` changes — and by `uv run pytest -q`, which must stay green
  without any test edit for the date-keyed sensors.
- [ ] 1.2 Update the `request_partitions` docstring and the module docstring so
  they describe partition keys rather than dates only, and say why SDE needs a
  numeric key. Verify by reading the same file.

## 2. The corpus surface used by the sensor

- [ ] 2.1 Consult the `dagster-expert` skill before touching the sensor
  definition; this task group changes a `@dg.sensor`. Verify by naming, in the
  commit body, what it said about the installed Dagster version's sensor and
  `SensorResult` API.
- [ ] 2.2 Teach `tests/fake_corpus.py` to answer a `state query` for committed
  changelog Gold — `tier = 'gold' AND dataset = 'sde-changelog'` — from the
  `state["sde_gold"]["sde-changelog"]` list it already maintains, returning rows
  shaped `{"dataset", "tier", "partition_key": "build=<n>"}` to match
  `corpus-cli/src/sde.rs::commit_gold`. Verify with a test that ingests a build,
  builds its changelog, and asserts the query reports it.

## 3. The sensor

- [ ] 3.1 Rewrite `sde_gold_sensor` to read both run-state queries, drop builds
  whose changelog Gold is committed, drop the baseline build (no smaller build
  has committed Silver), and hand the remainder to `request_partitions` with
  `sort_key=int`, `asset_key=sde.sde_changelog_gold.key` and label `sde-gold`.
  Verify with `uv run pytest -q tests/test_sde.py`.
- [ ] 3.2 Rewrite the sensor's docstring to state the readiness rule it now
  implements and why the baseline is excluded, replacing the sentence that
  claims `run_key` dedup is what keeps a build from re-queuing. Verify by
  reading `src/eve_industry_orchestration/defs/sensors.py`.

## 4. Tests

- [ ] 4.1 Rewrite the two existing Gold-sensor tests in `tests/test_sde.py`,
  which assert the old static run keys `sde-changelog-100` /
  `sde-changelog-200`, to assert the new behaviour: with two committed Silver
  builds and no Gold, only the newer build is requested. Verify with
  `uv run pytest -q tests/test_sde.py`.
- [ ] 4.2 Add a regression test for the stall itself: with more outstanding
  builds than `MAX_PARTITIONS_PER_TICK`, a first tick requests the cap, and after
  those builds' Gold is committed a second tick requests the next ones. Verify
  with `uv run pytest -q tests/test_sde.py`.
- [ ] 4.3 Add tests for the remaining spec scenarios — a build with committed
  Gold is not re-requested; a cold corpus with one build requests nothing; a
  build ingested below the previous baseline makes the old baseline outstanding;
  numeric ordering puts build 99 before build 100; a still-outstanding build is
  requested again on a later tick with a distinct run key. Verify with
  `uv run pytest -q tests/test_sde.py`.

## 5. Verification

- [ ] 5.1 `uv run ruff check .`, `uv run ruff format --check .` and
  `uv run pytest -q` are all green in the worktree.
- [ ] 5.2 `openspec validate sde-gold-sensor-stall --strict` passes.
