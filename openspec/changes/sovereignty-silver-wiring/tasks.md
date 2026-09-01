## 1. Resolve the sovereignty partition starts

- [ ] 1.1 Add `_tenure_lookback` to `src/eve_industry_orchestration/defs/config.py`, shaped like the existing `_flat_lookback`: read the derivative's `tenure` block, raise `PartitionConfigError` when it is absent or `tenure_lookback_days` is missing or not a positive integer, return the day count. Verify by reading the function beside `_flat_lookback` and by the unit test in 1.5.
- [ ] 1.2 Extend `_lookback_for_shape` in the same file so `sov-ownership` and `sov-events` resolve through `_tenure_lookback`. Verify with `uv run pytest tests/test_config.py -q`.
- [ ] 1.3 Extend `_lookback_for_shape` so `sov-adm` resolves through the existing `_flat_lookback` arm — reuse the `flat-multi-horizon` body, do not duplicate it. Verify with `uv run pytest tests/test_config.py -q`.
- [ ] 1.4 Extend `_lookback_for_shape` so `sov-contests` and `sov-panel` resolve to a zero reach-back, the rule `structures-snapshot` already carries. Verify with `uv run pytest tests/test_config.py -q`.
- [ ] 1.5 Add config tests asserting the resolved Silver start of `sovereignty-map`, `sovereignty-structures` and `sovereignty-campaigns` against the dates the corpus dataset YAML implies — not merely that resolution no longer raises. Include a test pinning that `sov-panel` contributes a zero reach-back, so its `panel.flip_window_days` can never be read as a Silver look-back. Verify with `uv run pytest tests/test_config.py -q`.
- [ ] 1.6 Add a config test asserting `_tenure_lookback` raises `PartitionConfigError` on a derivative whose `tenure` block is absent or malformed. Verify with `uv run pytest tests/test_config.py -q`.

## 2. Teach the fake corpus binary the three datasets

- [ ] 2.1 Add `sovereignty-map`, `sovereignty-structures` and `sovereignty-campaigns` to the dataset tables in `tests/fake_corpus.py` — the `_DERIVATIVES` mapping with their five Gold derivative names, and the dataset-name lists that drive `ingest`, `verify`, `everef missing-partitions` and `state query`. Verify with `uv run pytest tests/ -q -k fake_corpus` and by materialising one partition in task 3.4.
- [ ] 2.2 Verify the fake binary answers `everef missing-partitions --dataset sovereignty-map --format json` and `state query` for a `date=` key of each of the three datasets, in the same JSON shape the landed datasets use. Verify by the sensor tests in 4.2.

## 3. The three Silver assets

- [ ] 3.1 Consult the `dagster-expert` skill before writing any asset in this group; every task here adds a Dagster definition, and it tracks the installed Dagster version.
- [ ] 3.2 Add `src/eve_industry_orchestration/defs/sovereignty_map.py` with a day-partitioned `sovereignty_map_silver` asset: partitions from `resolve_partition_starts`, `pool="everef_download"`, `corpus ingest` then `corpus verify --tier silver` on success only, returning a `MaterializeResult` merged with `corpus.partition_metadata(...)`. Mirror `defs/system_jumps.py`, not `market_history_silver`'s `incomplete` branch. Verify with `uv run pytest tests/ -q` and by reading the module beside `defs/system_jumps.py`.
- [ ] 3.3 Add `defs/sovereignty_structures.py` and `defs/sovereignty_campaigns.py` with `sovereignty_structures_silver` and `sovereignty_campaigns_silver`, identical in shape to 3.2. Verify with `uv run pytest tests/ -q`.
- [ ] 3.4 Add asset tests against the fake binary covering, per dataset: a successful ingest is followed by verify for the same dataset and date; a failing ingest fails the run and never invokes verify; the run-state `rows`, `retention_class` and `parquet_sha256` are recorded; a missing or failing run-state read still succeeds and warns. Verify with `uv run pytest tests/ -q`.
- [ ] 3.5 Add one test per dataset materialising a date before `2022-12-16` and one after, asserting the ingest command is identical in shape across the layout-era boundary — no Python branch on the era. Verify with `uv run pytest tests/ -q`.

## 4. The three availability sensors

- [ ] 4.1 Consult the `dagster-expert` skill before writing the sensors, then add three availability sensors to `src/eve_industry_orchestration/defs/sensors.py`, mirroring `market_history_availability_sensor`: `corpus.everef_missing_partitions(DATASET)` to `request_partitions(...)` with a `run_key_prefix`, capped by `sensor_util.MAX_PARTITIONS_PER_TICK`. Verify by reading them beside the landed sensor and with `uv run pytest tests/test_sensors.py -q`.
- [ ] 4.2 Add sensor tests against the fake binary covering, per dataset: missing partitions are requested; nothing missing requests nothing; a backlog longer than the per-tick cap requests at most the cap and leaves the rest reported missing on the next tick. Verify with `uv run pytest tests/test_sensors.py -q`.
- [ ] 4.3 Confirm the three assets and three sensors are picked up by the code location's definitions — no manual registration is missing. Verify with `uv run pytest tests/ -q` and whichever existing test enumerates the definitions.

## 5. Verify the whole change

- [ ] 5.1 Confirm `deploy/dagster.yaml` and `tests/test_concurrency_pools.py` are unmodified and `EXPECTED_POOLS` still passes — the three assets join `everef_download` and declare no new pool. Verify with `git diff --stat` showing neither file and `uv run pytest tests/test_concurrency_pools.py -q`.
- [ ] 5.2 Run the full gate: `uv run ruff check . && uv run ruff format --check . && uv run pytest -q`.
