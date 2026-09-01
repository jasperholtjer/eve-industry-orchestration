## 1. Fake corpus binary

- [ ] 1.1 Confirm `tests/fake_corpus.py` already answers `gold build --derivative`
  and `gold ready-dates --derivative` for all five sovereignty derivative names,
  and add whatever is missing; verify by driving each of the five names through the
  fake binary in a test that asserts a written partition
- [ ] 1.2 Make the fake binary able to report a skipped day for the panel — the
  `status: "skipped"` build result with a reason — under a fixture switch, and
  verify a test can obtain both a written partition and a skipped day for the same
  derivative

## 2. Per-dataset Gold assets

- [ ] 2.1 Add `sovereignty_adm_gold` to `defs/sovereignty_structures.py`: its own
  daily partitions from `resolve_partition_starts(DATASET, ADM_DERIVATIVE).gold`,
  `output_required=False`, the skipped-day observation branch, Gold-tier verify,
  and a derivative-keyed `MaterializeResult`; verify the asset materialises a date
  against the fake binary and that its partition start matches the derivative's
  configured served start
- [ ] 2.2 Add `sovereignty_contests_gold` to `defs/sovereignty_campaigns.py` in the
  same shape; verify the same two ways
- [ ] 2.3 Add `sovereignty_ownership_gold` and `sovereignty_changes_gold` to
  `defs/sovereignty_map.py`, each with its own derivative constant and partitions
  definition; verify both materialise against the fake binary and that the two
  builds are invoked with their own `--derivative` and neither produces the
  other's partition

## 3. The assembled panel

- [ ] 3.1 Add `sovereignty_panel_gold` to `defs/sovereignty_map.py` with
  `deps=[sovereignty_ownership_gold, sovereignty_changes_gold,
  sovereignty_structures.sovereignty_adm_gold,
  sovereignty_campaigns.sovereignty_contests_gold, sde.sde_snapshot_gold]` and its
  own partitions definition; verify the definitions load with no import cycle and
  a test asserts the five dependency keys
- [ ] 3.2 Verify the panel's partition start is the configured 2022-01-31 while its
  siblings start 2022-01-01, with no literal date anywhere in `defs/` — a test that
  reads both starts and a grep-style assertion over the sovereignty modules
- [ ] 3.3 Verify the non-partitioned SDE dependency carries lineage only: a test
  that materialises a panel partition against the fake binary without providing any
  SDE partition

## 4. Readiness sensors

- [ ] 4.1 Add a Gold sensor factory for the sovereignty family to `defs/sensors.py`,
  parameterised on dataset, derivative, asset and partitions definition, mirroring
  `_build_orderbook_gold_sensor`; verify one sensor built from it requests the
  dates corpus reports ready
- [ ] 4.2 Build the five sensors from it; verify each polls with its own
  `--derivative`, targets only its own asset, and requests nothing when corpus
  reports no ready dates
- [ ] 4.3 Verify the shared per-tick fan-out cap and the partition-range filter
  hold for a sovereignty Gold sensor: a backlog longer than the cap is truncated
  and carried, and a ready date before the derivative's start is not requested

## 5. Gate behaviour

- [ ] 5.1 Verify the skipped-day gate for the panel: a build reporting
  `status: "skipped"` yields no materialisation, no Gold-tier verify call, a
  successful run and an observation naming the reason
- [ ] 5.2 Verify the incomplete-window gate is not a skip: a build reporting a
  written partition alongside an incomplete flip window materialises and verifies
  like any other date, and no code path in the assets inspects window coverage
- [ ] 5.3 Verify a failing build fails the materialisation without invoking the
  Gold-tier verify

## 6. Records and checks

- [ ] 6.1 Verify a successful sovereignty Gold materialisation records the
  run-state row count, retention class and checksum read under the derivative's
  name, and that a missing or failing run-state read still succeeds and warns
- [ ] 6.2 Verify `tests/test_concurrency_pools.py` is still green with no edit —
  no sovereignty Gold asset declares a pool and the declared pool set is unchanged
- [ ] 6.3 Run `uv run ruff check .`, `uv run ruff format --check .` and
  `uv run pytest -q` in the worktree and verify all three are green

## 7. Documentation

- [ ] 7.1 Update `ROADMAP.md` where this row changes what it claims about the
  sovereignty family's Gold trees, and the **State of the repository** paragraph in
  `openspec/config.yaml` where it says the family is Silver-only; verify by reading
  both back against what the row landed
