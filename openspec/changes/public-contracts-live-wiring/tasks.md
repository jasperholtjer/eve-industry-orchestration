## 1. The live asset

- [ ] 1.1 Consult the `dagster-expert` skill before writing the asset and the
  schedule below — both are Dagster definitions and must be written against the
  installed version, not from memory.
- [ ] 1.2 Add `src/eve_industry_orchestration/defs/public_contracts_live.py`
  with a non-partitioned `public_contracts_live_gold` asset mirroring
  `defs/market_orders_live.py`: `kinds={"corpus"}`, its own `group_name`, and
  `pool=` the EVE Ref download politeness pool. Verify by reading the file that
  it declares no `partitions_def` and no `deps=`.
- [ ] 1.3 Shell `corpus live build --dataset public-contracts-live --sink-path
  <sink>` through `CorpusResource.run` and return a `dg.MaterializeResult`
  carrying `dataset`, `tier` and `partition` plus the freshness keys the binary
  reports — `snapshot_at`, `snapshot_file`, `date`, `rows` — copied only when
  present. Verify no path is constructed and no parquet is opened by reading the
  module: it imports neither `pathlib` for the tree nor any parquet library.
- [ ] 1.4 Carry the two reasons in the module docstring and at the point they
  bind, as the sibling does: why a schedule replaces the availability sensor for
  a current-overwrite tree, and why no `partition_metadata` enrichment and no
  `corpus verify` call happen here. Verify by reading the file.

## 2. The schedule

- [ ] 2.1 Add a default-stopped `dg.ScheduleDefinition` at `*/30 * * * *` in
  `defs/sensors.py`, targeting the new asset, beside
  `market_orders_live_schedule`. Verify by reading the definition that its
  cadence and `default_status` match the sibling's.

## 3. Fake binary and tests

- [ ] 3.1 Extend `_do_live` in `tests/fake_corpus.py` with a
  `public-contracts-live` branch emitting the status shape the real binary
  prints for the contracts snapshot — a `.v2.tar.bz2` snapshot file, a `date`,
  and a `snapshot_at` — keeping the existing branches unchanged. Verify with
  `uv run --project <worktree> pytest -q tests/test_market_orders_live.py
  tests/test_market_prices_live.py`, which must stay green.
- [ ] 3.2 Add `tests/test_public_contracts_live.py` mirroring
  `tests/test_market_orders_live.py`: the asset writes the `current/` partition
  and returns the freshness metadata including `snapshot_at`; the asset is
  non-partitioned; the schedule's cadence, target and default status; and no
  run-state query is issued (monkeypatch `CorpusResource.state_query` to fail).
  Verify with `uv run --project <worktree> pytest -q
  tests/test_public_contracts_live.py`.
- [ ] 3.3 Add a test that a freshness key absent from the binary's status output
  is omitted from the metadata rather than defaulted, so the advisory rule the
  spec states is pinned. Drive it by monkeypatching `CorpusResource.run` to
  return a status dict without `snapshot_at` — the fake binary gains no knob for
  it, because its job is to mimic the contract the real binary honours, not to
  emit shapes that contract forbids. Verify with the same pytest invocation.

## 4. Documentation

- [x] 4.1 Add one line for `corpus live build` to `ROADMAP.md`'s confirmed
  corpus CLI surface list. One line only — the contract stays in corpus, and
  this row is its fourth caller. Verify by reading the list.
- [x] 4.2 Add the dataset where `README.md` enumerates what this code location
  drives, in the shape the sibling live datasets already use. Verify by reading
  the enumeration.
- [x] 4.3 Update the **State of the repository** paragraph in
  `openspec/config.yaml`, which currently reads as though the sovereignty family
  is the newest thing here. Verify by reading the paragraph.

## 5. Verification

- [ ] 5.1 Run `uv run --project <worktree> ruff check .`, `ruff format --check .`
  and `pytest -q` — all three green, with `tests/test_concurrency_pools.py`
  unchanged and passing, since no pool is added.
