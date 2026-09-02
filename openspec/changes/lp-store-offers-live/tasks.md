## 1. The live asset

- [x] 1.1 Consult the `dagster-expert` skill before writing the asset and the
  schedule below — both are Dagster definitions and must be written against the
  installed version, not from memory. Run the
  `add-dataset-to-orchestration` skill for the touchpoints rather than
  reassembling them.
- [x] 1.2 Add `src/eve_industry_orchestration/defs/lp_store_offers_live.py`
  with **one** non-partitioned `lp_store_offers_live_gold` asset mirroring
  `defs/market_prices_live.py`: `kinds={"corpus"}`, its own `group_name`, no
  `pool=`, no `partitions_def`, no `deps=`. Verify by reading the file.
- [x] 1.3 Shell `corpus live build --dataset lp-store-offers-live --sink-path
  <sink>` through `CorpusResource.run` — `--sink-path` is an option of `live
  build`, not a global flag — and return a `dg.MaterializeResult` carrying
  `dataset`, `tier`, `partition`, the run-level keys the binary reports
  (`snapshot_at`, `source`, `corporations`, `empty_stores`) copied only when
  present, and one row count per entry of the status object's `partitions`
  list, keyed on that entry's `derivative`. Verify by reading the module that
  no path is constructed and no parquet library is imported.
- [x] 1.4 Carry the reasons in the module docstring and at the point they bind:
  why one asset rather than one per Gold tree, why a schedule replaces the
  availability sensor, why no pool, why the status object is read as
  multi-partition, and why there is no `partition_metadata` enrichment and no
  `corpus verify` call. Verify by reading the file.

## 2. The schedule

- [x] 2.1 Add a default-stopped `dg.ScheduleDefinition` at `30 11 * * *` in
  `defs/sensors.py`, targeting the new asset, beside the sibling live
  schedules, with the measured expiry that fixes the hour in a comment. Verify
  by reading the definition.

## 3. Fake binary and tests

- [x] 3.1 Extend `_do_live` in `tests/fake_corpus.py` with an
  `lp-store-offers-live` branch: write both `gold/lp-store-offers/current/` and
  `gold/lp-store-offer-items/current/`, and print the multi-partition status
  object ADR-0070 §3 pins — `corporations`, `empty_stores`, `snapshot_at`,
  `source`, `url` and a `partitions` list of `derivative` / `rows` /
  `parquet_sha256` / `partition_dir`. Leave the existing branches unchanged and
  verify with `uv run --project <worktree> pytest -q
  tests/test_market_prices_live.py tests/test_public_contracts_live.py
  tests/test_market_orders_live.py`.
- [x] 3.2 Add `tests/test_lp_store_offers_live.py` mirroring
  `tests/test_market_prices_live.py`: the asset writes both `current/` trees
  from one invocation and returns a row count per derivative; it is
  non-partitioned; it declares no pool; the schedule's cadence, target and
  default status; and no run-state query is issued (monkeypatch
  `CorpusResource.state_query` to fail).
- [x] 3.3 Add a test that the binary being invoked **once** is what produces
  both trees, and one that a status key the binary omits is left out of the
  metadata rather than defaulted.

## 4. The real run

- [ ] 4.1 Materialise `lp_store_offers_live_gold` once in a scratch Dagster
  instance against the real `corpus` binary — `DAGSTER_HOME` and
  `CORPUS_SINK_PATH` under `C:\tmp\orchestration-scratch\lp-store-offers-live`,
  `Y:\` read and never written. Record the row counts, the corporation counts
  and the wall time for the reviewer, and confirm both `_INDEX.json` files
  carry the same `run_id` — that equality is what says the two trees came from
  one fetch.

## 5. Documentation

- [x] 5.1 Add the dataset where `README.md` enumerates the live snapshot
  family, naming what makes it the odd one: two trees from one fan-out, daily
  rather than the siblings' half-hourly/hourly, and no pool. Verify by reading
  the enumeration.
- [x] 5.2 Update the **State of the repository** paragraph in
  `openspec/config.yaml`, which reads as though `public-contracts-live` is the
  newest thing here.

## 6. Verification

- [ ] 6.1 Run `uv run --project <worktree> ruff check .`, `ruff format --check
  .` and `pytest -q` — all three green, with `tests/test_concurrency_pools.py`
  unchanged and passing, since no pool is added.
