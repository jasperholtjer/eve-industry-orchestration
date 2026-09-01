## 1. The read

- [x] 1.1 Add the run-state key helpers to
  `src/eve_industry_orchestration/defs/corpus_resource.py` — `date_key`,
  `build_key`, `month_key` and a `LATEST_KEY` constant, producing the
  `date=<iso>` / `build=<n>` / `month=<yyyy-mm-01>` / `latest` forms corpus
  writes. Verify against the fixture keys in `tests/fake_corpus.py` (lines
  578, 678, 784, 890) and by task 1.4's test.
- [x] 1.2 Add `CorpusResource.partition_metadata(dataset, tier, partition_key)`
  in the same module: one `state_query` over the run-state `partitions` table
  returning `rows`, `retention_class` and `parquet_sha256` for that triple, as
  a `dict[str, Any]` ready to merge into `MaterializeResult` metadata.
  `partition_key` is the run-state key from 1.1, never a bare Dagster key. The
  SQL interpolates its values the way the other run-state queries in this
  module already do; do not introduce parameter binding for one query. Verify
  by reading the method and by task 1.4's test.
- [x] 1.3 Make it advisory: no matching row, a non-zero exit, a timeout or
  unparseable output all return an empty mapping and log at warning with the
  dataset, tier and key. It never raises. Verify by task 1.4's test.
- [x] 1.4 Extend `tests/fake_corpus.py` so its `state query` branch answers the
  new SQL from per-partition fixture state (`rows`, `retention_class`,
  `parquet_sha256`), and add tests in `tests/` covering: a partition found
  under each of the four key schemes using the fake's real prefixed fixture
  keys, a zero-row partition, an absent row, and a failing query. The
  scheme test is the one that would catch a bare key matching nothing, so it
  asserts non-empty metadata, not merely that no exception was raised. Verify
  with `uv run --project .worktrees/materialisation-metadata pytest -q tests/`.

## 2. The call sites

Each group merges `partition_metadata(...)` over the existing metadata dict at
every site that records a partition corpus just wrote, keeping the identifying
fields, and builds its key with the 1.1 helper matching that asset's own
partition scheme. Sites recording a skipped build (`"built": False`) and the
serving-load assets in `serving.py` are left alone. Every group consults the
`dagster-expert` skill before touching a Dagster definition.

- [x] 2.1 `market_history.py` — both sites, and delete the two
  `# TODO: enrich metadata from _INDEX.json / corpus state query` markers.
  Verify with `pytest -q -k market_history` and by grepping that neither TODO
  remains.
- [x] 2.2 `market_orders.py`, `system_jumps.py`, `system_kills.py`,
  `structures.py`, `industry_cost_indices.py`. Verify with `pytest -q -k
  "market_orders or system_jumps or system_kills or structures or cost_indices"`.
- [x] 2.3 `sde.py`, `mer.py` — the multi-derivative and per-build sites, whose
  key is a build number or a derivative name rather than a date. Verify with
  `pytest -q -k "sde or mer"`.
- [x] 2.4 `news.py`, `killmails.py`, `transcripts.py` — including the sites that
  build a local `metadata` dict before yielding. Verify with `pytest -q -k
  "news or killmails or transcripts"`.
- [x] 2.5 The `*_live.py` snapshot assets (`market_orders_live.py`,
  `market_prices_live.py`, `industry_cost_indices_live.py`): enrich where the
  asset records a partition registered in run-state, and leave the site alone
  where it does not. State which it was in the commit body. Verify with
  `pytest -q -k live`.

## 3. Close

- [ ] 3.1 Confirm nothing scheduling-shaped moved: `git diff develop --stat`
  shows no change to `defs/config.py`, `defs/sensors.py`, any partition
  definition or `deploy/dagster.yaml`, and
  `pytest -q tests/test_concurrency_pools.py` passes.
- [ ] 3.2 Update `openspec/config.yaml`'s **State of the repository** paragraph,
  which currently reads "Still open: materialisation metadata is static rather
  than read from `_INDEX.json`", and the matching work item in `ROADMAP.md`.
  Verify by reading both.
- [ ] 3.3 Green gate in the worktree: `uv run ruff check .`,
  `uv run ruff format --check .`, `uv run pytest -q`.
