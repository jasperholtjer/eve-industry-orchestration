## 1. `structures_silver` — add the reachable `incomplete` branch

- [ ] 1.1 Consult the `dagster-expert` skill before touching this Dagster
      asset definition.
- [ ] 1.2 In `src/eve_industry_orchestration/defs/structures.py`, add an
      `incomplete` branch to `structures_silver` mirroring
      `defs/public_contracts.py#public_contracts_silver` (lines 113-127):
      `yield dg.AssetObservation(asset_key=..., partition=date,
      metadata={"skip_reason": "upstream_incomplete", "detail": ...})` then
      `return`, placed after the existing `skipped` branch and before the
      unconditional `corpus.run("verify", ...)`. Update the function's
      docstring to name the ADR-0064 `PublicationFrontier` path (declared
      `member_suffix: .v2.json.bz2`) as the reachable producer. Verify: `uv
      run ruff check src/eve_industry_orchestration/defs/structures.py` and
      `uv run ruff format --check` pass.
- [ ] 1.3 In `tests/test_structures.py`, add a case that sets
      `FAKE_INCOMPLETE_DATES` to a partition date and asserts the run
      succeeds with the partition left Missing and an `AssetObservation`
      carrying `skip_reason: upstream_incomplete` — mirror however
      `tests/test_public_contracts.py` already asserts the equivalent case
      for `public_contracts_silver`. Verify: `uv run pytest
      tests/test_structures.py -q` passes.
## 2. `killmails_silver` — add the reachable `incomplete` branch

- [ ] 2.1 Consult the `dagster-expert` skill before touching this Dagster
      asset definition.
- [ ] 2.2 In `src/eve_industry_orchestration/defs/killmails.py`, add the same
      `incomplete` branch shape to `killmails_silver` (after the existing
      `skipped` branch, before `corpus.run("verify", ...)`, preserving the
      existing `freshness_token` handling that follows verify). Update the
      docstring to name the ADR-0028 (2026-09-01 `daily-tar-of-json`
      extension) `classify_absent_date` → `IndexVerdict::NotYetPublished`
      path as the reachable producer. Verify: `uv run ruff check
      src/eve_industry_orchestration/defs/killmails.py` and `uv run ruff
      format --check` pass.
- [ ] 2.3 In `tests/test_killmails.py`, add the equivalent
      `FAKE_INCOMPLETE_DATES` case asserting Missing + the
      `upstream_incomplete` `AssetObservation`. Verify: `uv run pytest
      tests/test_killmails.py -q` passes.
## 3. Seven unreachable siblings — docstring only, no branch

- [ ] 3.1 Consult the `dagster-expert` skill before touching these Dagster
      asset definitions (docstring-only, but they remain Dagster asset
      definitions).
- [ ] 3.2 Add one docstring sentence to the Silver function's docstring in
      each of `defs/system_jumps.py`, `defs/system_kills.py`,
      `defs/market_orders.py`, `defs/industry_cost_indices.py`,
      `defs/sovereignty_map.py`, `defs/sovereignty_structures.py`,
      `defs/sovereignty_campaigns.py`, mirroring
      `defs/market_history.py#market_history_silver`'s "unreachable rather
      than protective" sentence: this dataset's `hourly-folder(-tar)` layout
      declares no `member_suffix`, so `corpus`'s
      `FolderEmptiedByDeclaredSuffix` → `PublicationFrontier` path never
      fires and every non-`skipped` failure is either a clean
      `UpstreamAbsent` skip or a fatal error — an `incomplete` branch here
      would be dead code. ONE sentence per module, not a paragraph: the full
      reasoning lives in this row's spec and in the two modules that do carry
      the branch. No code branch is added. Verify: `uv run ruff
      check .` and `uv run ruff format --check .` pass (docstring-only diff,
      no behaviour change) and `uv run pytest -q` still passes unchanged.

## 4. The run

- [ ] 4.1 Against the **real** `corpus` binary
      (`../eve-industry-corpus/target/release/corpus.exe`, rebuilt 2026-09-02
      from corpus develop), in a scratch Dagster instance — `DAGSTER_HOME` and
      `CORPUS_SINK_PATH` under
      `C:\tmp\orchestration-scratch\silver-incomplete-status-family`, `Y:\`
      read and never written — materialise one ordinary partition of
      `structures_silver` and one of `killmails_silver`. This proves the two
      changed functions still work end to end on the path they take every day;
      pick recent dates that are actually served, and allow killmails time, it
      is the corpus's largest Silver.
- [ ] 4.2 The `incomplete` branch itself cannot be reached against the real
      binary on demand — it needs an upstream day that is genuinely mid-
      publication, which is not reproducible. Drive it against the fake binary
      instead: `CORPUS_BINARY_PATH` at `tests/fake_corpus.py` with
      `FAKE_INCOMPLETE_DATES` set to the partition's date, for each of the two
      assets. Verify the run succeeds, the partition is left Missing rather than
      Failed, and the log carries the `upstream_incomplete` observation. Report
      both halves separately and say plainly which ran against which binary — a
      fake-binary run reported as a real one is worse than no run.

## 5. Whole-row verification

- [ ] 5.1 Run `uv run ruff check . && uv run ruff format --check . && uv run
      pytest -q` across the full repo and confirm all pass — no fake-binary
      change was needed (`tests/fake_corpus.py`'s `FAKE_INCOMPLETE_DATES`
      check in `_do_ingest` is already dataset-generic).
