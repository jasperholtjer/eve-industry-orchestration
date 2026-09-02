## 1. `config.py` — teach the four shapes

- [x] 1.1 Add `_CONTRACTS_FOLD_LOOKBACK_DAYS = 0` beside the two existing
      zero-lookback constants and one `_lookback_for_shape` arm covering
      `contract-facts`, `contract-item-facts`, `contract-item-prices` and
      `courier-rates`, with a comment citing corpus ADR-0068's "no cross-day
      state": each of the four folds one day of Silver into one day of Gold, so
      the reach-back is zero — zero, not `None`, because the derivative still
      anchors Silver. Verify: against the real
      `../eve-industry-corpus/datasets` dir in a throwaway `python -c`,
      `resolve_partition_starts("public-contracts", "contract-facts", ...)`
      returns `gold="2021-06-17"` with no `PartitionConfigError`, and the same
      for the other three.
- [x] 1.2 Confirm `resolve_silver_start("public-contracts")` still returns
      `2021-06-17`: with a `gold:` block present the resolver now takes the
      derived branch rather than the Gold-less floor branch added by
      `public-contracts-silver-wiring`, and at zero reach-back the two must
      agree. Do not delete the Gold-less branch — check first whether any
      dataset in `../eve-industry-corpus/datasets` still declares no `gold:`
      block, and report what you found. Verify: `python -c` against the real
      datasets dir.

## 2. `tests/fixtures/datasets/public-contracts.yaml` — stop lagging reality

- [x] 2.1 Add the real `gold:` block — four derivatives, each with the `shape`,
      `sort_keys` and `served_start: 2021-06-17` the corpus YAML declares — and
      drop the stale "no `gold:` block" comment. Copy the shapes and starts
      field-by-field from `../eve-industry-corpus/datasets/public-contracts.yaml`.
- [x] 2.2 Add `test_config.py` cases, reading the **fixture** and never the
      sibling checkout: one per derivative asserting `gold == "2021-06-17"`,
      one asserting `resolve_silver_start` is still the `2021-06-17` coverage
      floor, and one asserting an unknown shape still raises
      `PartitionConfigError` naming the derivative and the shape. Verify:
      `uv run pytest tests/test_config.py -q`.
- [x] 2.3 Add one guard test that **loads the defs folder** in a subprocess
      against `../eve-industry-corpus/datasets`, skipped when that directory is
      absent. Not "resolve every YAML": twelve corpus datasets declare shapes
      `_lookback_for_shape` has never known and are resolved elsewhere, so a
      blanket resolve-all guard is permanently red — and `@definitions` is lazy,
      so importing `definitions.py` alone proves nothing. Loading the defs
      folder is the break's exact shape. Keep it a guard, not the coverage —
      2.2 is the coverage. Proven red with one shape name typo'd, green
      restored.

## 3. `defs/public_contracts.py` — four Gold assets

Consult the `dagster-expert` skill before adding these asset definitions.

- [ ] 3.1 Add a `_build_gold(context, corpus, derivative)` helper mirroring
      `sovereignty_map.py#_build_gold`: shell the corpus Gold build naming that
      derivative and date, branch on a reported skip (leave the partition
      Missing, emit an `AssetObservation`), otherwise run the Gold-tier
      contract verification and yield a `MaterializeResult` carrying `dataset`,
      `derivative`, `tier` and `partition` merged with
      `corpus.partition_metadata(...)`. Follow the sovereignty module's
      advisory-read rule: a missing run-state row warns and yields nothing
      rather than failing a run corpus passed.
- [ ] 3.2 Add a `_gold_start(derivative)` helper and four
      `DailyPartitionsDefinition`s, each from
      `resolve_partition_starts("public-contracts", <derivative>).gold`. No
      literal start dates.
- [ ] 3.3 Add the four `@dg.asset` definitions — `contract_facts_gold`,
      `contract_item_facts_gold`, `contract_item_prices_gold`,
      `courier_rates_gold` — `deps=[public_contracts_silver]`, the module's
      existing `group_name`, `kinds={"corpus"}`, `output_required=False`, and
      **no `pool=`**: no `/usr/bin/time -v` peak exists for any of the four, and
      membership of a memory-bearing pool is by measured peak. They take the
      global cap only, exactly as the sovereignty Gold assets do. Do not touch
      `deploy/dagster.yaml`.
- [ ] 3.4 Correct the module docstring, which still claims "there is no Gold
      asset and no `ready-dates` sensor here" (lines 19-22) — stale against the
      merged corpus `gold:` block.

## 4. `defs/sensors.py` — Gold-readiness sensors

Consult the `dagster-expert` skill before adding these sensor definitions.

- [ ] 4.1 Add one Gold-readiness sensor per derivative, mirroring the
      sovereignty builder at `sensors.py:636-721`: keyed on the run-state of
      `public_contracts_silver`, bounded by that derivative's own partition
      keys, proposing nothing for a date outside its matrix or already
      materialised. Add tests against `tests/fake_corpus.py` for the three
      scenarios the spec names.

## 5. The real run and the checks

- [ ] 5.1 Real run, in a scratch Dagster instance under
      `C:\tmp\orchestration-scratch\public-contracts-gold-wiring` (`DAGSTER_HOME`
      and `CORPUS_SINK_PATH` both there) against the real `corpus` binary, `Y:\`
      read and never written. **No public-contracts Silver exists on `Y:\`**, so
      materialise `public_contracts_silver` for `2021-06-17` into the scratch
      sink first — the Silver row measured that at ~14.6 M rows, 118 MiB, 21.9 s
      — then materialise `contract_facts_gold` for the same date. Report what
      the Gold `_INDEX.json` and `_DONE` under the scratch sink show. If the
      local `corpus` binary predates the `public-contracts-gold` builders, say
      so with the version and the exact error: that is evidence for the
      reviewer, not a pass, and do not work around it by editing the row's code.
- [ ] 5.2 Preview one tick of the `contract_facts_gold` readiness sensor in the
      same scratch instance, with the run-state that 5.1 produced. Report the
      run requests it emitted.
- [ ] 5.3 `uv run ruff check . && uv run ruff format --check . && uv run pytest -q`.
