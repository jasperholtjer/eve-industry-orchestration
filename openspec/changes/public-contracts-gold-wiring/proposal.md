## Why

Implements roadmap row `public-contracts-gold-wiring`: corpus merged
`public-contracts-gold` (2026-09-02), adding a `gold:` block with four
derivatives (`contract-facts`, `contract-item-facts`, `contract-item-prices`,
`courier-rates`) to `public-contracts.yaml`. `config.py`'s
`_lookback_for_shape` knows none of the four shapes, so it raises
`PartitionConfigError` at import against the real datasets dir — the code
location does not load. `tmp/brief.md` confirms all four derivatives are
zero-lookback (corpus ADR-0068: "no cross-day state", each day a pure
function of its own Silver), the same rule `config.py` already gives
`structures-snapshot`, and that `resolve_silver_start`'s Gold-less branch
(`public-contracts-silver-wiring`) stays for other datasets — public-contracts
itself just stops taking it.

## What Changes

- Add the four public-contracts shapes to `config.py#_lookback_for_shape`,
  each returning the existing zero-lookback constant (mirroring
  `structures-snapshot`).
- Wire four Gold assets in `defs/public_contracts.py`, one per derivative,
  sharing a `_build_gold` helper and shelling `corpus gold build --dataset
  public-contracts --derivative <name> --date <date> --sink-path`, mirroring
  `sovereignty_map.py`'s `_build_gold` / `_gold_start` pattern. No `pool=`: no
  measured peak exists for any of the four builds.
- Add a Gold-readiness sensor per derivative, mirroring the sovereignty
  builder in `sensors.py`, off the existing `public_contracts_availability_sensor`.
- Correct `public_contracts.py`'s module docstring, which still claims "there
  is no Gold asset and no `ready-dates` sensor here."
- Refresh `tests/fixtures/datasets/public-contracts.yaml` with the real
  `gold:` block (four derivatives, `served_start: 2021-06-17` each), and add
  one `test_config.py` case per shape, plus a guard that resolves every
  dataset in the sibling corpus checkout when it is present, so the next
  unknown shape fails the suite rather than the code location.

## Capabilities

### New Capabilities
- `public-contracts-gold` — how the four derivatives are offered, built and
  recorded: per-derivative build then verify, a partition start read from each
  derivative's own `served_start` with a zero-day Silver reach-back, an
  unresolvable shape failing loudly rather than defaulting, and readiness
  decided from the run-state of the day's Silver.

### Modified Capabilities
- none

## Impact

- Modules under `defs/`: `defs/config.py` (`_lookback_for_shape`),
  `defs/public_contracts.py` (four new Gold assets), `defs/sensors.py` (four
  new Gold-readiness sensors).
- Corpus CLI surface shelled / what is recorded: `corpus gold build --dataset
  public-contracts --derivative <name> --date <date> --sink-path`, then
  `corpus verify --dataset <derivative> --date <date> --tier gold
  --sink-path`; each asset records `dataset`, `derivative`, `tier`,
  `partition` plus the run-state facts from `corpus.partition_metadata`.
- Sensors, schedules, pools: four new Gold-readiness sensors (no schedule
  change); no pool joined (global cap only — no measured peak).
