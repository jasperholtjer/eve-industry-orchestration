## Why

Implements roadmap row `sovereignty-silver-wiring`.

The three hourly EVE Ref sovereignty datasets — `sovereignty-map`,
`sovereignty-structures` and `sovereignty-campaigns` — landed Silver and all five
Gold trees in `eve-industry-corpus` under its ADR-0066, and the code location has
no asset for any of them. Nothing has ever been materialised, so the family is
inert: no partition definition, no availability sensor, and no way to schedule the
backfill.

The blocker is narrower than "add three assets". `defs/config.py` cannot resolve a
partition start for any of the three. `_lookback_for_shape` knows
`rolling-window`, `flat-multi-horizon`, `recency-weighted` and
`structures-snapshot`, and raises `PartitionConfigError` on every `sov-*` shape.
Because a dataset's Silver start is the earliest preload across *all* its Gold
derivatives, one unresolvable derivative makes the whole dataset unresolvable —
so all five sovereignty shapes have to be taught here even though four of the five
Gold trees are the next row's work.

## What Changes

- Teach `_lookback_for_shape` five new Gold shapes, so
  `resolve_partition_starts` answers for all three datasets:
  - `sov-ownership` and `sov-events` read `tenure.tenure_lookback_days` through a
    new `_tenure_lookback` helper — the first shape family that reads a `tenure`
    block.
  - `sov-adm` reads `flat.horizons` through the existing `_flat_lookback`, the
    same rule `flat-multi-horizon` already uses under a different shape name.
  - `sov-contests` and `sov-panel` have no reach-back and resolve to a zero
    lookback, the rule `structures-snapshot` already carries. `sov-panel`'s
    inputs are sibling Gold trees, not a Silver window, so it contributes no
    Silver preload of its own.
- Three new day-partitioned Silver assets, one module per dataset —
  `sovereignty_map_silver`, `sovereignty_structures_silver`,
  `sovereignty_campaigns_silver`. Each shells out to `corpus ingest --dataset
  <name> --date <date>`, then `corpus verify --tier silver` for the same date on
  success, and records the run-state `rows`, `retention_class` and
  `parquet_sha256` for the partition it wrote. A day the upstream never published
  — reported by the ingest, never decided in Python — skips the verify and leaves
  its partition Missing with an observation, so a four-year backfill is not
  stopped by a gap. No JSON, tar or parquet is opened in Python.
- Three availability sensors keyed on `corpus everef missing-partitions`, one per
  dataset, mirroring `market_history_availability_sensor`: run-state driven, never
  NAS globbing, capped at the shared per-tick fan-out limit.
- Pool placement: all three join `pool="everef_download"`, the existing
  politeness cap on EVE Ref fetches. **No memory-bearing pool.** `everef_download`
  is declared negligible-memory in `deploy/dagster.yaml`; a Silver ingest of
  hourly snapshots is the same shape as the eight members already there, so the
  measured-peak rule that governs `heavy` does not apply and no new pool is
  declared.
- The fake `corpus` binary learns the three datasets — derivative names, the
  `everef missing-partitions` and `state query` JSON — so the assets and sensors
  are exercised without a Rust build or the NAS.

Not in this change: any `corpus gold build` asset. All five Gold trees, including
the panel's cross-dataset dependency, are row `sovereignty-gold-panel`.

No breaking changes. Three datasets that resolved to an error now resolve to a
partition start; nothing that resolved before changes.

## Capabilities

### New Capabilities
- `sovereignty-silver`: how a sovereignty Silver partition is produced and
  offered — the shape-to-lookback rules that make the three datasets' partition
  starts resolvable, the ingest-then-verify order each Silver asset invokes, what
  the availability sensor decides on its own, and the concurrency bound the
  ingests run under.

### Modified Capabilities
<!-- None. `concurrency-pools` is unchanged: the three assets join an existing
     pool and declare no new one, so the set that spec pins does not move. -->

## Impact

- `src/eve_industry_orchestration/defs/config.py` — `_lookback_for_shape` gains
  five shapes and one `_tenure_lookback` helper.
- New: `defs/sovereignty_map.py`, `defs/sovereignty_structures.py`,
  `defs/sovereignty_campaigns.py`.
- `src/eve_industry_orchestration/defs/sensors.py` — three availability sensors.
- `tests/fake_corpus.py` — three datasets added to the dataset and derivative
  tables; new tests alongside the landed Silver-asset and sensor tests.
- Read-only inputs from `../eve-industry-corpus`: `datasets/sovereignty-map.yaml`,
  `datasets/sovereignty-structures.yaml`,
  `datasets/sovereignty-campaigns.yaml`, and its ADR-0066. Nothing in that repo
  is edited.
- `deploy/dagster.yaml` and `tests/test_concurrency_pools.py` are untouched —
  `everef_download` already exists and is already budgeted.
