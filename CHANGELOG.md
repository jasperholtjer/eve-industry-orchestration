# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this project adheres
to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- `market-orders` orchestration (`defs/market_orders.py`, ADR-0033): a
  daily-partitioned `market_orders_silver` asset (full k-space orderbook, upstream
  gaps left Missing) plus `market_orders_gold` — the single `orderbook-sweep`
  (`orderbook-aggregate`) derivative, a daily Gold asset whose verify keys on the
  derivative tree and whose `--derivative` is passed explicitly.
- `market_orders_availability_sensor` and `market_orders_gold_sensor`.
- `system-jumps` orchestration (`defs/system_jumps.py`): a daily-partitioned
  `system_jumps_silver` asset plus the two ADR-0025 Gold derivatives —
  `system_jumps_history_gold` (`flat-multi-horizon`, daily Gold asset +
  `ready-dates` sensor) and `system_jumps_recent_gold` (`recency-weighted`,
  non-partitioned asset on an hourly schedule).
- `system_jumps_availability_sensor`, `system_jumps_history_gold_sensor`, and
  `system_jumps_recent_schedule`.
- `sde` orchestration (`defs/sde.py`): the first build-versioned dataset
  (ADR-0030/0031). A `DynamicPartitionsDefinition` keyed on build number drives
  three `@multi_asset`s — `sde_silver` and the two Gold derivatives
  `sde_changelog_gold` (`entity-changelog`, baseline builds left Missing) and
  `sde_snapshot_gold` (`entity-snapshot`) — each fanning out over the configured
  `silver.entities` via one `corpus` call per build.
- `sde_build_discovery_sensor` (registers builds from `corpus everef list` and
  requests Silver) and `sde_gold_sensor` (requests both Gold derivatives for
  builds whose Silver is committed, keyed on corpus run-state).

### Changed
- `defs/config.py` resolves the `orderbook-aggregate` shape (ADR-0033): its
  one-snapshot look-back maps to a one-day Silver preload, clamped to
  `silver.served_start` (`market-orders` Silver and Gold both start `2021-07-09`).
- `defs/config.py` adds `sde_entities` and `sde_gold_derivatives`, reading the SDE
  entity fan-out and Gold derivatives from the dataset YAML (the build-versioned
  SDE has no `served_start` / look-back, so `resolve_partition_starts` does not
  apply to it).
- `CorpusResource.everef_list_builds` wraps `corpus everef list` for
  build-versioned discovery.
- `defs/config.py` resolves partition starts per `(dataset, derivative)` from the
  ADR-0025 `gold` list (a single-derivative dataset is a one-element list). Silver
  start is the earliest look-back preload across a dataset's windowed derivatives.
  New per-derivative override `CORPUS_<DATASET>_<DERIVATIVE>_GOLD_START`.
- `CorpusResource.gold_ready_dates` accepts an optional `derivative` and passes
  `--derivative` through to the binary.
- `defs/config.py` clamps the Silver partition start to `silver.served_start` (the
  upstream coverage floor, ADR-0027) from the dataset YAML when present, so the
  matrix never reaches before upstream data exists; `system-jumps` Silver now starts
  `2021-07-01` instead of the doomed derived `2021-01-01`.
- `CorpusResource.run` attaches the corpus subprocess's output tail to the raised
  `dg.Failure`, so a failed asset surfaces the real error in the Failure instead of
  only the command line.
- `CorpusResource.run` parses and returns the `corpus ingest` / `gold build` stdout
  status object (`written` / `skipped`, ADR-0028/0029); `system_jumps_silver` and
  `system_jumps_history_gold` are now `output_required=False` and, on a genuinely-
  absent upstream day (interior EVE Ref gap — Silver) or a target day whose Silver is
  that gap (Gold), leave the partition Missing — skipping the verify and emitting an
  `AssetObservation` (`skip_reason=upstream_absent` / `upstream_gap`) instead of
  failing or materialising an empty day.
