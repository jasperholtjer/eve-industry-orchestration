# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this project adheres
to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- `system-jumps` orchestration (`defs/system_jumps.py`): a daily-partitioned
  `system_jumps_silver` asset plus the two ADR-0025 Gold derivatives —
  `system_jumps_history_gold` (`flat-multi-horizon`, daily Gold asset +
  `ready-dates` sensor) and `system_jumps_recent_gold` (`recency-weighted`,
  non-partitioned asset on an hourly schedule).
- `system_jumps_availability_sensor`, `system_jumps_history_gold_sensor`, and
  `system_jumps_recent_schedule`.

### Changed
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
