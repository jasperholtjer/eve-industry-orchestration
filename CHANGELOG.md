# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this project adheres
to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- `market_orders_live_gold` non-partitioned asset + `market_orders_live_schedule` (every 30 min, STOPPED by default): shells `corpus live build --dataset market-orders-live` to overwrite the live `gold/market-orders-live/current/` orderbook aggregate (corpus ADR-0039). A deliberate cron-over-sensor exception — there is no per-date matrix, only "rebuild the newest snapshot". Joins the `everef_download` pool; the fake corpus gains a `live build` subcommand.
- `market_prices_live_gold` non-partitioned asset + `market_prices_live_schedule` (hourly, STOPPED by default): shells `corpus live build --dataset market-prices-live` to overwrite the live `gold/market-prices-live/current/` price passthrough (corpus ADR-0040). Same cron-over-sensor pattern as the orderbook live asset, but the fetch hits ESI (not EVE Ref), so it joins no `everef_download` pool and obeys only the global cap. The fake corpus `live build` now emits the ESI-shaped status (`snapshot_at`/`source`) for this dataset.

### Changed
- Capped rayon at `RAYON_NUM_THREADS=6` in both systemd units so the one in-flight market-orders Silver run (the only rayon parser) leaves 2 cores free on the 8-core LXC for concurrent `heavy` Gold and single-threaded datasets — a multi-day market-orders backfill no longer monopolises every core. Set in both `dagster-daemon.service` and `dagster-webserver.service` because a run inherits the env of whichever process launches it (daemon for sensors/backfills, webserver for launchpad). `redeploy.sh` now installs the systemd units (with `daemon-reload`) so unit changes ship with a redeploy instead of a manual edit.
- Renamed the `market-orders` Gold derivatives `orderbook-snapshot` → `market-orders-snapshot` and `orderbook-changes` → `market-orders-changes` (corpus ADR-0038), tracking the upstream rename. Asset keys become `market_orders_snapshot_gold` / `market_orders_changes_gold` (the name template no longer double-prefixes); the `--derivative` selector, sensor run keys, and `gold/<derivative>/` verify tree follow the new names.
- Renamed the `gold_heavy` concurrency pool to `heavy`: it now bounds every heavy
  Gold `corpus` subprocess (a 365-day rolling window peaks ~3-4 GB), capping Gold
  memory at ~`heavy_limit × ~4 GB` (≈ 8 GB at the `default_limit` of 2).
- Kept `market-orders` Silver in its own `market_orders` concurrency pool at
  limit 1, on CPU grounds: it is the only ingestor that parses with rayon, so a
  single run saturates every core and a second concurrent run only oversubscribes
  them (observed loadavg `r` ~9 on 4 cores during a backfill) with no throughput
  gain. The limit-1 pool also bounds its memory (~3-4 GB peak after the corpus
  streaming ingest). The pool limit cannot live in `dagster.yaml` (only
  `default_limit` does), so `deploy/redeploy.sh` sets it via
  `dagster instance concurrency set market_orders 1` after publishing the config.
  Worst-case heavy memory is 2 Gold + 1 market-orders Silver ≈ 12 GB (unchanged
  by the split — same heavy-slot count), so the LXC sizing rule stays `>= 12 GiB`.

### Added
- `market-orders` orchestration (`defs/market_orders.py`, ADR-0036): a
  daily-partitioned `market_orders_silver` asset (full k-space orderbook, upstream
  gaps left Missing) plus the two split Gold derivatives —
  `market_orders_snapshot_gold` (`orderbook-snapshot`/`orderbook-aggregate`) and
  `market_orders_changes_gold` (`orderbook-changes`/`orderbook-delta`) — daily Gold
  assets whose verify keys on the derivative tree and whose `--derivative` is
  passed explicitly.
- `market_orders_availability_sensor`, `orderbook_snapshot_gold_sensor`, and
  `orderbook_changes_gold_sensor`.
- `system-kills` orchestration (`defs/system_kills.py`, ADR-0037): a
  daily-partitioned `system_kills_silver` asset plus six per-measure Gold
  derivatives — `system_kills_{ship,npc,pod}_history_gold` (`kills-flat`, daily Gold
  assets + `ready-dates` sensors) and `system_kills_{ship,npc,pod}_recent_gold`
  (`kills-recent` EWMA, non-partitioned assets on hourly schedules).
- `system_kills_availability_sensor`, the three
  `system_kills_{ship,npc,pod}_history_gold_sensor`, and the three
  `system_kills_{ship,npc,pod}_recent_schedule`.
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
- `defs/config.py` resolves the `orderbook-aggregate` / `orderbook-delta` shapes
  (ADR-0033/0036): their one-snapshot look-back maps to a one-day Silver preload,
  clamped to `silver.served_start` (`market-orders` Silver and Gold both start
  `2021-07-09`). It also resolves the `kills-flat` (max-horizon look-back) and
  `kills-recent` (EWMA warmup) shapes (ADR-0037).
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

### Fixed
- `CorpusResource.run` kills the corpus subprocess when the streaming loop is
  interrupted (run cancelled / daemon restart), so it is no longer orphaned holding
  the run-state SQLite lock; safe under the `_DONE`-last contract (a half-written
  partition reads as absent).
