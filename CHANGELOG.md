# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this project adheres
to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- `transcripts` Silver + Gold + embeddings orchestration (corpus ADR-0055/0053), mirroring the news chain: `transcripts_silver` (`corpus ingest --dataset transcripts` — single-version, one row per video, caption-less videos absent) plus three Gold assets downstream of it — `transcripts_videos_gold`, `transcripts_sections_gold`, `transcripts_entity_mentions_gold` (`corpus gold build --derivative <name>`, ADR-0025 multi-derivative, verify keyed on the derivative tree). `transcripts_entity_mentions_gold` additionally depends on `sde_snapshot_gold` (its SDE-name vocabulary is a **cross-dataset Gold input**, `dependency_fingerprint` in `_INDEX.json`). A single-derivative `transcripts-embeddings` chain follows the news pattern: `transcripts_embeddings_bronze` (`corpus enrich embed`), `transcripts_embeddings_silver` (`corpus ingest`) and `transcripts_embeddings_gold` (`corpus gold build`, also depends on `transcripts_sections_gold` as a cross-dataset Gold input). Every tier is **non-partitioned** and keyed on the fetch date — each Gold partition is a pure function of that day's Silver (no look-back, no coverage gate) and `transcripts.yaml` declares no `served_start` — so a past fetch date is re-processed via the `TranscriptsDateConfig` run-config, not a partition matrix. The group-targeted `transcripts_daily_schedule` (30 22 UTC, STOPPED by default) now runs fetch → ingest → Gold → embeddings in one daily run (the group selection replaced the earlier Bronze-only single-asset target). The embed step **shares the `news_embed` limit-1 pool** with news-embeddings (both run the same ~4.4 GB ONNX model, so no two embeds of either dataset overlap on the 12 GB LXC). New `transcripts_listed_vs_archived` asset check (non-blocking, WARN) on `transcripts_silver` reconciles the videos Silver scanned (`corpus transcripts match-stats` → `report.videos`) against the `seen_documents` ledger (`corpus state query`) — expected metadata, never a failure. `transcripts-annotations` is deliberately **never wired**: its generation is a manual operator run via the `annotate-transcripts` skill (contract `t2`), the same way news keeps `corpus enrich annotate` out of Dagster. The fake corpus gains `transcripts match-stats`, the transcripts Gold derivatives, and a transcripts seen-ledger.
- `news-embeddings` orchestration (corpus ADR-0053): three non-partitioned assets downstream of `news_sections_gold` — `news_embeddings_bronze` (`corpus enrich embed`, the pinned local ONNX run archived verbatim like a fetch), `news_embeddings_silver` (`corpus ingest`, the deterministic parse of the archived vector shards) and `news_embeddings_gold` (`corpus gold build`, the `embeddings_v1` join, which also depends on `news_sections_gold` as a cross-dataset Gold input). Same fetch-date keying and `NewsDateConfig` run-config as the rest of the news chain, and they sit in the `news` asset group, so the existing group-targeted `news_daily_schedule` runs them in dependency order — no new schedule. `NewsEmbedConfig.limit` caps the chunks one embed run does; the step is ledgered (`embedded_chunks`, chunk × `model_rev`), so a capped, interrupted or OOM-killed run resumes with no data loss. `corpus enrich annotate` is deliberately NOT wired — it costs money and stays a manual operator run.
- `news_embed` concurrency pool (limit 1, set in `redeploy.sh`) on `news_embeddings_bronze`: the embed peaks 4.4 GB RSS at 3.76 chunks/s (a full 11 320-chunk generation ≈ 50 min), so it gets its own pool rather than joining `heavy` — `heavy`'s limit of 2 would allow two overlapping embeds (~8.8 GB) on the 12 GB LXC. Limit 1 guarantees no two embed runs ever overlap, across every launch path. An asset holds only one pool, so a concurrent `heavy` Gold build (~3 GB floor) is still possible: worst case ≈ 7.4 GB, within budget.
- `CorpusResource.embedding_model_dir` (`CORPUS_EMBEDDING_MODEL_DIR`, set in both systemd units, `.env.example` documented): the local ONNX snapshot dir `corpus enrich embed` loads the pinned model from. Deployment, never contract — no path in the code or the dataset YAML; absent ⇒ the embed asset fails loud instead of producing an unlabeled generation. The fake corpus gains `enrich embed` and mirrors that failure.
- `news` Silver + Gold orchestration (corpus ADR-0050/0051/0052): `news_silver` (`corpus ingest --dataset news` — bitemporal, one row per fetched article version) plus four Gold assets downstream of it — `news_articles_gold`, `news_sections_gold`, `news_entity_mentions_gold`, `news_events_gold` (`corpus gold build --derivative <name>`, ADR-0025 multi-derivative, verify keyed on the derivative tree). `news_entity_mentions_gold` additionally depends on `sde_snapshot_gold`: its vocabulary is a **cross-dataset Gold input** (the `sde-*` snapshot trees, recorded as `dependency_fingerprint` in `_INDEX.json`). Every tier is **non-partitioned** and keyed on the fetch date — each Gold partition is a pure function of that day's Silver (no look-back, no coverage gate; a quiet news day is a legal 0-row partition) and `news.yaml` declares no `served_start` to anchor a matrix with — so a past fetch date is re-processed via the `NewsDateConfig` run-config (`date`, defaults to today UTC), not a partition backfill. New `news_listed_vs_archived` asset check (non-blocking, WARN) on `news_silver` reports the listed-vs-archived article delta (Silver articles via `corpus news match-stats` minus the `seen_documents` ledger via `corpus state query`) — the 7 slugs that return HTTP 500 at CCP are expected metadata, never a failure. The fake corpus gains `news match-stats`, the news Gold derivatives, and a seen-ledger.
- `news` + `transcripts` context orchestration (`defs/news.py`, `defs/transcripts.py`, corpus ADR-0045/0046/0048): the first Bronze-only archival datasets. Each is a single **non-partitioned** asset (`news_bronze` / `transcripts_bronze`) shelling `corpus context fetch --dataset <name>`, which archives the raw CCP news RSS + article HTML (news) or the YouTube Atom feed + Supadata transcript JSON (transcripts) verbatim to a keep-forever `bronze/<dataset>/year=/month=/day=/` partition keyed on the **fetch** date (`_MANIFEST.json` + `_DONE` last). No Silver, no Gold, no `corpus ingest`, no coverage gate — the binary decides "today" and dedups already-archived documents via its seen-ledger, so there is no per-date matrix to diff. Driven by daily `news_daily_schedule` / `transcripts_daily_schedule` (late UTC evening, staggered 30 min, STOPPED by default), not a sensor. Neither asset joins a concurrency pool (it hits neither EVE Ref nor ESI; request pacing lives in the binary). The historical sweep is a manually-triggered `news_backfill_job` / `transcripts_backfill_job` with run-config caps (`max_articles` / `max_videos`) over `corpus context backfill` — the caps bound the paid Supadata credits and the ~45-min news sweep; re-run until the summary reports `capped: false`. `transcripts` defaults `max_videos` to 90 so a bare run never sweeps uncapped and blows the monthly Supadata budget (set higher, or `null` to uncap). The fake corpus gains `context fetch` / `context backfill` and a `FAKE_CONTEXT_DATE` knob.
- `serving_load_sde_industry_products` serving-load asset (serving ADR-0044): shells `eve-serving load --dataset sde-industry-products` to load the latest-only SDE product universe Gold into `industry.products`. Non-partitioned, downstream of `serving_load_sde` (the SDE rewrite TRUNCATEs `industry.products` and clears its load-state), added to `serving_load_job` so the hourly `serving_load_schedule` reloads it in dependency order. Fake `eve-serving` gains the dataset.
- `sde_industry_products_gold` non-partitioned asset (corpus ADR-0044): the latest-only SDE industrial product universe (`sde-industry-products` derivative, `industry-products` shape). Mirrors `sde_snapshot_gold` — `corpus gold build --derivative sde-industry-products --latest` overwrites the single flat `gold/sde-industry-products/` tree, self-skips when no Silver is committed. Rematerialised by the existing `sde_snapshot_schedule` (now targets both latest-only SDE Gold assets). `config.py`/fixtures gain the derivative; the fake corpus already handles the generic `gold build`.
- `sde_industry_facilities_gold` + `sde_industry_hubs_gold` non-partitioned assets (corpus ADR-0056): the latest-only SDE-derived NPC industry-station dimension (`sde-industry-facilities`) and its per-system aggregate (`sde-industry-hubs`). Both mirror `sde_industry_products_gold` — `corpus gold build --derivative <name> --latest` overwrites the single flat `gold/<name>/` tree, self-skips when no Silver is committed, reports `row_count`. Added to the existing `sde_snapshot_schedule` (now targets all four latest-only SDE Gold assets, daily, STOPPED by default). The fixture `sde.yaml` + config-derivatives test gain the two derivatives; the fake corpus returns their `--latest` status with a `row_count`. Serving/graph load deferred (ADR-0056).
- `industry-cost-indices` orchestration (`defs/industry_cost_indices.py`, corpus ADR-0043): a daily-partitioned `industry_cost_indices_silver` asset (hourly cost-index snapshots, upstream gaps left Missing) plus a single windowed Gold derivative `industry-cost-indices-history` (`cost-index-history` shape, horizons `[7,30,90,365]`, `coverage_min_ratio 0.5`) as the `industry_cost_indices_history_gold` daily asset, driven by `industry_cost_indices_history_gold_sensor` (STOPPED by default). Mirrors system-jumps' `system-traffic-history`: shared Silver, `--derivative`-keyed Gold build + verify tree, 365d coverage gate owned by the binary. Silver joins `everef_download`; the Gold build is light (measured peak ~97 MiB) so it omits `pool=` and obeys only the global cap. `config.py` learns the `cost-index-history` shape (max-of-`flat.horizons` look-back). Resolved starts come from the dataset YAML: Gold `2022-01-01`, Silver floor-clamped to `silver.served_start 2021-07-01` (ADR-0027).
- `industry_cost_indices_live_gold` non-partitioned asset + `industry_cost_indices_live_schedule` (hourly, STOPPED by default): shells `corpus live build --dataset industry-cost-indices-live` to overwrite the live `gold/industry-cost-indices-live/current/` cost-index level (corpus ADR-0043, `daily-snapshot` shape). Same cron-over-sensor pattern as the market-orders live asset; the source is EVE Ref, so it joins the `everef_download` pool (not `heavy`).
- `market_orders_events_gold` daily Gold asset + `market_orders_events_gold_sensor` (STOPPED by default): the third `market-orders` Gold derivative `market-orders-events` (`orderbook-events` shape, corpus ADR-0042), the un-collapsed per-order event log feeding `market-orders-changes` — one row per order state-change keyed by `order_id`. Mirrors `market-orders-changes` exactly: same shared Silver, one-snapshot look-back, no coverage gate, upstream-gap days left Missing, `--derivative` and derivative-keyed verify tree passed explicitly. Joins the `heavy` pool. `config.py` learns the `orderbook-events` shape (1-day look-back); the fake corpus and fixtures gain the derivative.
- `market_history_silver` handles the `incomplete` ingest status (corpus ADR-0041): EVE Ref publishes the daily market-history file incrementally, so a not-yet-settled day reports `status: incomplete` (exit 0, no partition). The asset is now `output_required=False` and, on `incomplete`, skips the verify and emits an `AssetObservation` (`skip_reason=upstream_incomplete`) instead of materialising — leaving the partition Missing so the sensor re-proposes it until the upstream file settles (retryable, unlike the permanent absent-day skip). The fake corpus gains a `FAKE_INCOMPLETE_DATES` knob.
- Serving-tier load wiring (`defs/serving.py`, `defs/serving_resource.py`): a `ServingResource` shells the idempotent `eve-serving load` CLI on the DB-VM over SSH (default `serving@192.168.2.212`, configurable via `SERVING_HOST`/`SERVING_USER`), streams its output, fails the asset on a non-zero exit, and surfaces the `loaded`/`skipped` row count as metadata. Four non-partitioned load assets (`serving_load_sde`, `serving_load_market_history`, `serving_load_market_orders_live`, `serving_load_market_prices_live`) hang off their source Gold availability via `deps=`, with `serving_load_sde` modelled upstream of the three market loads (an SDE rebuild TRUNCATEs `market.*` and clears their load-state). A `serving_load_job` over the four plus an hourly `serving_load_schedule` (STOPPED by default) reload the serving tier in dependency order; idempotency on `parquet_sha256` makes an unchanged run a no-op. No credentials in code — auth rides the corpus account's existing SSH key. Tests use a fake `eve-serving` (`tests/fake_serving.py`) standing in as the SSH binary.
- `serving_load_industry_cost_indices_live` non-partitioned load asset, added to `serving_load_job`: shells `eve-serving load --dataset industry-cost-indices-live` to load the live cost-index `current` snapshot into the serving tier's new `industry.cost_indices_live` table. Modelled downstream of `serving_load_sde` like the market loads — the rows carry a `system_id` FK into the map dimension, so the SDE rewrite TRUNCATEs the table and clears its load-state, forcing a reload in the same pass. The fake `eve-serving` now clears this dataset on an SDE load too.
- `market_orders_live_gold` non-partitioned asset + `market_orders_live_schedule` (every 30 min, STOPPED by default): shells `corpus live build --dataset market-orders-live` to overwrite the live `gold/market-orders-live/current/` orderbook aggregate (corpus ADR-0039). A deliberate cron-over-sensor exception — there is no per-date matrix, only "rebuild the newest snapshot". Joins the `everef_download` pool; the fake corpus gains a `live build` subcommand.
- `market_prices_live_gold` non-partitioned asset + `market_prices_live_schedule` (hourly, STOPPED by default): shells `corpus live build --dataset market-prices-live` to overwrite the live `gold/market-prices-live/current/` price passthrough (corpus ADR-0040). Same cron-over-sensor pattern as the orderbook live asset, but the fetch hits ESI (not EVE Ref), so it joins no `everef_download` pool and obeys only the global cap. The fake corpus `live build` now emits the ESI-shaped status (`snapshot_at`/`source`) for this dataset.

### Changed
- Wired the context-dataset secrets into the systemd units via an optional `EnvironmentFile=-/etc/eve-industry-orchestration/secrets.env` (corpus ADR-0047, Doppler retired): `SUPADATA_API_KEY` (transcripts fetch + backfill) and `YOUTUBE_API_KEY` (transcripts backfill), passed through to the `corpus` subprocess by `CorpusResource._env()`. The `news` dataset needs no secret — its backfill discovers the Contentful token from the public site bundle itself (corpus ADR-0049). Added to both `dagster-daemon.service` and `dagster-webserver.service` (a backfill launched from the launchpad inherits the webserver's env, a scheduled fetch the daemon's); kept out of the repo, `-`-optional so a box without the file still starts. `redeploy.sh` gains an advisory `check_context_secrets` step that reports whether the file exists and which of the three keys are defined (never aborts the deploy — the no-secret `news` fetch works regardless), so a missing key surfaces at deploy time instead of inside a run. `CorpusResource.run` now accepts an `OpExecutionContext` too, so the backfill ops can stream through it.
- Removed `system-jumps`, `system-kills` (×3 measures) and `industry-cost-indices` Gold from the `heavy` concurrency pool. They use the same 365d k-way merge as market-history/market-orders Gold but over ~5k-row/day narrow snapshots; measured peak RSS is ~90–97 MiB (`/usr/bin/time -v`), ~40x under the ~4 GiB `heavy` budget. In `heavy` they only starved the genuinely-heavy backfills of scarce slots. They now omit `pool=` and obey the global cap alone. `heavy` membership is now by measured peak, not by "is windowed" — only market-history and market-orders Gold (plus market-orders Silver) remain.
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

### Fixed
- Availability and Gold-readiness sensors now retry a partition corpus keeps reporting actionable, instead of stalling it forever. A static per-date `run_key` was deduped by Dagster after the first run, so an upstream-incomplete Silver day (ADR-0041) that finishes as a green no-op — corpus still reporting it `missing` — was never re-requested (observed: market-history stuck from 2026-06-27). The shared `defs/sensor_util.request_partitions` now appends a rotating per-tick cursor token to the `run_key` so each tick's request for a still-actionable date is a distinct, launchable run, self-limiting once corpus commits the partition. An in-flight guard skips dates that already have a non-terminal run for the target asset, so the rotating key never races two `corpus` writers on one contract dir. Routes every daily Silver/Gold sensor (market-history, system-jumps, cost-indices, market-orders ×3, system-kills ×3) through the helper; SDE sensors keep their dynamic-partition gating.
- Serving-load assets now carry a `RetryPolicy` (3 retries, 30s exponential backoff): the loader reads Gold over NFS from the single-HDD NAS, so a load can hit a transient `IO Error: … Stale file handle` when a Gold build overwrites the tree under a reader's cached handle. Each load is idempotent on the partition's `parquet_sha256`, so a retried load re-converges.

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
