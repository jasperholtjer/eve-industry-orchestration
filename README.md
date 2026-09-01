# eve-industry-orchestration

Thin Dagster orchestrator for the [eve-industry-corpus](https://github.com/jasperholtjer/eve-industry-corpus)
data platform. Dagster owns the partition matrix, the backfill UI, and the
materialisation log; it shells out to the static `corpus` binary, which does the
ingest -> Silver -> Gold compute and writes the `parquet + _INDEX.json + _DONE`
contract to the NFS sink. No compute or run-state lives here.

The homelab deployment (LXC, NFS mount, build order) is documented in
`homelab_docs`; this repository holds the orchestration code and its deploy wiring.

## Architecture

- **`CorpusResource`** (`defs/corpus_resource.py`) — wraps the binary. `run`
  streams a subcommand into the run log (Silver/Gold materialisations);
  `everef_missing_partitions`, `gold_ready_dates`, and `state_query` capture JSON
  off stdout for the sensors.
- **Partition config** (`defs/config.py`) — resolves Silver/Gold start dates from
  the corpus dataset YAML per `(dataset, derivative)`, instead of hardcoding them.
  Gold start is the derivative's `served_start`; Silver reaches back the look-back
  window (largest `rolling` horizon, `max(flat.horizons)`, or the EWMA warmup) and
  is shared across a dataset's derivatives (earliest preload wins). Reads the
  ADR-0025 `gold` list of named derivatives (market-history is a one-element list,
  system-jumps has two). Override per tier with
  `CORPUS_<DATASET>_<TIER>_START`, or per derivative with
  `CORPUS_<DATASET>_<DERIVATIVE>_GOLD_START`.
- **Dataset assets** (`defs/market_history.py`, `defs/system_jumps.py`,
  `defs/market_orders.py`, `defs/system_kills.py`, `defs/structures.py`,
  `defs/killmails.py`) —
  daily-partitioned Silver and
  Gold assets, with **distinct** partition start dates (Silver reaches back one
  window before Gold). Gold depends on Silver via `deps=` (lineage only). A
  multi-derivative dataset (ADR-0025) gets one Gold asset per derivative, each
  passing `--derivative`: a `flat-multi-horizon` /​ `rolling` /​
  `orderbook-aggregate` /​ `orderbook-delta` /​ `kills-flat` derivative is a
  daily-partitioned asset + `ready-dates` sensor, while a `recency-weighted` /​
  `kills-recent` derivative is a single **non-partitioned** asset a schedule
  rematerialises against the latest buildable date. `market-orders` (ADR-0036, derivatives renamed in ADR-0038)
  splits its Gold into two daily-partitioned derivatives — `market-orders-snapshot`
  (per-snapshot aggregate) and `market-orders-changes` (cross-snapshot activity
  delta), both a one-day look-back. `system-kills` (ADR-0037) fans its three
  measures into six derivatives — `system-kills-{ship,npc,pod}-history`
  (`kills-flat`, daily-partitioned + sensor) and `system-kills-{ship,npc,pod}-recent`
  (`kills-recent` EWMA, non-partitioned + hourly schedule). `structures`
  (ADR-0057/0062) has two daily-partitioned derivatives with **different** Gold
  starts — `structures-snapshot` (no look-back, served from the first v2 archive)
  and `structure-population-history` (a 30-day churn window, so served a month
  later) — each with its own partition matrix and readiness sensor, and both
  depending on `sde_snapshot_gold` for the `type_id → facility_class` resolution.
  `killmails` (ADR-0059/0061) has one daily-partitioned `kills-consumption`
  derivative that depends on **two** other Gold trees — `sde_snapshot_gold` for
  the region map and `market_history_gold` for the reference price — and is the
  one dataset whose partitions **mutate**: see "Mutable partitions" below.
  Every derivative name
  differs from the dataset, so each Gold call passes `--derivative`; Gold verify
  keys on the derivative name (its own `gold/<derivative>/...` tree).
- **Build-versioned assets** (`defs/sde.py`, ADR-0032) — the SDE static
  reference is not a daily time-series: a partition is a game *build*, so it uses a
  `DynamicPartitionsDefinition` keyed on build number (no `served_start` /
  look-back). Silver is one build-partitioned `@asset` (one atomic unified
  `corpus ingest --build <n>` per build); `sde-changelog` is one build-partitioned
  `@asset` that leaves baseline builds (no predecessor) Missing
  (`output_required=False`). `sde-snapshot` (the latest-only catalogue) and
  `sde-industry-products` (ADR-0044, the latest-only industrial product universe)
  are both **non-partitioned** `@asset`s a schedule rematerialises against
  `--latest` (mirroring the `recency-weighted` recent asset), not part of the
  partition matrix.
- **Context datasets** (`defs/news.py`, `defs/transcripts.py`, ADR-0045/0046/0048) —
  archival datasets keyed on the *fetch* date, the exception to the partition-matrix
  mould. Each shells `corpus context fetch` (raw CCP news RSS + article HTML, or the
  YouTube feed + Supadata transcripts) into a keep-forever
  `bronze/<dataset>/year=/month=/day=/` partition. Both continue into Silver and Gold:
  `news` into four Gold trees (ADR-0050/0051/0052) — `news-articles`, `news-sections`,
  `news-entity-mentions` and `news-events` — and `transcripts` into three (ADR-0055) —
  `transcripts-videos`, `transcripts-sections`, `transcripts-entity-mentions` — plus a
  single-derivative `*-embeddings` chain each (ADR-0053, `corpus enrich embed` → ingest
  → gold). The `*-entity-mentions` and `*-embeddings` builds read the `sde-*` /
  `*-sections` Gold trees as **cross-dataset Gold inputs**, so those assets also depend
  on `sde_snapshot_gold` / the sections Gold. Every tier is **non-partitioned**: each
  Gold derivative is a pure function of that fetch date's Silver alone (no look-back, no
  coverage gate), so `news_daily_schedule` / `transcripts_daily_schedule` (group-targeted)
  run the whole chain in one daily run and a past fetch date is re-processed via the
  `{News,Transcripts}DateConfig` run-config instead of a partition matrix. Both embed
  steps share the single `news_embed` limit-1 pool, so no two embeds ever overlap. The
  `news_listed_vs_archived` / `transcripts_listed_vs_archived` asset checks surface the
  listed-vs-archived delta as metadata, never as a failure. `transcripts-annotations` is
  **never wired** — its generation is a manual operator run via the `annotate-transcripts`
  skill (contract `t2`), like news keeps `corpus enrich annotate` out of Dagster. The
  historical sweep is a manually-triggered `{news,transcripts}_backfill_job` with
  run-config caps (`max_articles` / `max_videos`) over `corpus context backfill`. The
  fetch/parse tiers join no concurrency pool. Secrets ride the process env (ADR-0047) —
  see the deploy section.
- **Availability sensors** (`defs/sensors.py`) and **schedules** — thin
  cap-and-dedup loops. A Silver sensor per dataset polls
  `corpus everef missing-partitions`; a Gold sensor per windowed derivative polls
  `corpus gold ready-dates [--derivative <d>]`. The `recency-weighted` /
  `kills-recent` "recent" assets have no sensor — `system_jumps_recent_schedule`
  and the three `system_kills_{ship,npc,pod}_recent_schedule` rematerialise them
  hourly. SDE instead has `sde_build_discovery_sensor` (registers build
  partitions from `corpus everef list` and, keyed on corpus run-state, keeps
  proposing every registered-or-discovered build whose `sde` Silver is not
  yet committed, so a failed ingest is retried and a hole in the build
  sequence heals on a later tick), `sde_gold_sensor` (the changelog for
  builds whose Silver is committed and whose changelog Gold is not, minus the
  baseline build — which has no predecessor to diff against and which the
  binary skips — plus any changelog that was diffed across a hole and is now
  stale; a Gold build is held back while a lower Silver run that could change
  its predecessor is still in flight), and `sde_snapshot_schedule` (daily
  rematerialise of the non-partitioned latest snapshot and
  `sde-industry-products`). All key status on corpus run-state, never on
  globbing the NAS tree. The sensors that go through `request_partitions` — every
  Silver and Gold availability sensor, `sde_build_discovery_sensor` and
  `sde_gold_sensor` included — carry a per-tick token in the `run_key`, so a
  partition corpus still reports actionable is retried, with an in-flight
  guard keeping that rotation from putting a second writer on one contract
  directory. `mer_report_discovery_sensor` is the remaining exception: it
  registers a partition and requests it once, under a static `run_key`, so a
  run that fails leaves a hole no later tick re-proposes.
- **Mutable partitions** (`killmails` only, corpus ADR-0060) — every other everef
  partition is immutable once published, so `_DONE` plus the recorded source
  sha256 is a complete freshness contract. Killmail days are not: zKillboard keeps
  discovering old kills and EVE Ref re-archives the day with more members, months
  or years later. Neither normal signal can see that — `missing-partitions` reports
  only days with **no** partition, `ready-dates` only days with **no** Gold — so
  the dataset carries two extra sensors. `killmails_freshness_sensor` polls
  `corpus killmails freshness --json` (upstream's own `totals.json` diffed against
  the count each partition recorded at ingest) and re-proposes the changed days for
  re-ingest; `killmails_consumption_gold_repair_sensor` then rebuilds the Gold
  those days feed, asking run-state which Gold partitions predate their own Silver
  (`silver.last_seen_at > gold.last_seen_at`) — stateless, and correctly ordered
  because `last_seen_at` only moves once the repair ingest commits. Both run daily.
  Repair scope is the changed day itself, **not** its 365-day forward window: that
  day's `qty_destroyed` / `isk_value_destroyed` become correct while downstream
  window features stay marginally stale, a deliberate trade against queueing 366
  heavy builds per drifted day.
- **Serving-load assets** (`defs/serving.py`, `defs/serving_resource.py`) — the
  "when" of the serving tier. `ServingResource` shells the idempotent
  `eve-serving load` CLI on the DB-VM over SSH (the corpus account's existing key;
  no credentials here) and surfaces the loader's `loaded`/`skipped` row count. Four
  non-partitioned load assets (`sde`, `market-history`, `market-orders-live`,
  `market-prices-live`) hang off their source Gold availability via `deps=`, with
  `serving_load_sde` modelled **upstream** of the three market loads — an SDE
  rebuild rewrites the static reference and TRUNCATEs `market.*`, so the markets
  must reload after it. `serving_load_job` + the hourly `serving_load_schedule`
  reload the tier in dependency order; the loader's `parquet_sha256` idempotency
  makes an unchanged run a no-op. See [docs/serving-seam.md](docs/serving-seam.md).
- **Resources** (`defs/resources.py`) — binds `corpus` and `serving` to the assets
  and sensors from env vars.

Adding a dataset is a new module mirroring `market_history.py` (single `rolling`
derivative) or `system_jumps.py` (multi-derivative, ADR-0025); see the
`add-dataset-to-orchestration` skill. Factor the asset bodies into a shared
factory once a third dataset shows what actually varies.

## Local development

```bash
cp .env.example .env     # fill in paths; on the LXC these point at the real mounts
uv sync
uv run dg dev            # UI at http://localhost:3000
```

`dg` loads `.env` automatically. Locally, point `CORPUS_SINK_PATH` at a throwaway
directory and only materialise against dates the binary can reach.

## Deployment (Dagster LXC)

The LXC and the NFS mount are stood up per the homelab how-tos; the `corpus`
binary and its dataset configs are pulled by `redeploy.sh` (see below). This repo
supplies the orchestration wiring in `deploy/`:

- `dagster.yaml` — instance config; `QueuedRunCoordinator` carries the global run
  cap and the concurrency pools that bound heavy-corpus memory and EVE Ref fetches
  across every launch path. The pool set, limits and memory budget are documented
  there. Copy to `$DAGSTER_HOME/dagster.yaml`.
- `workspace.yaml` — code location (`eve_industry_orchestration.definitions`).
- `dagster-webserver.service` / `dagster-daemon.service` — systemd units running as
  `corpus`, with `CORPUS_*` env and `DAGSTER_HOME` set. Adjust the `WorkingDirectory`
  to the real clone path before installing. Both load an optional
  `EnvironmentFile=-/etc/eve-industry-orchestration/secrets.env` for the
  context-dataset secrets (ADR-0047) — `SUPADATA_API_KEY` (transcripts fetch +
  backfill) and `YOUTUBE_API_KEY` (transcripts backfill). The `news` dataset needs
  no secret: its backfill discovers the Contentful token from the public site
  bundle itself (ADR-0049). Create that root-only file on the LXC; it is never
  committed, and its absence only disables the transcript paths (`news` keeps
  working).

Deploy = `git clone` + `uv sync` on the LXC, install the units, `systemctl enable --now`.
First-time host setup (LXC, UID/GID map, NFS mount, `gh` auth, `uv`) is in the
homelab `deploy-dagster-lxc.md` how-to.

- `redeploy.sh` — recurring update: pull + `uv sync` as `corpus`, install the
  `corpus` binary + datasets from the private release — the **latest** release by
  default, or the `CORPUS_VERSION` pin (`gh`, checksum-verified,
  `--version`-asserted), publish `dagster.yaml` to `DAGSTER_HOME`, restart the
  services. Run as root from anywhere in the container
  (`bash /opt/eve-industry-orchestration/deploy/redeploy.sh`); it pulls itself and
  drops to `corpus` for the repo work so the corpus-owned tree is not rewritten as
  root. By default it tracks the latest corpus release; pin an exact one with
  `CORPUS_VERSION=vX.Y.Z redeploy`. The download needs `gh` authenticated as root
  (`gh auth login`, or `GH_TOKEN`). Symlink it onto PATH once for a bare `redeploy`:
  `ln -s /opt/eve-industry-orchestration/deploy/redeploy.sh /usr/local/bin/redeploy`.

## Testing

`uv run pytest` exercises the Silver path without the Rust build: a fake `corpus`
binary (`tests/fake_corpus.py`) mimics the contract — it writes
`parquet + _INDEX.json + _DONE` and answers `everef missing-partitions` /
`state query` with the real JSON shapes. Tests cover the config-derived partition
starts, the sensor's `RunRequest` generation, and the resource's non-zero-exit ->
`dg.Failure` path.

## Open items

- **Materialisation metadata** — enrich `MaterializeResult` from `_INDEX.json` /
  `corpus state query` (rows, retention_class) for both Silver and Gold.
