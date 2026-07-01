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
  `defs/market_orders.py`, `defs/system_kills.py`) — daily-partitioned Silver and
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
  (`kills-recent` EWMA, non-partitioned + hourly schedule). Every derivative name
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
- **Availability sensors** (`defs/sensors.py`) and **schedules** — thin
  cap-and-dedup loops. A Silver sensor per dataset polls
  `corpus everef missing-partitions`; a Gold sensor per windowed derivative polls
  `corpus gold ready-dates [--derivative <d>]`. The `recency-weighted` /
  `kills-recent` "recent" assets have no sensor — `system_jumps_recent_schedule`
  and the three `system_kills_{ship,npc,pod}_recent_schedule` rematerialise them
  hourly. SDE instead has `sde_build_discovery_sensor` (registers build partitions
  from `corpus everef list`), `sde_gold_sensor` (the changelog for builds with
  committed Silver), and `sde_snapshot_schedule` (daily rematerialise of the
  non-partitioned latest snapshot and `sde-industry-products`). All key status on corpus run-state, never on
  globbing the NAS tree; `run_key` dedup prevents re-queuing in-flight work.
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

- `dagster.yaml` — instance config; `QueuedRunCoordinator` (`max_concurrent_runs: 4`,
  the NAS-spindle I/O cap) plus concurrency pools (`heavy`, `everef_download`,
  `default_limit: 2`) that bound Gold memory and EVE Ref fetches across every launch
  path. Copy to `$DAGSTER_HOME/dagster.yaml`.
- `workspace.yaml` — code location (`eve_industry_orchestration.definitions`).
- `dagster-webserver.service` / `dagster-daemon.service` — systemd units running as
  `corpus`, with `CORPUS_*` env and `DAGSTER_HOME` set. Adjust the `WorkingDirectory`
  to the real clone path before installing.

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
