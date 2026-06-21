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
  the corpus dataset YAML (`gold.served_start`, minus one rolling window for
  Silver) instead of hardcoding them. Override per tier with
  `CORPUS_<DATASET>_<TIER>_START`.
- **Dataset assets** (`defs/market_history.py`) — daily-partitioned Silver and Gold
  assets, with **distinct** partition start dates (Silver reaches back one rolling
  window before Gold). Gold depends on Silver via `deps=` (the data dependency runs
  over the NAS contract, not an IOManager).
- **Availability sensors** (`defs/sensors.py`) — two thin cap-and-dedup loops.
  The Silver sensor polls `corpus everef missing-partitions` and requests Silver
  runs for newly available dates; the Gold sensor polls `corpus gold ready-dates`
  and requests Gold runs for dates whose rolling window is complete (`deps=` is
  lineage only, so Gold needs its own trigger). Both key status on corpus
  run-state (the SQLite `partitions` table), never on globbing the NAS tree;
  `run_key` dedup prevents re-queuing in-flight dates.
- **Resources** (`defs/resources.py`) — binds `corpus` to the assets and sensors
  from env vars.

Adding a dataset later is a new module mirroring `market_history.py` (or a factory
once the second dataset lands).

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
  the NAS-spindle I/O cap) plus concurrency pools (`gold_heavy`, `everef_download`,
  `default_limit: 2`) that bound Gold memory and EVE Ref fetches across every launch
  path. Copy to `$DAGSTER_HOME/dagster.yaml`.
- `workspace.yaml` — code location (`eve_industry_orchestration.definitions`).
- `dagster-webserver.service` / `dagster-daemon.service` — systemd units running as
  `corpus`, with `CORPUS_*` env and `DAGSTER_HOME` set. Adjust the `WorkingDirectory`
  to the real clone path before installing.

Deploy = `git clone` + `uv sync` on the LXC, install the units, `systemctl enable --now`.
First-time host setup (LXC, UID/GID map, NFS mount, `gh` auth, `uv`) is in the
homelab `deploy-dagster-lxc.md` how-to.

- `redeploy.sh` — recurring update: pull + `uv sync` as `corpus`, pull the
  `CORPUS_VERSION`-pinned `corpus` binary + datasets from the private release
  (`gh`, checksum-verified, `--version`-asserted), publish `dagster.yaml` to
  `DAGSTER_HOME`, restart the services. Run as root from anywhere in the container
  (`bash /opt/eve-industry-orchestration/deploy/redeploy.sh`); it pulls itself and
  drops to `corpus` for the repo work so the corpus-owned tree is not rewritten as
  root. Bumping corpus is editing the pin (or `CORPUS_VERSION=v0.1.6 redeploy`) and
  re-running; the download needs `gh` authenticated as root (`gh auth login`, or
  `GH_TOKEN`). Symlink it onto PATH once for a bare `redeploy`:
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
