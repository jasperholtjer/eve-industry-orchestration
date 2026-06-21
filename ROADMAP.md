# Roadmap

Planned work for the orchestrator, ordered by dependency. The repo currently
materialises `market-history` partitions by hand; the goal is automated,
interval-driven materialisation against a pinned `corpus` release. Compute, data
config, and ADRs live in [eve-industry-corpus](https://github.com/jasperholtjer/eve-industry-corpus);
this repo owns only the partition matrix, scheduling, and the materialisation log.

## Confirmed corpus CLI surface

Verified against `crates/corpus-cli/src/main.rs`. `--sink-path` is a global flag
(default `/mnt/corpus`); per-tier roots default to `<sink-path>/silver` and
`<sink-path>/gold` (Gold primary is NVMe, mirrored to the NAS).

- `corpus ingest --dataset <name> --date <YYYY-MM-DD>` — one day to Silver.
- `corpus gold --dataset <name> --date <YYYY-MM-DD> [--silver-path] [--gold-path]`
  — Silver -> Gold for one date. **Not** `build --tier gold` (the placeholder in
  `market_history.py` guessed wrong).
- `corpus verify --dataset <name> [--date <d>] --tier <silver|gold> [--full]`.
- `corpus state query --sql <sql> --format json` — read-only, JSON for sensors.
- `corpus everef missing-partitions --dataset <name> --window-days <n> --format json`
  — diff EVE Ref availability against the local `partitions` table.
- `corpus mirror --dataset <name> --year <y> --month <m>` — rsync NVMe Gold -> NAS.

## Decisions

- **Binary delivery: deploy-script pins the version.** A deploy script downloads a
  specific `corpus` GitHub release asset to `/usr/local/bin/corpus`. The version is
  explicit and reproducible; Dagster only invokes the binary and never reaches the
  releases API at runtime. The binary bakes `CORPUS_VERSION` into `corpus --version`,
  so the deploy can assert the pin landed. Release-checking inside Dagster was
  rejected to keep deploy concerns out of orchestration.
- **Storage boundary: corpus owns the contract, orchestration owns placement.**
  Corpus owns the *contract* — the partition shape (`parquet + _INDEX.json +
  _DONE`), the `year=/month=/day=` layout, sha256 integrity, schema. Orchestration
  owns *placement policy* — which root a partition lands in (NVMe vs NAS, which
  mount), when to promote/mirror, retention tiering. The hard line: orchestration
  selects roots (flags / `CORPUS_*` env) and triggers contract-aware operations; it
  never constructs the path layout or moves contract bytes itself.
- **Gold coverage gate: binary authoritative, sensor pre-checks.** The binary must
  always enforce `gold.coverage_min_ratio: 1.0` (a by-hand `corpus gold` must not be
  able to corrupt Gold). The sensor additionally pre-checks coverage via `corpus
  state query` so it does not queue runs that immediately fail. The orchestration
  check is an optimisation, not a correctness dependency.
- **Cadence: sensor over fixed cron.** EVE Ref publishes one file per calendar day
  with ~1 day lag, and the intraday publish time is not fixed (the `2026-06-19` file
  appeared `2026-06-20 16:56 UTC`). A fixed daily cron would miss "yesterday"
  structurally. Drive materialisation from a Dagster sensor over
  `corpus everef missing-partitions ... --format json`, which fires when the data
  actually lands. The dataset's `daily_cron: cron(30 4 * * ? *)` is the declared
  baseline, not a hard contract.

## Work items

### 1. Correct the partition start dates (blocking) — done

> [!NOTE]
> Implemented in `defs/config.py`: Silver/Gold start dates are resolved from the
> dataset YAML (`gold.served_start`, minus the max rolling horizon for Silver),
> overridable via `CORPUS_<DATASET>_<TIER>_START`.

`market_history.py` uses `DailyPartitionsDefinition(start_date="2024-01-01")` for
both Silver and Gold — a placeholder that contradicts the config source of truth.

- Source of truth: `datasets/market-history.yaml` (corpus repo) —
  `gold.served_start: 2021-01-01`; Silver preload start `2020-01-02` (derived by
  resolving the rolling 365-day window back from `served_start`). Background:
  ADR 0019 (earliest legal Gold target) and ADR 0011 (365-day rolling window).
- **Silver and Gold need different start dates.** Gold's earliest date
  (`2021-01-01`) needs Silver back to `2020-01-02` to fill its window, so a single
  shared `DailyPartitionsDefinition` is wrong: Silver from `2020-01-02`, Gold from
  `2021-01-01`.
- Partition layout is `year={year}/month={month:02d}/day={day:02d}` (per the YAML),
  matching daily partitions.
- Prefer reading the dates from config (dataset YAML / env) over hardcoding, so the
  corpus config stays the single source of truth.

### 2. Wire the Silver -> Gold corpus subcommand (blocked upstream)

`market_history_gold` raises `NotImplementedError`. The CLI command now exists, but:

- **Upstream caveat:** `corpus gold` documents *"full rolling-window implementation
  deferred to plan 07 follow-up"* — the builder is not yet complete. Wiring it now
  would invoke a half-built builder. Track the corpus-side completion before
  enabling this asset; until then keep it failing loudly.
- When ready, wire `corpus gold --dataset market-history --date <d>` followed by
  `corpus verify --dataset market-history --date <d> --tier gold`. Drop the old
  `build --tier gold` guess and the per-tier `--sink-path` pattern.
- **Gold coverage gate** (decided): binary authoritative + sensor pre-check — see
  Decisions. The asset relies on `corpus gold` enforcing `coverage_min_ratio: 1.0`;
  the sensor pre-checks via `corpus state query` to avoid queuing doomed runs.
- **Gold on the NAS** (decided): the canonical Gold lives on the NAS — it is the
  single source of truth that downstream consumers fan out from (see Future phases).
  Default is option (a): point `corpus gold --gold-path` at the NAS directly (the
  365-day Silver read already comes from the NAS, so only the Gold write/serve loses
  NVMe speed). NVMe-primary + `corpus mirror` (option b) is revisited only if a
  consumer needs NVMe read speed — a corpus-side ADR touching storage layout (plan
  02). A hand-rolled rsync that rebuilds the layout is ruled out by the storage
  boundary; this repo only records the root choice.

### 3. Add an availability-driven sensor — done

> [!NOTE]
> Implemented in `defs/sensors.py`: `market_history_availability_sensor` polls
> `corpus everef missing-partitions` and emits one Silver `RunRequest` per newly
> available date (capped per tick, oldest first; `run_key` dedup). Status is keyed
> on corpus run-state via `missing-partitions`, never on globbing the NAS tree.

This is the "process on an interval" requirement. With no schedule today, all
materialisation is manual.

- Add a Dagster sensor polling `corpus everef missing-partitions --dataset
  market-history --format json` and requesting Silver runs for the newly available
  dates; Gold follows via the `deps=` chain once item 2 is unblocked.
- Respect concurrency: `deploy/dagster.yaml` pins `max_concurrent_runs: 4` (the NAS
  spindle is the real limiter) plus concurrency pools — `everef_download` (EVE Ref
  endorses ~2 parallel transfers) and `gold_heavy` (Gold memory), both at
  `default_limit: 2` — keyed on the assets' `pool=` so they bound every launch path.
  The sensor must also not stampede the queue, hence the per-tick cap.

### 4. Automate the release pull in deploy — done

> [!NOTE]
> `deploy/redeploy.sh` installs the corpus binary and its version-matched dataset
> configs from the private corpus release via `gh` — the **latest** release by
> default, or the `CORPUS_VERSION` pin — verifies the release `SHA256SUMS`,
> installs both into root-owned `/usr/local`, and asserts `corpus --version`
> matches the resolved tag. The install paths reuse the same `CORPUS_BINARY_PATH`
> / `CORPUS_DATASETS_DIR` env vars the systemd units pass to Dagster, so the
> running and deployed binaries cannot drift. Moving corpus is re-running redeploy
> (latest) or pinning with `CORPUS_VERSION=vX.Y.Z redeploy`.

Per the decision above. The pull is folded into the existing flow (`git pull` +
`uv sync` + binary pull + systemd restart); downloading from the private repo
needs `gh` authenticated as root (`gh auth login`, or `GH_TOKEN`).

### 5. Enrich materialisation metadata

Makes the "overview / logging" purpose of the repo real.

- Populate `MaterializeResult` metadata from `_INDEX.json` / `corpus state query
  --format json` (rows, retention_class) instead of the current static fields.

## Future phases

### Downstream fan-out from NAS Gold

Not now — sketched so the earlier items don't paint it into a corner. Once Gold is
served on the NAS, consumers fan out from that canonical contract, each as an
independent Dagster asset downstream of the Gold partition (the same `deps=` over
the NAS contract pattern Gold already uses on Silver):

- One loader per target (e.g. Postgres, Neo4j derivatives), never a single chained
  step — independent retry/backfill, independent cadence, and the NAS stays the
  source of truth that every derived store is rebuildable from.
- Per-target boundary call: a shape transform is compute (corpus, e.g. a `corpus
  load` subcommand); a plain load is placement (a thin orchestration loader). Decide
  per target when the phase lands.
- Trigger via a Dagster sensor, not a raw file watcher: a sensor is the subscriber
  model with lineage, backfill, and retry on top, and keyed on the `_DONE` contract
  it never fires on a half-written partition. Key the sensor on `corpus state query`
  (the SQLite run-state), not on globbing the NAS tree — cheap and race-safe.
- For any genuinely external / polyglot consumer, add a terminal emit asset that
  publishes an event to a queue once the internal loaders succeed. Keeps external
  systems decoupled while the emit itself stays in the materialisation log.

## Out of scope

- Compute, data validation, and the `parquet + _INDEX.json + _DONE` contract — owned
  by `eve-industry-corpus`.
- Homelab provisioning (LXC, NFS mount, build order) — documented in `homelab_docs`.
