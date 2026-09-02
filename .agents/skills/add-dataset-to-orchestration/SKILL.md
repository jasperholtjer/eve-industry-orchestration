---
name: add-dataset-to-orchestration
description: >-
  Step-by-step procedure for wiring a corpus dataset into the
  eve-industry-orchestration Dagster code location, modelled on the landed
  market-history assets. Use this whenever the user wants to orchestrate,
  schedule, materialise, or add Dagster assets/sensors for a corpus dataset
  (e.g. system-jumps, market-orders) — including phrasings like "wire up
  <dataset> in Dagster", "add the orchestration for <dataset>", "schedule
  <dataset>", "make a Gold sensor for <dataset>", or "materialise <dataset>
  partitions". Covers the polymorphic-Gold mapping (ADR-0025: rolling /
  flat-multi-horizon / recency-weighted → daily asset+sensor vs scheduled
  rematerialise), the per-derivative `--derivative` gotcha, the config
  resolver, sensors, resources, tests, and deploy. NOT for the corpus Rust
  data plane (ingestor, schema, golden fixtures) — that is the sibling
  `add-everef-dataset` skill in the corpus repo; this skill is the thin
  Dagster shim layer only.
---

# Wire a corpus dataset into the Dagster orchestrator

This codifies the path `market-history` took so the next dataset lands with the
same contracts instead of being reinvented. The hard rule of this repo
(`AGENTS.md`): **assets are thin Python shims that `subprocess.run` the `corpus`
binary or read its `--json` subcommands.** No `pandas`/`pyarrow`/`rusqlite`, no
business logic, no partition bytes in a Python process. If a shim grows past a
trivial dispatch, push the logic into a Rust subcommand instead — consult the
`dagster-expert` skill before adding or changing any Dagster definition.

Read `defs/market_history.py`, `defs/config.py`, and `defs/sensors.py` alongside
this skill — they are the worked reference. This document tells you *what* to
replicate and *why*; the code shows *how*.

## Boundary check: is this an orchestration task at all?

This skill is for the **Dagster shim layer**. It assumes the dataset already
exists in the corpus Rust workspace (a registered ingestor, a `datasets/<name>.yaml`
config, and a working `corpus ingest` / `corpus gold build`). If the dataset is
*not* yet in corpus, stop — that is the `add-everef-dataset` skill in the corpus
repo. Wiring orchestration for a dataset the binary cannot build is dead code.

Confirm the corpus side is real before writing any asset:

```bash
corpus ingest      --dataset <name> --date <a-known-date> --sink-path <tmp>
corpus gold build  --dataset <name> --derivative <d> --date <a-known-date> --sink-path <tmp>
corpus gold ready-dates --dataset <name> --derivative <d> --format json
```

## Ground yourself in the dataset's Gold shapes first

The whole orchestration design follows from **how many Gold derivatives the
dataset declares and what shape each is** (ADR-0025). Read `datasets/<name>.yaml`
in the corpus repo and list every entry under `gold:`. Each derivative is its own
Hive tree (`gold/<derivative>/...`) and maps to its own orchestration construct.

The **`--derivative` selector is the central gotcha.** `corpus gold build` and
`corpus gold ready-dates` take an optional `--derivative <name>`:

- A dataset with **exactly one** derivative resolves automatically — omit the
  flag (market-history does). The resource methods can stay derivative-agnostic.
- A dataset with **two or more** derivatives is **ambiguous** — the binary exits
  non-zero unless `--derivative` is passed. Each derivative therefore needs its
  **own asset and its own readiness trigger**, each passing its `--derivative`.

So: count the derivatives first. **Zero** (the `gold:` key is omitted — a
Silver-only dataset, which ADR-0025 explicitly allows) ⇒ no Gold asset, no Gold
sensor; ship just the Silver asset + availability sensor and stop. **One** ⇒ copy
market-history almost verbatim. **Two+** ⇒ one asset (and trigger) per derivative,
and the resource methods must thread a `derivative` argument through. Multiple
derivatives of the *same* shape (e.g. two `rolling`) are still one asset+sensor
each — the axis below decides the construct per derivative, the count decides how
many.

## Map each Gold shape to an orchestration construct

The decisive question for **every** derivative — known shape or one added later —
is one axis: **is this output a backfillable historical matrix, or a
point-in-time "latest" signal?** Everything else follows from that answer, so map
by the axis rather than by memorising the table.

- **Backfillable historical matrix** — each date is an independent, reproducible
  artifact you may want to fill for any past day. Construct: a **daily-partitioned
  Gold asset + a `ready-dates` sensor**. The binary owns the readiness decision
  (coverage gate); the sensor is a thin cap-and-dedup loop that never recomputes
  coverage in Python.
- **Point-in-time "latest" signal** — only "now" is meaningful; backfilling a
  past value is misleading bookkeeping. Construct: a **single non-partitioned
  asset that a `dg.schedule` rematerialises** against the latest available date.

The known shapes map onto that axis as follows:

| Gold shape (`gold[].shape`) | Axis | Look-back / gate | Construct |
| --- | --- | --- | --- |
| `rolling` | historical matrix | max horizon window, coverage gate | Daily Gold asset + `ready-dates` sensor |
| `flat-multi-horizon` | historical matrix | max `flat.horizons` window, coverage gate | Daily Gold asset + `ready-dates` sensor |
| `recency-weighted` (EWMA) | point-in-time | no fixed window, no coverage gate | Scheduled non-partitioned rematerialise |

Why `recency-weighted` lands on the point-in-time side even though
`corpus gold ready-dates` *does* report it (gate = target-day Silver present, no
coverage window, so a daily sensor is technically possible): it answers "what is
the heat **right now**", not "reconstruct any historical day". A daily partition
matrix would imply a backfill story that does not exist. Resolve the date to
build from `corpus gold ready-dates --derivative <d>` (take `max(ready)`) or
`corpus state query` for the max Silver date — never hardcode "today" (EVE Ref
lags ~1 day and the publish hour drifts).

A **shape not in this table** (a future ADR-0025 arm) needs no new skill — ask
the axis question. If it has a fixed look-back / coverage gate and each day is
reproducible, it is a historical matrix (daily asset + sensor). If it only makes
sense "now", it is point-in-time (scheduled rematerialise). When genuinely
ambiguous, default to the historical-matrix pattern — it is backfillable and a
schedule can always be layered on later, whereas a non-partitioned asset cannot
be retrofitted with history.

## The touchpoints

Adding a dataset is mechanical once the shapes are mapped. Do all that apply.

### 1. `defs/config.py` — partition starts from the polymorphic `gold` list

`config.py` already resolves partition starts **per `(dataset, derivative)`** from
the ADR-0025 `gold` **list** (a single-derivative dataset is a one-element list).
A new dataset of an existing shape needs **no resolver change** — only test
coverage. The resolver is config-driven and dataset-agnostic; never pin a dataset's
dates here. Understand the resolution it performs (and extend it only for a
genuinely new shape):

- Find the derivative by name in the `gold:` list.
- Gold start = that derivative's `served_start` (a `recency-weighted` derivative
  may have **none** — for the scheduled "latest" model it has no partition matrix,
  so it needs no Gold start).
- Silver preload start = Gold start − the derivative's look-back window:
  - `rolling` → `max(horizons_basic ∪ horizons_vwap ∪ horizon_52w)`
  - `flat-multi-horizon` → `max(flat.horizons)`
  - `recency-weighted` → the EWMA warmup (a few half-lives); only relevant if a
    `recency-weighted` derivative ever gets a partition matrix — for the schedule
    model the Silver start is driven by the *other* derivative(s).
- **Coverage floor (ADR-0027).** The derived preload can reach before the dataset's
  upstream data exists. When the YAML declares `silver.served_start`, the resolver
  clamps Silver up to it: `max(derived_preload, silver.served_start)`. Omitted ⇒ no
  floor (the historical default). This is config-owned — if a new dataset's window
  reaches before its dense upstream coverage, the fix is the YAML field, not a
  constant in Python.
- `CORPUS_<DATASET>_<TIER>_START` env overrides still win outright, including over
  the floor; the per-derivative `CORPUS_<DATASET>_<DERIVATIVE>_GOLD_START` key
  disambiguates a dataset with more than one windowed Gold start.

Silver is shared across all derivatives of a dataset (one Silver tree feeds them
all), so its start is the **earliest** preload across the windowed derivatives,
then floor-clamped.

When you do touch the resolver (a genuinely new shape), update
`tests/test_config.py` to cover it and the floor clamp.

### 2. `defs/<dataset>.py` — the assets

Mirror `market_history.py`. Per dataset:

- **One Silver asset** (`<dataset>_silver`): daily-partitioned on the resolved
  Silver start; `corpus ingest` then `corpus verify --tier silver`. Carries
  `pool="everef_download"` (politeness cap on EVE Ref fetches). Identical shape
  regardless of how many Gold derivatives exist — Silver is shared.
- **One Gold asset per windowed derivative** (`<dataset>_<derivative>_gold` or a
  clear short name): daily-partitioned on that derivative's Gold start;
  `deps=[<dataset>_silver]` (lineage only — it does **not** trigger Gold, the
  sensor does); `corpus gold build --dataset <name> --derivative <deriv>` then
  `corpus verify --tier gold --dataset <deriv>`. Carries `pool="heavy"` only
  if the build is memory-heavy; lightweight derivatives omit `pool=`.
  - **Gold verify keys on the derivative name, not the dataset.** Gold writes to
    `gold/<derivative>/…` and `corpus verify --tier gold` resolves
    `gold/<--dataset>/…` — so the Gold verify call passes the **derivative name**
    as `--dataset` (Silver verify still uses the dataset name). For a
    single-derivative dataset the two names coincide (market-history), which is
    why this only bites once a dataset has a derivative named differently from
    itself. Confirm against `partition_path_for` / the gold-build write path in
    `crates/corpus-cli/src/main.rs` for the dataset at hand.
- **For a `recency-weighted` derivative**: one **non-partitioned** asset
  (`<dataset>_<derivative>`) that resolves the latest buildable date itself
  (`corpus gold ready-dates --derivative <name>` → `max(ready)`) and calls
  `corpus gold build --derivative <name> --date <latest>`. No `deps=` partition
  chain; it is driven by a schedule (touchpoint 3), not a sensor.

Use distinct `group_name` per dataset so the UI groups cleanly.

Once a second dataset lands, factor the Silver/windowed-Gold asset bodies into a
small factory (the README anticipates this) rather than copy-pasting a third
time — but only after the second concrete case shows what actually varies.

### 3. `defs/sensors.py` (windowed) and a schedule (recency-weighted)

- **Silver availability sensor** — one per dataset: polls
  `corpus everef missing-partitions --dataset <name> --format json`, requests a
  Silver run per newly available date, capped per tick (oldest first), `run_key`
  dedup. Status keyed on corpus run-state, never on globbing the NAS. (This
  assumes an EVE Ref source — the only time-series shape corpus ingests today. A
  non-everef source would need a different availability signal here, but the
  same cap-and-dedup loop.)
- **Gold readiness sensor** — one per **windowed** derivative: polls
  `corpus gold ready-dates --dataset <name> --derivative <name> --format json`
  and requests a Gold run per ready date. `deps=` is lineage only, so Gold needs
  its own trigger. Keep it a thin cap-and-dedup loop — the readiness decision
  lives in the binary.
- **Recency-weighted schedule** — one `dg.schedule` per `recency-weighted`
  derivative, targeting its non-partitioned asset, at a cadence matching the
  freshness you want (e.g. hourly for navigate-now heat). No sensor: there is no
  per-date matrix to diff.

Set `default_status=dg.DefaultSensorStatus.STOPPED` (operator enables explicitly),
`minimum_interval_seconds=300`, and reuse `MAX_PARTITIONS_PER_TICK`.

### 4. `defs/corpus_resource.py` — thread `--derivative`

If the dataset is multi-derivative, extend `gold_ready_dates` (and any gold-build
helper, though the asset usually shells `run(...)` directly) to accept and pass
`--derivative <name>`. Confirm the JSON shape: `ready-dates` returns the `ready`
date list plus `derivative` / `served_start` keys — the sensor reads `ready`.
Keep methods derivative-agnostic for single-derivative datasets (no flag).

### 5. `defs/resources.py` + `definitions.py`

- Bind `corpus` to the new assets, sensors, and schedules in `resources.py`.
- Ensure the new `defs/<dataset>.py` and its sensors/schedules are picked up by
  the code location (`definitions.py` / the `defs` package autoload). Run
  `dg dev` and confirm every new asset, sensor, and schedule appears.

### 6. `tests/` — extend the fake corpus

`tests/fake_corpus.py` mimics the contract without the Rust build. Extend it to:

- Answer `gold ready-dates --derivative <name>` with the real JSON shape
  (`{"derivative", "served_start", "ready": [...]}`) for each derivative.
- Honour `--derivative` in `gold build` (write to `gold/<derivative>/...`).
- Cover the new sensors' `RunRequest` generation and the config-derived starts
  for the dataset's shapes. Add a test that a multi-derivative `gold build`
  **without** `--derivative` fails (matches the binary's ambiguity error).

### 7. `deploy/dagster.yaml`

- Assign heavy Gold builds to the `heavy` pool and EVE Ref fetches to
  `everef_download` via the assets' `pool=` (the pools are defined here).
- A high-cadence recency-weighted schedule still shares `max_concurrent_runs` and
  the `heavy` pool — verify it cannot starve the windowed backfills (cap its
  cadence or give it its own small pool if it competes). A new pool means
  adding its measured peak to the budget in `deploy/dagster.yaml` and its name
  to `EXPECTED_POOLS` in `tests/test_concurrency_pools.py`.

## Verify before declaring done

- `uv run pytest` — config, sensor, and resource tests green, including the
  multi-derivative ambiguity case.
- `uv run dg dev` — every new asset, sensor, and schedule loads without error and
  appears in the UI under the dataset's group.
- `ruff check --fix` and `ruff format` on every touched file.
- Spot-check against a throwaway `CORPUS_SINK_PATH`: materialise one Silver
  partition and one Gold partition (or the recent asset) and confirm the
  `parquet + _INDEX.json + _DONE` contract appears — the binary owns the write,
  the asset only shells out.
- Confirm the resolved partition starts match the corpus YAML (`served_start`
  per derivative minus the look-back, then clamped up to any `silver.served_start`
  coverage floor), not a hardcoded date.

## CLI surface (verify against the corpus binary, not docs)

The orchestration `ROADMAP.md` predates `corpus gold build` and still shows the
older `corpus gold` form — **trust `crates/corpus-cli/src/main.rs`**, not the
roadmap. Current surface:

- `corpus ingest --dataset <n> --date <d> --sink-path <p>`
- `corpus gold build --dataset <n> --derivative <d> --date <date> [--silver-path] [--gold-path]`
- `corpus gold ready-dates --dataset <n> --derivative <d> --format json`
- `corpus verify --dataset <n> [--date <d>] --tier <silver|gold>`
- `corpus everef missing-partitions --dataset <n> --sink-path <p> --format json`
- `corpus state query --sql <sql> --format json`

`--sink-path` is global (default `/mnt/corpus`); per-tier roots default to
`<sink-path>/silver` and `<sink-path>/gold/<derivative>`.

## Worked reference: market-history (landed) and system-jumps (next)

- **market-history** — one `rolling` derivative ⇒ no `--derivative`, one Silver
  asset + one Gold asset + one Gold sensor. The verbatim template.
- **system-jumps** — two derivatives ⇒ the multi-derivative path:
  `system-traffic-history` (`flat-multi-horizon`, daily Gold asset +
  `ready-dates --derivative system-traffic-history` sensor, Gold served
  `2022-01-01`, Silver preload 365d back but floor-clamped to
  `silver.served_start: 2021-07-01` per ADR-0027) and `system-traffic-recent`
  (`recency-weighted`, scheduled non-partitioned rematerialise — **not** a
  sensor). This is the case
  that forces every multi-derivative consideration above; see the implementation
  brief under `.tmp/prompts/` (gitignored, local) if present.
