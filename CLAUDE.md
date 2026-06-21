# eve-industry-orchestration

Thin Dagster orchestrator. It owns the partition matrix, scheduling, and the
materialisation log — nothing else. All compute and the data contract live in the
**`eve-industry-corpus`** repo. Scope-specific rules only; defer to the global
rules for Python, git, and PowerShell conventions.

## Use the Dagster skill

Invoke the `dagster-expert` skill before any Dagster-specific work (assets,
partitions, sensors, schedules, components, the `dg` CLI). It is the official,
upstream-maintained `dagster-io/skills` skill and tracks the installed Dagster
version — **do not hand-edit it**; refresh it from upstream instead so it stays
eval-tested and current.

## Architecture invariants

These are decisions, not preferences — breaking them defeats the repo's purpose.
The full rationale lives in [ROADMAP.md](ROADMAP.md); the load-bearing rules:

- **Thin orchestration.** Dagster only invokes the `corpus` binary and records the
  run. No compute, no run-state, no data validation here.
- **Storage boundary.** Corpus owns the *contract* (the `parquet + _INDEX.json +
  _DONE` shape, the `year=/month=/day=` layout, sha256, schema). Orchestration owns
  *placement* — it selects roots via flags / `CORPUS_*` env and triggers
  contract-aware operations. Never construct the path layout or move contract bytes
  from Python.
- **The corpus repo is read-only.** Never modify `../eve-industry-corpus`; it owns
  the binary, the CLI contract, and the dataset YAML config.
- **Config is the source of truth.** Partition start dates come from the dataset
  YAML (`gold.served_start`, minus the rolling window for Silver) via
  `defs/config.py`, never hardcoded. Silver and Gold have distinct starts.
- **Sensor over cron.** EVE Ref publishes with a variable lag, so availability is
  driven by the sensor polling `corpus everef missing-partitions`. Status is keyed
  on corpus run-state (the SQLite `partitions` table), **never** on globbing the
  NAS tree.
- **Gold gate is binary-authoritative.** `corpus gold` reads the full
  `[date - max_horizon, date]` Silver window and enforces `coverage_min_ratio:
  1.0` itself; an incomplete window exits non-zero and fails the run. The
  `market_history_gold` asset only shells out and records the run — it never
  pre-validates the window in Python. The sensor pre-checks only to avoid
  queuing doomed runs.
- **Concurrency.** Two layers in `deploy/dagster.yaml`. `max_concurrent_runs: 4`
  is the global I/O cap — the single-HDD NAS spindle is the real limiter, every
  run taps it. Per-class limits are concurrency **pools** keyed on the assets'
  `pool=`, not run tags: `everef_download` (Silver fetch politeness; EVE Ref
  endorses ~2 parallel transfers) and `gold_heavy` (Gold memory), both at
  `default_limit: 2`. A pool gates **every** launch path — sensor, UI backfill,
  manual — unlike a sensor-set run tag, and a pooled run is bounded by
  `min(global, pool)`. Lightweight datasets omit `pool=` and obey only the global
  cap. Keep sensor fan-out capped per tick.
  - **Gold memory governs the `gold_heavy` pool.** A Gold build streams its
    `[date - max_horizon, date]` Silver window via a k-way merge (corpus
    ≥ v0.1.6) and peaks ~3–4 GiB in the `corpus` subprocess. Peak Gold RAM ≈
    `gold_heavy limit × ~4 GiB`, so set the limit to `floor((RAM_GiB − ~4
    headroom) / 4)` — at the default 2, budget ~8 GiB for Gold alone, so the LXC
    wants **≥ 12 GiB RAM** (or drop the pool to 1 at 8 GiB). Measure the real peak
    with `/usr/bin/time -v` before raising. Set RAM/cores on the Proxmox host, not
    here: `pct set 211 --cores 4 --memory 12288 --swap 2048`. Authoritative host
    provisioning lives in `homelab_docs` (`docs/howto/deploy-dagster-lxc.md`).

## Testing without the Rust build

`uv run pytest` exercises the Silver path against a fake `corpus` binary
(`tests/fake_corpus.py`) that mimics the contract and the `missing-partitions` /
`state query` JSON. Point `CORPUS_BINARY_PATH` at it and `CORPUS_SINK_PATH` at a
throwaway dir to materialise locally — no compute repo or NAS needed.

## Coverage target

Keep the orchestration logic (`config.py`, `sensors.py`, `corpus_resource.py`)
covered by the fake-binary tests; the corpus boundary is the only seam, so test it.
