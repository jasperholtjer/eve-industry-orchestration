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
- **Concurrency.** `deploy/dagster.yaml` pins `max_concurrent_runs: 4` with a
  `tag_concurrency_limit` of 2 on `corpus/everef-download`. The single-HDD NAS is
  the real limiter (every run taps the one spindle), so the global cap stays
  modest; the EVE Ref lane is bounded separately because only Silver carries that
  tag and EVE Ref itself endorses ~2 parallel transfers. Gold (no EVE Ref) fills
  the rest. Keep sensor fan-out capped per tick.

## Testing without the Rust build

`uv run pytest` exercises the Silver path against a fake `corpus` binary
(`tests/fake_corpus.py`) that mimics the contract and the `missing-partitions` /
`state query` JSON. Point `CORPUS_BINARY_PATH` at it and `CORPUS_SINK_PATH` at a
throwaway dir to materialise locally — no compute repo or NAS needed.

## Coverage target

Keep the orchestration logic (`config.py`, `sensors.py`, `corpus_resource.py`)
covered by the fake-binary tests; the corpus boundary is the only seam, so test it.
