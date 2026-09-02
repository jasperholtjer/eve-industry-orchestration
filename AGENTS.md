# eve-industry-orchestration

Thin Dagster orchestrator. It owns the partition matrix, scheduling, and the
materialisation log — nothing else. All compute and the data contract live in the
**`eve-industry-corpus`** repo. Scope-specific rules only; defer to the global
rules for Python, git, and PowerShell conventions.

## Read first

- [`CONTEXT.md`](CONTEXT.md) — the words this repo uses; read it before writing
  anything a person or a later session reads, and use its word rather than a
  near-miss
- [`ROADMAP.md`](ROADMAP.md) — the corpus CLI surface, the decisions, and the
  work items behind them
- `tmp/` — local scratch (gitignored). Often holds the newest thinking; read it,
  never promote it into `docs/` unasked.

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
- **Concurrency.** Two layers in `deploy/dagster.yaml`: a global
  `max_concurrent_runs` I/O cap — the single-HDD NAS spindle is the real limiter,
  every run taps it — and per-class limits as concurrency **pools** keyed on the
  assets' `pool=`, not run tags. A pool gates **every** launch path — sensor, UI
  backfill, manual — unlike a sensor-set run tag, and a pooled run is bounded by
  `min(global, pool)`. Lightweight datasets omit `pool=` and obey only the global
  cap. Keep sensor fan-out capped per tick.
  - **Membership of a memory-bearing pool is by measured peak**, never by "is
    windowed" — a narrow build put in one only starves the big backfills of
    scarce slots. Measure with `/usr/bin/time -v` (needs `CORPUS_DATASETS_DIR`)
    before adding a build. Every memory-bearing pool counts against **one** box
    budget: the pools do not know about each other, so their peaks add.
  - **The arithmetic lives in [`deploy/dagster.yaml`](deploy/dagster.yaml)** and
    nowhere else — which pools exist, their limits, each holder's peak, the worst
    case against the box, and where the box itself is provisioned. Read it before
    changing a limit or adding a pool; `tests/test_concurrency_pools.py` pins the
    set of declared pools so a new one cannot arrive unaccounted.

## How a change is worked

**Two entrances.** A **row** is one logical topic — the size of one ADR or one
dataset's wiring — and is one OpenSpec change, one worktree, one merge. It
comes from [`roadmap.yaml`](roadmap.yaml), whose `depends_on` may point into a
sister repo as `<repo>:<id>`, and the `roadmap-next` skill carries it from
picked to merged. Work that moves nothing of what fires when (a partition
definition, a start date, a sensor's trigger condition, a schedule's cadence),
nothing an asset records, no memory budget in `deploy/dagster.yaml` and no
recorded decision, wires no dataset and adds nothing to shell out to, and needs
no design choice with options is a **fix**: the `fix` skill, a `fix/<slug>`
worktree, no row and no OpenSpec change. `ROADMAP.md` stays what it is — the
corpus CLI surface, the decisions, and the work items behind them — and what to
work on next across all six repos is the platform-level `next` skill, one
directory up.

**What could be built here at all** — the open rows, the work items `ROADMAP.md`
has not finished, the future phases it describes without ever making a row, and
what the decisions deferred — is this repo's `candidates` skill. It is scoped to
this repository on purpose: the same question across all six repos is the root's
`candidates`, and a candidate whose answer is compute in Python is a corpus row,
named and handed over, never built here. It reports; it never edits
`roadmap.yaml` or `ROADMAP.md`.

| Skill | When you reach for it |
|---|---|
| `roadmap-next` | build one row, from picked to merged on `develop` (you type it) |
| `candidates` | what could still be built in this repository (you type it) |
| `fix` | one bounded change that is not a row |
| `add-dataset-to-orchestration` | wire a corpus dataset, all its touchpoints |
| `openspec-propose` / `-apply-change` / `-archive-change` / `-explore` / `-sync-specs` | the OpenSpec change a row is written as |
| `dagster-expert` | any Dagster definition, before you write it |

The two marked *(you type it)* are user-invoked and will not fire on their own.
The rest stay model-invoked because other skills call them — a user-invoked skill
cannot be. Which row to start, across all six repositories, is the `next` skill
one directory up.

- **Worktrees.** `.worktrees/<id>` on `feature/<id>`, or `.worktrees/<slug>`
  on `fix/<slug>`, branched from `develop`. The root checkout stays on
  `develop` and belongs to the person; a command that means the worktree says
  so (`git -C`, `uv run --project`).
- **A real run before review.** A bundle that touches an asset, a sensor, a
  schedule or a resource method materialises one partition of that asset, or
  previews one tick of that sensor, in a scratch Dagster instance against the
  real `corpus` binary before it is reviewed: `DAGSTER_HOME` and
  `CORPUS_SINK_PATH` under `C:\tmp\orchestration-scratch\<id>`, `Y:\` read and
  never written — a materialise whose sink is `Y:\` is a defect. Testing in
  Dagster tests the orchestration, which is this repo's product; the run's
  evidence goes to the reviewer. The first materialise on the LXC stays the
  operator's.
- **One review, from outside**, by `row-reviewer` with the diff, the brief and
  the run evidence. `/code-review` in the session only for contract rows: a
  platform-exclusive area (`gold-contract`, `api-contract`, `calc`) in
  `areas`, or a row that moves the memory budget in `deploy/dagster.yaml`.
- **No session goal, no stop hook, no turn spent waiting.** A dispatched agent
  wakes the session when it lands; a row that is not finished is picked up by
  the next session from its worktree.
- **A dataset is not wired freehand.** The `add-dataset-to-orchestration`
  skill carries the touchpoints and the polymorphic-Gold mapping; a change
  invokes it rather than reassembling them. Any Dagster definition consults
  the `dagster-expert` skill first.
- **Work autonomously.** Do not ask unless the answer materially blocks correct
  implementation; what a measurement can answer is measured, not asked. When one
  thread is blocked, carry on with the independent work in hand rather than
  stopping the session. Where the harness has a checkpoint surface, update it
  after meaningful progress and when blocked, so the state is legible without
  reading the transcript; under Orca that is `orca worktree set --worktree
  active --comment "..."`. It is a checkpoint, never a third place to ask
  something.
- **Two ways to ask, and one test decides which.** *Would this question still
  have to exist if this session were killed right now?* **No** — it blocks only
  this session, and it is asked with the harness's own question tool, which
  surfaces it as work waiting on the person; nothing is written down. **Yes** —
  it blocks a roadmap row, and it is a file in
  [`docs/questions/open/`](docs/questions/README.md), committed on `develop`,
  never onto the row's branch, and only for the four cases its README names, on
  one screen. The folder is the state: `open/` waits on the person, `answered/`
  waits on a session, and there is no `resolved/` — applying an answer lands it
  in an ADR, a test or the code and deletes the file.
  A row that needs a corpus subcommand or a Gold shape that does not exist
  asks; its answer is a corpus row, never logic moved into Python here.
- **Under an Orca dispatch, the coordinator is the harness.** A session started
  as a worker by the platform's `orchestrate` skill was handed a task id, a
  dispatch id and a preamble; nothing else about how a row is built changes. The
  row's worktree is still this repository's to create and to merge —
  `.worktrees/<id>` on `feature/<id>` — because Orca placed the worker in this
  checkout, not in a worktree of its own. What changes is two things. The *no*
  half of the question test is asked with
  `orca orchestration ask --question "..." [--options a,b]`, which blocks until
  the coordinator answers, instead of the harness's question tool; the *yes* half
  is unchanged and still lands in `docs/questions/open/` on `develop`. And the
  session ends by reporting before it stops:
  `orca orchestration send --type worker_done --subject "<row>: <what happened>"
  --body "<what merged, what is parked, what remains>" --task-id <id>
  --dispatch-id <id> --outcome succeeded|failed`. A stop is still a stop; what
  changes is that the coordinator is told first. A parked row reports `failed`
  naming the question file — that is not a judgement on the work, it is how the
  coordinator learns the row is blocked.
- **The roles are definitions, not prompts.** `row-scout`, `row-builder`,
  `row-reviewer`, `row-fixer` and `check-runner` live in `.pi/agents/`, each with
  its own tools, thinking level and time budget; the model tiers are in
  `.pi/settings.json`. Dispatch one by name with the task and the paths; do not
  restate what its definition or this file already says. None of them carries the
  `subagent` tool, which is what keeps the delegation one level deep — the depth
  is closed by the tool list, not by a setting.
- **Commit by pathspec.** Never `git add -A`, never `git add .`, never
  `git commit -a`, and never `git push` — the remote is the person's. Name every
  path the commit contains, including a deletion: a rename staged as
  `R old -> new` is two paths, and a pathspec that lists only the new one commits
  half of it.
- **`.env` and `.env.*` are never read.** The deployment's secrets reach the
  binary through the environment on the LXC, never through a session's context.
- **Every "never" rule lives in two layers, on purpose.** In this file it is
  prose, and prose is the only layer that travels: a clone, a different harness,
  a session that loads no extension all still read `AGENTS.md`. In
  [`.pi/extensions/guard.ts`](.pi/extensions/guard.ts) it is a refusal, and a
  refusal is the only layer that holds when the prose is skimmed. The guard
  refuses five things, and each one is prose somewhere above: a write to `Y:\`
  or a materialise against it, a write into `../eve-industry-corpus/`, a write
  outside the active row's worktree, `git add -A` / `commit -a` / `push`, and a
  read of `.env`. A rule that exists in only one of the two layers is half a rule
  — so when a "never" is added or changed here, the guard changes in the same
  commit, and the reverse.
- **A personal override is `AGENTS.override.md`**, gitignored. pi loads it
  *instead of* this file in the directory that holds it — not on top of it — so
  it is a fork of the rules for one machine, never a patch.
- **ADRs are not append-only.** A superseded record under `docs/adr/` is
  rewritten or deleted, never stacked with a successor — the reason is in
  [`docs/decisions/not-taken/`](docs/decisions/not-taken/README.md).
- **A declined idea is written down**, in
  [`docs/decisions/not-taken/`](docs/decisions/not-taken/README.md) when it
  would have been built here, and in the platform's `not-taken/` when it belongs
  to no single repository. `candidates` prints the folder before it proposes
  anything, which is the only reason it is worth keeping. Re-opening one argues
  against the file by name, with new evidence, and deletes it in the change that
  makes the row.
- **Verify before you commit**, not through a failing hook:

  ```bash
  uv run ruff check . && uv run ruff format --check . && uv run pytest -q
  ```

- **`openspec/config.yaml` carries the repository context** every change is
  written against, and names the `row` schema: proposal → specs → tasks, no
  `design.md`, because the ADR is the design wherever a row has one. When a
  row changes what the context paragraph claims, the same change updates it.
- **No CHANGELOG.** The specs and the decisions are the record; nothing here is
  released, so there is no section for an entry to belong to
  ([why](docs/decisions/not-taken/2026-09-02-a-changelog-in-this-repository.md)).

## Testing without the Rust build

`uv run pytest` exercises the Silver path against a fake `corpus` binary
(`tests/fake_corpus.py`) that mimics the contract and the `missing-partitions` /
`state query` JSON. Point `CORPUS_BINARY_PATH` at it and `CORPUS_SINK_PATH` at a
throwaway dir to materialise locally — no compute repo or NAS needed.

## Coverage target

Keep the orchestration logic (`config.py`, `sensors.py`, `corpus_resource.py`)
covered by the fake-binary tests; the corpus boundary is the only seam, so test it.
