---
name: roadmap-next
description: Take one row from roadmap.yaml to merged on develop - scout the contract and the data, propose one OpenSpec change in a feature/<id> worktree, build it through builder subagents with one real run in a scratch Dagster instance against the corpus binary, verify with ruff and pytest, review once from outside, fix, archive, and merge --no-ff. Use this whenever the user wants to pick up, continue or finish a roadmap row, build the next feature, or work through the backlog in this repository - even when they mention neither OpenSpec nor the roadmap by name. A bounded change that is not a row is the `fix` skill, not this one.
compatibility: Requires the openspec CLI, uv, git with worktree support, a local develop branch, subagents, and for the real run a built corpus binary plus read access to the trees on Y:\.
metadata:
  author: eve-industry
  version: "2.0"
---

# Roadmap: next

One row, from where it stands to merged on `develop`. A row is one logical
topic — the size of one ADR or one dataset's wiring — and it may take more than
one session: the worktree and `tasks.md` carry it across. What a row is and how
big is [roadmap.yaml](../../../roadmap.yaml)'s to say; this procedure does not
resize it.

**Two entrances.** Work that moves what fires when — a partition definition or
a start date, a sensor's trigger condition, a schedule's cadence — what an asset
records, the memory budget in `deploy/dagster.yaml` or a recorded decision (an
ADR, `docs/serving-seam.md`, a `ROADMAP.md` decision), wires a dataset or a
Gold derivative that has no asset yet, adds a resource or something new to
shell out to, or needs a design choice with options, is a row and runs here.
Work that does none of those is a fix and runs through the `fix` skill: no
row, no OpenSpec change, no scout. Decide at intake, and decide again the
moment a fix answers yes to one of the three.

**Decide for yourself.** Naming, module layout, test shape, which helper goes
where — choose, and note the choice in the commit body. Step 6 lists the four
things that are not yours to settle.

## How this session is shaped

You orchestrate. Hold the index of the brief, `tasks.md` with every tick, and
every commit; delegate the rest. A source file that lands in this context is
paid for again on every turn that follows; a subagent's context is thrown away
when it returns. So:

- Never read a source file you are not about to quote. Never run the test
  suite here — `check-runner` returns the verdict and swallows the rest. Never
  write implementation code, and never write the spec artefacts from scratch:
  the scout drafts, you edit.
- **Never take a turn to wait.** A dispatched agent wakes this session when it
  lands. Dispatch in the background only when you have independent work in
  hand; otherwise dispatch in the foreground and let the turn end. A session
  that polls `git status` while an agent runs is the most expensive thing this
  repository has produced. There is no session goal and no stop hook: a stop
  is a stop, and a row that is not finished is picked up by the next session
  from its worktree.
- The roles are definitions. `row-scout`, `row-builder`, `row-reviewer`,
  `row-fixer` and `check-runner` carry their own instructions, model and
  budget in their agent definitions; hand over the task and the paths, never
  what they already carry. `AGENTS.md` reaches every subagent on its own.
  Override a model at the call only for a named reason, and name it in the
  report.
- Any bundle that adds or changes a Dagster definition consults the
  `dagster-expert` skill first; it tracks the installed Dagster version, and a
  definition written from memory is written against the wrong one.

## Two trees

- **The row's worktree**: `.worktrees/<id>` on `feature/<id>`, inside the
  repository and gitignored. A worktree is a full checkout, so `AGENTS.md`,
  `.agents/skills/` and `.pi/` are already there — what does not travel is
  Orca's registration of it, which is why Orca's per-repo *Worktree Location*
  points at this same path. Everything this session builds happens there, and
  every command says so: `git -C .worktrees/<id>`,
  `uv run --project .worktrees/<id>`. The shell's working directory resets
  between calls.
- **The root checkout** stays on `develop` and belongs to the person, who has
  it open in an editor. Reach into it for three things only: reading
  `docs/questions/`, writing a question there, and the merge.

**Never `git add -A`, never `git commit -a`.** Every commit names its paths.
`docs/questions/` is the person's and lives on `develop`; this session writes a
question there and edits nothing it did not write.

## 1. Intake

`git worktree list` and `git branch --show-current` first.

- A `feature/<id>` worktree already stands: that row is half-finished. Pick it
  up at step 4 rather than starting a second one.
- The root checkout is on `main`: stop and say so.

Read `docs/questions/` on `develop`. A file at `status: answered` is work
waiting for you (step 6 says how it is applied); a file at `status: open`
blocks the row it names.

Take the row, from the first of these that gives one: the id the caller named
or the session prompt carries; an unarchived OpenSpec change (`openspec list
--json`); the lowest-`priority` row in `roadmap.yaml` whose `depends_on` are
all `done` — `<repo>:<id>` reads `../<repo>/roadmap.yaml`, and an absent sister
checkout is an unsatisfied dependency, not a guess. A named row whose
dependencies are not all done: say which, and stop.

Apply the two-entrances test to what you took. A fix is the `fix` skill; say
so and hand over. Nothing is committed on `develop` at intake: a standing
worktree is what marks a row as taken, and its status moves to `done` in the
adoption commit.

Announce one line — `Row: <id> — <title>` — and carry on.

## 2. Worktree and change

```bash
git worktree add .worktrees/<id> -b feature/<id> develop
uv sync --project .worktrees/<id>
openspec new change <id>      # from inside the worktree; schema `row` is the default
```

`uv sync` because a fresh worktree has no `.venv`; it is seconds, not minutes.
`.env` is gitignored and does not come along, and this session may not read
it. The `row` schema is proposal, specs, tasks: there is no `design.md`,
because the ADR is the design wherever a row has one.

The row's scratch root for the real run is `C:\tmp\orchestration-scratch\<id>`
— a scratch `DAGSTER_HOME` and a scratch corpus sink. The builder creates it on
demand; step 7 removes it.

## 3. Scout, then propose

Dispatch `row-scout` with the row's id, goal and areas, the absolute worktree
path and the change directory. It answers six questions into
`<worktree>/tmp/brief.md` — the sixth is what the run-state, the trees on `Y:\`
and the corpus dataset YAML actually show — and drafts `proposal.md` and
`tasks.md` in the change directory from what it found. Every brief bullet opens
`[gen]`; step 4 stamps `[ver <role>]` on the ones a builder re-opened the
source for.

**Keep the index, not the brief.** Open a section only to quote it or to
settle a conflict the scout flagged; a `CONFLICT:` bullet is a finding, and a
finding of that shape is a question (step 6). A scout that reports `upstream:`
has found that the row needs a corpus subcommand, a `--format json` shape or a
Gold tree that does not exist; that is a question too, and its answer is a row
in `eve-industry-corpus`, never logic moved into Python here.

Then you, on the drafts, which are short: correct what the scout got wrong,
write the spec delta yourself where a capability's behaviour changes — only the
requirements that move, in the shape `openspec instructions specs --change
<id> --json` gives — or set `skip_specs: true` in `.openspec.yaml` where none
does, and run `openspec validate <id> --strict`. `openspec/config.yaml` carries
the rules a proposal here must meet. A row that wires a corpus dataset says it
runs the `add-dataset-to-orchestration` skill; it does not restate its
touchpoints.

The scope is the row's topic. A row that turns out to be two topics is two
rows; say so and stop. Never widen a proposal to match code you were already
imagining.

**Contract rows** — a platform-exclusive area (`gold-contract`, `api-contract`,
`calc`) in the row's `areas`, or a row that moves the memory budget in
`deploy/dagster.yaml` — get one more step: dispatch `row-reviewer` on the
proposal, the spec delta, `tasks.md` and the brief before any code exists.
`proceed`, `narrow` or `park`. Every other row takes the scout's `upstream:`
and `conflicts:` lines as that gate.

## 4. Build

Use the `openspec-apply-change` skill to read `tasks.md`, then hand the work
out. A bundle is a task heading — the tasks that share one file set. Bundles
with disjoint files run at the same time; bundles that share a file run in
sequence, and the later one is told what the earlier decided. Most rows here
are one area and therefore one bundle; do not manufacture parallelism a row
does not have.

Brief each `row-builder` in five lines, nothing else:

- the row's goal and this bundle's tasks, verbatim;
- the absolute worktree path and the paths this bundle owns — every other path
  belongs to another agent;
- the absolute path of the brief and the sections that bind this bundle;
- what the earlier bundle decided, where this one follows it;
- the scratch root, `C:\tmp\orchestration-scratch\<id>`.

**The real run is part of the bundle.** A bundle that touches an asset, a
sensor, a schedule or a resource method materialises one partition of that
asset, or previews one tick of that sensor, in a scratch Dagster instance
against the real `corpus` binary before it reports — the sink under the
scratch root, `Y:\` read and never written; `row-builder` carries the
commands. Testing in Dagster tests the orchestration, which is this repo's
product. It reports `run:`; a run that could not happen is evidence for the
reviewer, not a pass. The first materialise on the LXC stays the operator's.

Then you tick and commit. Confirm the report against `git status --short` and
`git diff --stat` — when they disagree, the tree is right — tick what is done,
stamp the brief, and commit by pathspec: `git -C .worktrees/<id> commit --
<paths>`. Small and often. Before each commit, read `docs/questions/` in the
root checkout again: an answer can arrive at any moment.

A bundle that comes back `stuck` gets one different approach or a question,
never the same thing twice. A bundle that reports `upstream:` has stopped
where it should; that is step 6.

## 5. Verify, review, fix

**Checks**, through `check-runner`, in the worktree:

```bash
uv run --project .worktrees/<id> ruff check .
uv run --project .worktrees/<id> ruff format --check .
uv run --project .worktrees/<id> pytest -q
```

The tests run against the fake `corpus` binary in `tests/fake_corpus.py`, so
they need neither a Rust build nor the NAS. Report which of the three ran. A
skipped suite is not a pass. There is no type checker here, deliberately;
`openspec/config.yaml` says why.

**One review, from outside.** Dispatch `row-reviewer` with the diff against
`develop`, the path to the brief, the row's goal, and the run evidence — the
builders' `run:` lines and the scratch root — and nothing else. It does not
share the assumptions that produced the code, and that is its value; do not
repair that by handing over the history. A row whose run did not happen where
the brief says it should is a finding, not an omission. A sensor that queues
doomed runs and one that never fires look identical in a unit test; the run is
what tells them apart, so hand the reviewer what it showed.

Contract rows add `/code-review` at `high` in this session, run alongside. No
other position exists; the run replaced it. `/security-review` opens on
`origin/HEAD` and this procedure never pushes, so it would review every
unpushed row rather than this one.

**Fix**: one `row-fixer` per finding, findings in one file to one agent,
independent findings in parallel, then `check-runner` for what the fix could
have broken. Override `row-fixer`'s model only for a correctness or
concurrency question or a second attempt, and say so. Two rounds is the
ceiling; still red after the second, stop with the tree committed and say what
is red.

A finding that would change an architecture invariant, a recorded decision or
the storage boundary is not a fix. It is step 6.

## 6. What parks a row

Four cases, and only these:

- a partition matrix, a start date or what an asset records would move under
  materialisations that already exist, or the corpus dataset YAML leaves it
  open;
- a new dependency — a corpus subcommand, a `--format json` shape or a Gold
  tree that does not exist, or a new service or resource to shell out to;
- a finding that would change an architecture invariant, a recorded decision
  or the storage boundary;
- a design fork whose consequences reach outside the row.

Not a question: anything a measurement answers — the run, one sensor tick, one
`corpus state query`, one `_INDEX.json`, one `/usr/bin/time -v`; measure it and
write the number where the decision is made. Not a question: a "not yet"
finding, which goes to `notes:` on its roadmap row or a *Known limits*
paragraph in the ADR; something noticed in another row's code, which goes to
that row's `notes:`; naming, layout and test shape, which you decide.

A question is one screen, in `docs/questions/<yyyy-mm-dd>-<slug>.md` on
`develop` in the shape its README gives, committed there on its own. Parking
means: commit what is finished in the worktree, write the question, report,
stop. The worktree stands.

An answer is applied by the session that reads it. Documents-only: apply and
resolve in one commit on `develop`. Code: apply in the worktree, and flip the
question to `resolved` on `develop` once the applying commit exists. A decision
becomes an ADR, rewritten in place, and the question file is deleted in that
commit.

## 7. Adopt, archive, merge

1. `openspec validate <id> --strict`, the `openspec-sync-specs` skill, then
   `openspec archive <id> --yes`.
2. `roadmap.yaml`: the row to `status: done`, with a `notes:` line where the
   row learned something the next row needs.
3. `README.md`, `ROADMAP.md` and the **State of the repository** paragraph in
   `openspec/config.yaml`, only where the row changed what they claim — a work
   item in `ROADMAP.md` moving to done is exactly such a change. This
   repository keeps no `CHANGELOG.md`.
4. One adoption commit, in the shape `git log` already uses.
5. Merge, and do not push.

```bash
git -C .worktrees/<id> status --porcelain   # empty, or stop and commit
git worktree remove .worktrees/<id>         # deregisters; may leave .venv
rm -rf .worktrees/<id>                      # finish the job
rm -rf C:/tmp/orchestration-scratch/<id>
git merge --no-ff feature/<id>
git branch -d feature/<id>
```

`--no-ff` because the row is the unit and the merge is its boundary in
`git log`. If the merge refuses because a file is modified in the root
checkout, stop and say which: that is the person's work.

Commit shape: imperative subject of at most 72 characters, no prefix, a body
only where the why is not obvious, naming the row (`Implements <id>`) and any
question it resolves.

## 8. Report, then stop

Six lines: the row; what the three checks returned; what the review found and
what became of it; the run — what ran, on which partition or tick, what it
showed; what was parked; every model override and its reason.
