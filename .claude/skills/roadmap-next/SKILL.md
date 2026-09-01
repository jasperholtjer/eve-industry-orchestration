---
name: roadmap-next
description: Take one row from roadmap.yaml all the way to merged - read the contract through a scout, propose and apply one OpenSpec change in a feature/<id> worktree through builder subagents, verify, review, fix, archive, and merge --no-ff into develop. Use this whenever the user wants to pick up, continue or finish a roadmap row, build the next feature, work through the backlog, or asks what to work on next in this repository - even when they mention neither OpenSpec nor the roadmap by name, and including when they have set a session goal naming the row.
compatibility: Requires the openspec CLI, uv, git with worktree support, a local develop branch, and subagents.
metadata:
  author: eve-industry
  version: "1.0"
---

# Roadmap: next

One session, one roadmap row, from where it stands to merged on `develop`.

[roadmap.yaml](../../../roadmap.yaml) cuts the backlog into rows a single
session can carry, and states what one row is: **one OpenSpec change, one goal,
one worktree, one merge.** This procedure does not widen that. What it removes is
the reason a session has to stop and ask — the row comes from the dependency
graph, the contract is read before code is written, and anything that cannot be
decided here becomes a question file rather than a prompt in the terminal.

Work the steps in order, and where a step says to think, think. Every row is
different; this is a procedure, not a script.

**Decide for yourself.** Naming, module layout, test shape, which helper goes
where — choose, and note the choice in the commit body. Step 8 lists the small
set of things that are genuinely not yours to settle.

---

## How this session is shaped

You are the orchestrator, not the builder. Hold three things and delegate the
rest:

- the brief from step 4, and the index of it you kept,
- `tasks.md`, and every tick in it,
- every commit, and the merge.

Everything else — reading a file at length, running a command with long output,
writing implementation code, hunting a bug — goes to a subagent.

The reason is not tidiness. A session running this skill runs on the largest
model at the highest effort, and every source file that lands in this context is
paid for again on every turn that follows it. A subagent's context is thrown
away when it returns; yours is not. That asymmetry is the whole argument.

In practice:

- Never read a source file you are not about to quote. Ask an agent for the
  lines.
- Never run a command whose output you do not need line by line. `check-runner`
  returns the verdict and swallows the rest.
- Never write implementation code yourself. Step 7 says who does.
- Never re-type into a prompt what the agent already gets. `CLAUDE.md` reaches
  every subagent in this repository on its own, and the brief from step 4 is a
  file: hand over the path and the sections that bind, not the contents.
- Do keep every decision. A subagent reports; you judge, tick and commit.

**The roles are definitions, not prompts.** `row-scout`, `row-builder`,
`row-reviewer`, `row-fixer` and `check-runner` each carry their own
instructions, model, effort and turn budget in `.claude/agents/`. Dispatch one
by name with the task and the paths; the standing instructions cost you nothing
because you never hold them. Where a role's default model is wrong for one
dispatch, override it at the call and say why — that override is the escalation
ladder, and there is no other.

---

## Two trees, and which one is whose

A row runs in a worktree of its own. The checkout at the repository root stays
on `develop` and belongs to the person.

- **The row's worktree** — `.worktrees/<id>`, on branch `feature/<id>`, inside
  the repository and gitignored. Everything this session builds happens here,
  from step 3 until step 10, and every command names it:
  `git -C .worktrees/<id>`, `uv run --project .worktrees/<id>`. The shell's
  working directory resets between calls, so a single `cd` does not hold.
- **The root checkout** — the repository root, on `develop`. A person has it
  open in an editor while this session runs. A command with no `-C` runs here,
  which is exactly why every command that means the row has to say so. Reach
  into it deliberately for three things: reading `docs/questions/`, writing a
  question there (step 8), and the merge (step 10).

The split is not tidiness either. Switching branches in one checkout rewrites
the files under an open editor, and a person answering a question mid-row then
types into the same tree a builder is writing to: their half-finished answer
lands in the next commit, and `git status --short` — which step 7 treats as the
truth — stops being about this row at all.

Two rules follow, and they hold everywhere below.

- **Never `git add -A`, never `git commit -a`.** Every commit names its paths.
- **`docs/questions/` is the person's, and it lives on `develop`.** This session
  writes a question there, reads `status:` after that, and edits nothing it did
  not write itself.

---

## 1. Preflight and the row

Read `git worktree list` and `git branch --show-current` first, because
everything after this depends on where you are standing.

- A worktree for a `feature/<id>` already exists: an earlier session left that
  row half-finished. Work in it and pick that row up — go to step 7 — rather
  than starting a second one.
- The root checkout is on `main`: stop and say so. Nothing is ever committed to
  the default branch.
- The root checkout is on `develop`: the normal start, dirty or not. A person
  works in that tree. Its uncommitted state is theirs, and is not this session's
  to commit, stash or clean.

Then read [docs/questions/](../../../docs/questions/README.md) on `develop`.
Every file with `status: answered` is work waiting for you. An answer that is
documents-only and finished is committed on `develop` on its own, and the
question flipped to `status: resolved` in the same commit. An answer that is
code belongs to the row that asked it: apply it there, and flip the question on
`develop` in a separate commit once the commit that applies it exists. A file
with `status: open` that blocks a row means that row is not available.

**Then take the row, from the first of these that gives one:**

1. **What the caller named** — an id passed to this skill, or the `<id>` in the
   session goal the person already set (step 2 gives its shape). If its
   `depends_on` are not all `done`, say which one is missing and stop; starting
   it anyway produces a change that cannot be validated against specs that do
   not exist yet.
2. **An active OpenSpec change.** `openspec list --json`. A change that is not
   archived **is** the row — continue it rather than starting a second one.
3. **`roadmap.yaml`.** The lowest-`priority` row whose `depends_on` are all
   satisfied. A bare id refers to this file; `<repo>:<id>` refers to
   `../<repo>/roadmap.yaml` in the sister checkout, and a dependency there is
   satisfied when that row reads `status: done`. Where the sister checkout is
   absent, say so and treat the dependency as unsatisfied rather than guessing.

Set the row to `status: doing` in `roadmap.yaml` and commit that on `develop`
before the worktree exists, so a second session can see the row is taken.
Announce one line — `Row: <id> — <title>` — and carry on.

## 2. Set the session goal

A row is finished when it is merged, not when the code looks right, and the
session goal is what holds a session to that. It is one fixed sentence with one
slot, and the slot is the row:

```text
/goal Row <id> is archived under openspec/changes/archive/, its worktree is removed and its feature branch merged into develop with --no-ff and deleted, its roadmap.yaml row reads status: done, and ruff and pytest are green - or a blocking question is parked on develop in docs/questions/ with the row's tree committed
```

Everything but `<id>` reads the same for every row. That is what makes it a
pattern rather than a sentence somebody composes each time: the definition of
done for this repository does not vary, only which row it is being applied to.

**The person may set it before this skill runs**, and usually does — that is how
a row is chosen. Step 1 then reads `<id>` straight out of the goal rather than
picking from `roadmap.yaml`. Where they set the goal without an id (`Row the
next ready row is archived under…`), step 1 picks and you set the goal again
with the id filled in, so the condition names something checkable.

Set it yourself, in this shape, whenever it is not already set. Clear it early
only when the row parks; the escape hatch sits inside the condition so the goal
never argues a row out of parking.

## 3. Worktree

The row gets a worktree of its own, and the root checkout never leaves `develop`:

```bash
git worktree add .worktrees/<id> -b feature/<id> develop
uv sync --project .worktrees/<id>
```

Inside the repository and gitignored, which is what makes the rules in
`.claude/settings.json` reach it — deny rules, allow rules and the working
directory are all anchored to the project root, and a worktree beside the
repository sits outside every one of them. In exchange `.gitignore` carries one
`.worktrees/` line, so a check in step 9 does not walk one row's worktree while
verifying another.

`uv sync` because a fresh worktree has no `.venv`. `.env` is gitignored and does
not come along; copy it from the root checkout if the row needs one.

One row, one worktree, one branch: a row committed straight onto `develop` has
no boundary in `git log` and cannot be un-picked, and a row built in the root
checkout has no boundary against the person working in it.

## 4. Read the contract through a scout

Dispatch `row-scout` with the row's id, its goal, its areas and the absolute
path of the worktree. It answers five questions — which architecture invariants
and ADRs the row touches, what the specs already require of this capability,
which module and dependency boundaries the areas sit inside, what CLI and schema
surface the row must not break, and which existing files show the shapes it can
mirror — and writes the answers to `<worktree>/tmp/brief.md`.

Every bullet it writes opens with `[gen]`: true when written, and a hint by the
time a builder acts on it, because the row moves the tree underneath it. Step 7
stamps `[ver <role>]` onto the ones an agent has since re-opened the source for.

**Keep the index, not the brief.** The brief is a file, and a file read by
whoever needs it costs one read; the same text held here is re-read on every
turn that follows. Open a section yourself only when you are about to quote it,
or to settle a conflict the scout flagged. A conflict is a finding rather than a
decision: it becomes a question in step 8.

The brief is the only contract every later agent gets. Builders and reviewers
are handed its path and the sections that bind them — never a copy. It lives in
the worktree and goes with it at step 10, which is right: a brief that outlived
its row is a stale assumption looking for somewhere to be believed.

## 5. Propose

Use the `openspec-propose` skill for `<id>`. The proposal is written against the
brief from step 4 and names the ADRs the row carries and the invariants it must
not break. `openspec/config.yaml` carries the repository context and the rules a
proposal here has to satisfy; it reaches the skill on its own.

The scope ceiling is the roadmap's own: one spec capability, one area, a task
list that fits a session. A row that turns out to need two areas, a second
schema change, or a second capability is two rows — say so and stop. Widening
the row here is what produces the structural drift this repository split exists
to avoid.

Do not implement in this step.

## 6. Review the proposal, before there is code

Dispatch `row-reviewer` with the proposal, the spec delta, `tasks.md` and the
path to the brief. It knows the questions to ask and the verdicts to return; you
supply the paths.

Its default model rather than this session's: the question is structural — does
this break an invariant, contradict an ADR, or exceed one capability, one area
and one session — and structural is what a cheaper model is reliably good at. It
is the cheapest review in the procedure and it catches the most expensive
mistake: a proposal that quietly breaks an invariant costs one rewrite now, and
a spec delta, an archive and a merge after apply.

- `proceed`: go on.
- `narrow`: rewrite the proposal smaller. Never widen it to match the code you
  were already imagining.
- `park`: step 8.

## 7. Apply, as orchestrator

Use the `openspec-apply-change` skill to read `tasks.md`, then hand the work
out. Any bundle that adds or changes a Dagster definition consults the
`dagster-expert` skill first; it tracks the installed Dagster version, and a
definition written from memory is written against the wrong one.

**Bundle the tasks.** A bundle is the set of tasks that touch one file set;
`tasks.md` is already grouped, and the row's `areas` name the same partition.
Bundles with disjoint file sets run at the same time; bundles that share a file
run one after another, and the later one is handed what the earlier one decided.
Most rows here are one area and therefore one bundle. Do not manufacture
parallelism a row does not have — two agents in one file cost more than they
save.

**Brief each builder.** One `row-builder` per bundle, and the brief is four
lines, not four paragraphs:

- the row's goal and the tasks in this bundle, verbatim;
- the absolute path of the row's worktree, and the paths this bundle owns inside
  it — one sentence saying every other path belongs to another agent. A relative
  path resolved against the root checkout edits the person's tree instead of the
  row's;
- the absolute path of the brief, and which of its five sections bind this
  bundle;
- what the earlier bundle decided, where this one follows it in the same files.

Nothing else. `row-builder` already carries the boundaries, the conventions and
the shape of its report, and `CLAUDE.md` reaches it on its own; restating either
costs context on every turn until the row merges and buys a builder nothing.

Set `effort: high` at the call for a bundle that is genuinely hard — a
concurrency question, a schema whose shape is still moving — and say in your
report that you did. The default is medium because a bundle with a task list and
a brief is scoped work.

**Then you tick and commit.** Read what came back, confirm it against
`git status --short` and `git diff --stat` rather than the report, tick the
tasks that are actually done, and commit. Small and often: a ticked task and a
commit are the only progress signals this row has. When a report and the tree
disagree, the tree is right, and the difference is worth a sentence in your own
report. Commit by pathspec, `git commit -- <paths>`, because a commit that
sweeps up the whole tree is how a person's unfinished work ends up inside a row.

**Stamp the brief in the same breath.** A builder that reports `verified:` has
re-opened the source for those bullets; change their `[gen]` to
`[ver row-builder]` in `tmp/brief.md`. Builders do not do this themselves for
the reason they do not tick tasks — two bundles may run at once, and the second
write wins silently.

**Check for an answer between bundles.** Before each commit, read
`docs/questions/` in the root checkout again. An answer can arrive at any
moment: the person writing it is sitting in that tree while this session runs,
and reading the directory only in step 1 makes an answer written mid-row
worthless until the session after this one. A question this row parked that now
reads `status: answered` un-parks the row; a question belonging to another row
is left entirely alone.

**A bundle that comes back stuck** does not get a second attempt at the same
thing. Read what it tried, and either hand it to one more agent with a different
approach, or park a question.

**A builder reporting `upstream:`** has hit the thin-orchestration boundary and
stopped where it should: the row turned out to need a corpus subcommand, a JSON
shape or a Gold tree that does not exist. That is a question — step 8 — and its
answer is a row in `eve-industry-corpus`, never logic moved into Python here.

## 8. What parks a row

Questions never go to the terminal, and they do not go on the row's branch
either. Write `docs/questions/<yyyy-mm-dd>-<slug>.md` in the root checkout, on
`develop`, in the shape described in
[docs/questions/README.md](../../../docs/questions/README.md), and commit it
there on its own.

On `develop` because a parked row's branch may never merge — a question
committed there is one nobody can find — and because a person answers by hand
while this session runs, in exactly the tree the worktree exists to keep
builders out of.

Blocking, so park:

- a contract or data-model choice the spec leaves open;
- a new external service or dependency;
- a finding that would change an architecture invariant, a recorded decision,
  or the storage boundary;
- a row that needs a corpus CLI subcommand or a Gold shape that does not exist.

Not a question — choose, and note it in the commit body:

- naming, module layout, test shape;
- two implementations with no consequence outside this row.

Parking means: commit what is finished in the worktree, write the question on
`develop`, report, stop. Leave the worktree standing — it is where the next
session resumes, and step 1 finds it there. Parking never means quietly
narrowing the row to something that fits.

## 9. Verify and review

**Checks first**, through the `check-runner` agent, in the worktree:

```bash
uv run --project .worktrees/<id> ruff check .
uv run --project .worktrees/<id> ruff format --check .
uv run --project .worktrees/<id> pytest -q
```

The tests run against the fake `corpus` binary in `tests/fake_corpus.py`, so
they need neither a Rust build nor the NAS. It returns the verdict with the
failing lines, so the raw output never enters this session. Report which of the
three ran. A skipped suite is not a pass, and
neither is a pass you did not actually get.

**Then review, from more than one position.** Run these together; they see
different things:

1. `/code-review`, in this session. It shares the context that produced the code,
   which makes it good at "this does not do what you meant". Pick the level from
   the row rather than from habit: `high` where the row changed behaviour that
   already existed or touches an architecture invariant or the storage
   boundary, `medium` where it only added something new behind its own tests.
2. `row-reviewer`, given the diff against `develop`, the path to the brief and
   the row's goal — and nothing else. A review that shares the assumptions which
   produced a bug cannot see that bug. This one does not share them, and that is
   its entire value, so do not repair it by handing over the history.
3. By area, when the row is in one. A row that changes a sensor, a pool or a
   partition definition earns a scheduling position: brief `row-reviewer` for it
   with the diff, the brief, and what fires when — a sensor that queues doomed
   runs and one that never fires look identical in a test.
   `/security-review` opens on `origin/HEAD` and this procedure never pushes, so
   it would review every unpushed row rather than this one.

**Never take a turn to say you are waiting.** Position 1 runs *in* this session,
so it is your turn rather than a report that arrives. Position 2 is dispatched,
and the harness wakes you when it lands — polling it does nothing except pay for
this context again. While a dispatched review is out, do the part of step 10
that does not depend on it: validate, sync the specs. A position that has not
reported once you have run out of independent work is **not run**: name it in
the report and go on.

**Then fix.** One `row-fixer` per finding, given the finding, the file and the
line; findings in one file go to one agent, independent findings go in parallel.
Send `check-runner` back in for whichever checks the fix could have broken.

`row-fixer` is on Sonnet, because a finding that already names its file and line
is bounded work. Override the model at the call, and record the reason in your
report, for exactly these: a correctness or concurrency question, a finding two
reviewers disagree about, or a second attempt at something a first fix did not
hold. Nothing else earns it — a row being large does not, and a turn budget
running out never does.

Two fix rounds is the ceiling. Still red after the second: stop, leave the tree
committed, and say what is red and what you tried. A third round is where a
session starts inventing.

A finding that would change an invariant, an ADR or applied DDL is not a fix. It
is step 8.

## 10. Adopt, archive, merge

1. `openspec validate <id> --strict`, then the `openspec-sync-specs` skill, then
   `openspec archive <id> --yes`. The archive promotes the delta into
   `openspec/specs/`, which is what the next row's dependency check reads.
2. `roadmap.yaml`: set the row to `status: done`.
3. `README.md`, `ROADMAP.md` and the **State of the repository** paragraph in
   `openspec/config.yaml`, only where the row changed what they claim — a work
   item in `ROADMAP.md` moving to done is exactly such a change. This
   repository keeps no `CHANGELOG.md`.
4. One commit for the adoption, in the shape `git log` already uses.
5. Merge, and do not push.

```bash
git -C .worktrees/<id> status --porcelain   # empty, or stop and commit
git worktree remove .worktrees/<id>         # deregisters; may leave .venv
rm -rf .worktrees/<id>                      # finish the job
git merge --no-ff feature/<id>
git branch -d feature/<id>
```

`git worktree remove` refuses a tree with modified tracked files — that refusal
is the check that nothing was left uncommitted — but may leave the directory on
disk once it meets `.venv`, so the `rm -rf` finishes the job. Never `--force`;
it drops the check and does not fix the leftover.

The root checkout is already on `develop` and stays there, so nothing switches
branches under the person's editor. If the merge refuses because a file it
touches is modified in that tree, stop and say which file. That is their work,
not yours to stash.

`--no-ff` because the row is the unit. A fast-forward erases the only boundary
between two rows in `git log`, and that boundary is what makes a bad row
revertable. `rtk git log` hides merge commits; verify the merge with
`rtk proxy git log --graph --oneline` if you verify it at all.

Commit shape: an imperative subject of at most 72 characters, no trailing
period, no area prefix. A body only where the why is not obvious — two lines,
wrapped at 72 — naming the row (`Implements <id>`) and any question file it
resolves. Close with `Co-Authored-By: Claude <noreply@anthropic.com>`.

## 11. Report, then stop

Five lines: the row, what the four checks returned, what the three review
positions found and what became of it, what was parked, and every model override
you made with the reason for it.
