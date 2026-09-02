# Questions

## Is this a file at all?

One test: **would this question still have to exist if this session were killed
right now?**

**No.** It blocks a process, not the work. Ask it with the harness's own
question tool — the answer arrives, the session resumes, and nothing is written
down. *Which of these two implementations did you mean? May I add this
dependency? Is this the file you meant?*

**Yes.** It blocks a roadmap row. It outlives the terminal it was noticed in,
and something other than this session has to be able to find it. That is a file
here.

The two failure modes are each other's mirror, and both are silent. A
session-blocking question parked in a file rots, because no row is waiting for
it and nobody is looking. A row-blocking question asked in a chat prompt is
gone the moment the terminal closes, because the surface that showed it *was*
the process.

What a measurement can answer is neither: measure it.

## Two folders, and that is the whole state

```
questions/
  open/       waiting on the person   -> the person's inbox
  answered/   waiting on a session    -> the agent's inbox
```

`ls` answers "is this handled" without opening a file, and no frontmatter key
can drift from it. **There is no `resolved/`.** Applying an answer deletes the
file, because the record is then the ADR, the code or the test that enforces
it — see *Applying an answer* below.

A session that hits a question parks a file in `open/`, commits it on
`develop`, and stops or carries on with the rest of the row. The person answers
by editing the file and moving it:

```bash
git mv docs/questions/open/<file> docs/questions/answered/
```

This directory lives on `develop`, never on a row's branch: a parked row's
branch may never merge, and a question committed there is one nobody can find.

Across all six repositories, `uv run --with pyyaml python
.agents/skills/next/next.py` from `C:\Projecten\eve` lists both folders, joins
each file to its roadmap row, and reports the two states a question must never
rest in: open on a row that already shipped, and answered on a row that is
done.

## What earns a question

Exactly four cases:

- a partition matrix, a start date or what an asset records would move under
  materialisations that already exist — the Dagster instance keys its history
  on the partition keys, and the serving seam reads what a load records — or
  the corpus dataset YAML leaves it open;
- a new dependency: a corpus CLI subcommand, a `--format json` shape or a Gold
  tree that does not exist — its answer is a row in `eve-industry-corpus`,
  never logic moved into Python here — or a new service or resource to shell
  out to;
- a finding that would change an architecture invariant, a recorded decision,
  or the storage boundary;
- a design fork whose consequences reach outside the row.

Not a question:

- **anything a measurement answers** — what a sensor tick proposes, what a
  materialisation records, what the run-state holds for a day, what a pool
  holder peaks at. Preview the tick, materialise the partition into the
  scratch sink, run `corpus state query`, read the `_INDEX.json`, measure with
  `/usr/bin/time -v`, and write the number down where the decision is made;
- **a "not yet" finding** — a limit that only bites after an operator action
  that has not happened. It goes to `notes:` on the roadmap row it belongs to,
  or to a *Known limits* paragraph in the ADR;
- **something noticed while reviewing another row** — the same: that row's
  `notes:`;
- naming inside a module, asset grouping, test shape, two implementations with
  no consequence outside the row. Decide, and note the choice in the commit
  body.

## The shape

One file per question, `<yyyy-mm-dd>-<slug>.md`, and it fits one screen. The
evidence behind it is a number and a path, not a table; the options are two or
three, one line each; the recommendation is one paragraph. A question that
needs more than a screen is a design that has not found its ADR yet.

```markdown
---
row: gold-asset-wiring
---

# Does the Gold asset wait on the coverage pre-check or queue and let the binary decide?

## Why this is blocked

<What was being built, what forced the choice, why the session could not
settle it, and the one measurement that bears on it. One paragraph.>

## The options

- **Pre-check.** <what it costs, what it buys, what it forecloses — one line>
- **Queue anyway.** <same>

## What I would do

<A recommendation, with its reason. A question without one makes the person do
the thinking twice.>

## Answer

<Empty. The person writes here, then `git mv`s the file to `answered/`.>
```

The frontmatter is one key, and it is the coupling to the work. `next.py`
validates it: an id that no roadmap carries is a reported problem, not a
comment, and prose in the field (`none — found while doing X`) is a problem
too — the reason belongs in *Why this is blocked*.

## Applying an answer

An answered question is work, and the session that reads it does that work.

**`row:` names a row.** The answer is input to that row: the proposal cites it
and the row cannot be proposed against a stale reading of it.

**`row: none`.** The answer has no home yet, and applying it starts with giving
it one — a new `roadmap.yaml` row when it needs one, the `fix` skill when it
does not. Set `row:` to the id in the same commit that creates the row. An
answered question with no row is the one shape that rots, because nothing on
the roadmap is waiting for it.

**Then delete the file.** A documents-only answer is applied and deleted in one
commit on `develop`; an answer that is code is applied in the row's worktree
and the file deleted on `develop` once the applying commit exists. The deleting
commit names in its body where the answer landed.

An answered question does not linger. An answer that is a decision becomes an
ADR under `../adr/` — rewritten in place, never stacked with a successor — and
the question file is deleted in that commit; the ADR is the record, and keeping
both means two places to read and one to forget. `docs/adr/` was started by
ADR-0001; a decision that outgrows a paragraph in `ROADMAP.md` joins it there.
An answer that changed nothing structural is deleted outright once it is
applied. The directory should be short: a pile of resolved questions is a
second, worse ADR log.
