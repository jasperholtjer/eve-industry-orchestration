# Questions

A question for the person, written down instead of asked in a terminal, and
only for a fundamental problem. A session that hits one parks a file here,
commits it on `develop`, and stops or carries on with the rest of the row. The
person answers by editing the file.

This directory lives on `develop`, never on a row's branch: a parked row's
branch may never merge, and a question committed there is one nobody can find.

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
status: open
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

<Empty. The person writes here and sets `status: answered`.>
```

`status` moves `open` -> `answered` -> `resolved`. The person sets `answered`.

## After the answer

The session that reads the answer applies it. An answer that is documents-only
is applied and resolved in one commit on `develop`. An answer that is code is
applied in the row's worktree, and the question flipped to `resolved` on
`develop` once the applying commit exists.

A resolved question does not linger. An answer that is a decision becomes an
ADR under `../adr/` — rewritten in place, never stacked with a successor — and
the question file is deleted in that commit; the ADR is the record, and keeping
both means two places to read and one to forget. `docs/adr/` was started by
ADR-0001; a decision that outgrows a paragraph in `ROADMAP.md` joins it there.
An answer that changed nothing structural is deleted outright once it is
applied. The directory should be short: a pile of resolved questions is a
second, worse ADR log.
