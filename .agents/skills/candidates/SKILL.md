---
name: candidates
description: Survey what could be built in THIS repository - the open rows on its roadmap.yaml, the work items ROADMAP.md names and has not finished, the future phases it describes without ever making a row, and what the decisions chose not to build. Use this whenever the user asks what could we build here, what is left in orchestration, what are the options, what should go on the roadmap, or what did we defer - as opposed to which existing row to start now, which is `roadmap-next`. Repo-scoped by design - the platform-wide version of this question, over all six repos, is the `platform-candidates` skill one directory up. Reports and proposes; it never edits a roadmap and never starts the work.
compatibility: Runs from the repo root or a worktree under .worktrees/. Needs uv.
metadata:
  author: eve-industry
  version: "1.0"
---

# What could be built here

Two scopes, one question, and the boundary between them is the repository:

| Where | Skill | Reads |
| --- | --- | --- |
| `C:\Projecten\eve` | `platform-candidates` | six `roadmap.yaml`, six markdown roadmaps — the whole platform |
| here | `candidates` (this one) | `roadmap.yaml`, `ROADMAP.md`, `docs/adr/`, `docs/serving-seam.md`, `docs/decisions/not-taken/` — orchestration only |

Only the root's two skills carry the `platform-` prefix; the unprefixed name is
always the repo-level skill.

This one never leaves the repository. A candidate that turns out to belong to
`corpus`, `serving`, `predict`, `map` or `calc` is *named and handed over*, never
proposed as work here — that is the same boundary the data plane keeps.

## Run it

```bash
uv run --with pyyaml python .agents/skills/candidates/candidates.py
```

Five blocks, in the order you should weigh them:

- **OPEN ROWS** — `roadmap.yaml` minus what is done. Often empty; that is normal
  here and is not a reason to invent one.
- **WORK ITEMS** — the numbered items under `ROADMAP.md`'s *Work items* that
  carry no `— done`. Each was named as work and left; read the note under it,
  because several were finished without the marker moving and that is a
  documentation fix, not a row.
- **FUTURE PHASES** — what `ROADMAP.md` describes and never turned into a row.
  This is the catalogue: a line here becomes a row only when it is the next thing
  to build, so proposing one is proposing a row, and it displaces something.
- **DEFERRED IN THE DECISIONS** — what a record chose not to build, in the place
  the decision was made: `docs/adr/`, `docs/serving-seam.md`, and `ROADMAP.md`'s
  *Decisions* section. This is the bucket nobody collects. Open every line that
  looks live: the record says whether it was deferred (additive later, a genuine
  candidate) or refused (a decision, not a backlog item), and the two read alike
  in one grep line.
- **NOT TAKEN** — `docs/decisions/not-taken/`: the ideas that were considered here
  and declined, with what would change our mind. Printed last and read first: a
  candidate that is already in this block is not a candidate until you argue
  against the file by name, with new evidence.

## The gate

**The storage boundary, not the consumer.** Corpus's version of this skill asks
who would read the column; here the question is already answered — this repo has
one product, which is *what fires when*, and one consumer, which is the operator.
So the gate is the other boundary:

> Would this candidate put compute, parsing or validation in Python?

If yes it is not a candidate here at all. It is a **corpus row**: name the
subcommand, the `--format json` shape or the Gold tree that would have to exist,
say which repo owns it, and stop. A candidate that needs a corpus surface that
does not exist yet is reported as blocked on that corpus row, with the row named
— never as work to start here while waiting.

Two further rules from `AGENTS.md` bind what you may propose:

- A phase is in `ROADMAP.md`, not `roadmap.yaml`, until it is **the next thing to
  build**. Proposing one is proposing a row, so say what it displaces.
- Work that moves nothing of what fires when, nothing an asset records, no memory
  budget in `deploy/dagster.yaml` and no recorded decision, wires no dataset and
  needs no design choice is not a row at all. If a candidate turns out to be that,
  name it as a `fix`.

## Report it

Per candidate, one line: **what it gives, what it waits on, and which of the five
blocks it came from.** Then recommend two or three, with a reason each — usually
that it finishes something already paid for, or that a landed Gold tree has no
asset reading it. Order them; a survey without an order is a list.

End with the exact next step, and be precise about which entrance it is:

> Add `serving-load-sensor` to `roadmap.yaml` on `develop`, then
> `/roadmap-next serving-load-sensor`.
> The `corpus load` subcommand that a shape-transforming loader would need is a
> row in `eve-industry-corpus`, not here.

## What this skill must not do

- **Never edit `roadmap.yaml` or `ROADMAP.md`.** Adding a row is its own commit on
  `develop`, made deliberately, not as a side effect of a survey.
- **Never invent a row id** or a priority for something that is not a row yet.
- **Never re-propose a `NOT TAKEN` file** without new evidence, and never quietly:
  name the file and the argument you are arguing against.
- **Never widen into a sibling repo.** Name the row that repo would need, and stop.
- **Never start the work.** This skill reports; `roadmap-next` and `fix` build.
