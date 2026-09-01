# Questions

A question for the person, written down instead of asked in a terminal. A
session that hits something it cannot settle parks a file here, commits it on
`develop`, and stops or carries on with the rest of the row. The person answers
by editing the file.

This directory lives on `develop`, never on a row's branch: a parked row's
branch may never merge, and a question committed there is one nobody can find.

## The shape

One file per question, `<yyyy-mm-dd>-<slug>.md`:

```markdown
---
status: open
row: gold-asset-wiring
---

# Does the Gold asset wait on the coverage pre-check or queue and let the binary decide?

## Why this is blocked

<What was being built, what forced the choice, and why the session could not
settle it. One paragraph.>

## The options

- **Pre-check.** <what it costs, what it buys, what it forecloses>
- **Queue anyway.** <same>

## What I would do

<A recommendation, with its reason. A question without one makes the person do
the thinking twice.>

## Answer

<Empty. The person writes here and sets `status: answered`.>
```

`status` moves `open` -> `answered` -> `resolved`. The person sets `answered`;
the session that acts on the answer sets `resolved`, in a commit on `develop`
made once the commit that applies the answer already exists.

## What earns a question

Blocking, so park it:

- a decision about what an asset shells out to, or what it records;
- a partition matrix or a start date the corpus dataset YAML leaves open;
- a finding that would change an architecture invariant, a recorded decision, or
  the storage boundary;
- a row that needs a corpus CLI subcommand or a `--format json` shape that does
  not exist.

Not a question — decide, and note the choice in the commit body:

- naming inside a module, asset grouping, test shape;
- two implementations with no consequence outside the row that picks one.

## What happens to a resolved question

It does not linger. An answer that is a decision becomes an ADR in
`../adr/` and the question file is deleted in the same commit; the ADR is the
record, and keeping both means two places to read and one to forget. `docs/adr/`
was started by ADR-0001; a decision that outgrows a paragraph in `ROADMAP.md`
joins it there. An answer that changed nothing structural is deleted outright
once it is applied.

The directory should be short. A pile of resolved questions is a second,
worse ADR log.
