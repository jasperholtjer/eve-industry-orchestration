---
name: fix
description: Make one bounded change that is not a roadmap row - a defect, a guard, a rename inside a module, a doc correction - in a fix/<slug> worktree, verify it narrowly, and merge it --no-ff into develop. Use this whenever the user asks for a small change, a bug fix, a quick correction or a cleanup, or when roadmap-next's intake decides the work is not a row. Not for anything that moves what fires when (a partition definition, a start date, a sensor's trigger condition, a schedule's cadence), what an asset records, the memory budget or a recorded decision, wires a dataset or adds something to shell out to, or needs a design choice - those are rows and belong to roadmap-next.
compatibility: Requires git with worktree support, uv, and a local develop branch.
metadata:
  author: eve-industry
  version: "1.0"
---

# Fix

The second entrance. One bounded change, ten to fifteen minutes, no roadmap
row, no OpenSpec change, no scout, no question.

## Is it a fix?

Three questions. One yes makes it a row, and the `roadmap-next` skill:

1. Does it move what fires when — a partition definition or a start date, a
   sensor's trigger condition, a schedule's cadence — what an asset records,
   the memory budget in `deploy/dagster.yaml`, or a recorded decision (an ADR,
   `docs/serving-seam.md`, a `ROADMAP.md` decision)?
2. Does it wire a dataset or a Gold derivative that has no asset yet, or add a
   resource or something new to shell out to?
3. Does it need a design choice with options?

A fix that turns into a yes while you work: stop, add the row to
`roadmap.yaml` on `develop`, and leave the tree for `roadmap-next` under the
row's name:

```bash
git -C .worktrees/<slug> branch -m fix/<slug> feature/<id>
git worktree move .worktrees/<slug> .worktrees/<id>
```

## Procedure

1. **Preflight.** Root checkout on `develop`, `git worktree list`. Then:

   ```bash
   git worktree add .worktrees/<slug> -b fix/<slug> develop
   uv sync --project .worktrees/<slug>
   ```

   A fresh worktree has no `.venv`; `uv sync` is seconds. `.env` does not
   come along, and this session may not read it.

2. **Change.** This session is small, so read the lines you need and make the
   change yourself; dispatch one `row-builder` only when it spans more than
   one file set. A change to a Dagster definition consults the
   `dagster-expert` skill first, fix or not. Add or adjust the test that
   proves it — against the fake binary in `tests/fake_corpus.py` — because a
   fix with a test that failed before and passes after is the only kind that
   needs no review.

3. **Verify narrowly**, for the modules you touched:

   ```bash
   uv run --project .worktrees/<slug> ruff check --fix .
   uv run --project .worktrees/<slug> ruff format .
   uv run --project .worktrees/<slug> pytest -q tests/test_<module>.py
   ```

   The three checks through `check-runner` only when the fix touches shared
   machinery (`defs/config.py`, `defs/corpus_resource.py`, `defs/resources.py`,
   `defs/sensor_util.py`, `tests/fake_corpus.py`, `tests/conftest.py`) or more
   than one module. A fix that changes what an asset shells out to or records,
   or what a sensor proposes, also does the real run from `row-builder` once,
   into `C:\tmp\orchestration-scratch\<slug>`, and the report says what it
   showed.

4. **Review** only when the fix touches an architecture invariant in
   `AGENTS.md`, the storage boundary, a pool, or a run-state key helper: one
   `row-reviewer` with the diff. Otherwise none.

5. **Land.** Commit by pathspec; this repository keeps no `CHANGELOG.md`. From
   the root checkout `git merge --no-ff fix/<slug>`, delete the branch,
   `git worktree remove` and `rm -rf` the directory and the scratch root. Do
   not push.

6. **Report** in three lines: what changed, what was run, what was not.
