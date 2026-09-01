---
status: open
row: sde-gold-sensor-stall
---

# Does this repository adopt pyright, or is "no pyright here" still the decision?

## Why this is asked

The session goal for `sde-gold-sensor-stall` named pyright as one of the three
checks that must be green. This repository does not run it: `pyproject.toml` has
no pyright dependency and no `[tool.pyright]` section, `CLAUDE.md` and the
`roadmap-next` procedure both name ruff and pytest as the gate, and
`openspec/config.yaml` states it outright — "No pyright here." The two do not
agree, and the disagreement is about a recorded decision rather than about this
row, so the row was finished under the repository's own gate rather than
narrowed or parked.

For the record, measured on the four files the row touched
(`uvx pyright --pythonpath .venv/Scripts/python.exe <files>`):

| | `develop` | `feature/sde-gold-sensor-stall` |
|---|---|---|
| `defs/sensors.py` | 0 | 0 |
| `defs/sensor_util.py` | 0 | 0 |
| `tests/fake_corpus.py` | 0 | 0 |
| `tests/test_sde.py` | 42 | 127 |

Every error is the same one, and it is pre-existing: calling a
`@dg.sensor`-decorated object directly in a test returns
`SkipReason | RunRequest | ... | SensorResult | None`, so `result.run_requests`
is an error on the union. Nine new tests using the pattern the whole test suite
already uses raise the count; none of the production code the row changed adds
one. Making those four files clean means either an `isinstance` narrowing in
every sensor test in the repository, or a `[tool.pyright]` section that excludes
`tests/` — both repository-wide decisions, neither of them this row's to take.

## The options

- **Keep "no pyright here".** Costs nothing, and the ruff `E/F/I/S/UP/B` set plus
  the fake-binary tests is what the repo has always shipped against. The goal
  sentence in `roadmap-next` §2 keeps naming ruff and pytest only, and the
  session goal drops pyright. Leaves the untyped Dagster surface unchecked.
- **Adopt pyright in `basic` mode, `src/` only.** One roadmap row: add the dev
  dependency, a `[tool.pyright]` section with `include = ["src"]`, and fix
  whatever it finds in the twenty-odd asset modules. Tests stay out, which is
  where all 127 of these errors live. Adds a third gate to `roadmap-next` §9 and
  to the goal sentence.
- **Adopt pyright everywhere, including tests.** Also needs a typed helper that
  narrows a sensor call to `SensorResult`, applied across every `test_*.py`. The
  largest change, for the sensor-shaped bugs a narrowing would have caught —
  which, on the evidence of this row, ruff and the fake-binary tests caught
  anyway.

## What I would do

Adopt it in `basic` mode over `src/` only, as its own roadmap row, and leave
`tests/` out. The production modules are thin shims where a wrong type is a real
runtime failure on the LXC, and they are already clean — so the row is cheap to
land and cheap to keep. The 127 errors are all in test code, all one Dagster
decorator idiom, and narrowing them would add ceremony to every test in the repo
for no defect this row's evidence supports. If the answer is instead "keep no
pyright", the fix is to the goal sentence in `.claude/skills/roadmap-next`, so
the next row is not asked for a check the repository does not run.

## Answer

<!-- The person writes here and sets `status: answered`. -->
