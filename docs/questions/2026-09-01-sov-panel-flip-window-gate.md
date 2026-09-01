---
status: open
row: sovereignty-gold-panel
---

# Should `sov-panel` readiness gate on `sovereignty-changes`, so an early panel day cannot seal NULL flip counts?

## Why this is blocked

Row `sovereignty-gold-panel` landed the five sovereignty Gold assets, and the
panel declares all four sibling trees plus the SDE snapshot in `deps=`, which is
what corpus ADR-0066 decision 8 asks for. But `deps=` is lineage in this
repository — every asset is launched by its own single-target sensor, and there
is no `AutomationCondition` anywhere in `src/`. What actually sequences the panel
at runtime is corpus's own readiness gate, and that gate lists only the three
same-day trees: `sovereignty-ownership`, `sovereignty-adm` and
`sovereignty-contests`. `sovereignty-changes` is not among them, and neither is
the trailing 30-day flip window it feeds.

So `sovereignty_panel_gold_sensor` will request date D as soon as those three are
in run-state, whatever the changes tree has reached. Corpus then builds D with an
incomplete flip window, publishes `constellation_flips_30d` and
`region_flips_30d` as NULL, and warns on stderr — which is correct per the row's
own two-gate rule. The problem is what follows: `gold ready-dates` excludes dates
whose Gold is already built, so that day is never revisited. The NULLs are
permanent. An operator who enables four of the five sensors and not the changes
one, or whose changes runs fail for a stretch, silently seals a run of days with
flip counts that will never be filled in.

This cannot be fixed in orchestration. A Python pre-check on the changes tree
before requesting a panel date is exactly the pre-validation the
thin-orchestration invariant forbids, and it would also duplicate a decision the
binary already owns. The fix belongs in corpus, so this row shipped the assets
as specified and parked the finding here.

## The options

- **Add `sovereignty-changes` as a fourth readiness gate in corpus.** `sov-panel`
  becomes ready only when the trailing flip window is also complete, so a panel
  day is never built with NULL flip counts. Costs the panel its independence from
  the changes tree — a changes day that is permanently absent would stall the
  panel indefinitely rather than degrading it — which is precisely the
  distinction ADR-0065 draws, so it likely needs the absent-vs-late split too.
- **Make a NULL-flip-count panel day rebuildable.** Leave readiness alone and let
  `ready-dates` re-offer a built panel day whose flip window has since filled in,
  the way `sde_gold_sensor` already re-offers a changelog partition whose
  predecessor changed underneath it (ADR-0001). Keeps the panel degrading rather
  than stalling, at the cost of mutable panel partitions and a freshness question
  corpus would have to answer per day.
- **Accept it and document the operating rule.** The five sensors are enabled
  together or not at all, and a changes outage is an operational incident. Costs
  nothing to build and puts a silent, unrecoverable data defect behind a
  convention.

## What the data says

Measured 2026-09-01 against the live run-state (`<nas>/state/corpus-state.db`,
read from a copy) and against `data.everef.net` for archive availability. Every
number here is verified rather than inferred.

**The family does not exist yet.** No `sovereignty-*` row in the run-state
`partitions` table and no `sov*` tree under `silver/` or `gold/`. The set of
already-degraded panel partitions this question is about is therefore empty, and
no option owes a migration.

**The permanent gaps are two clusters, not twelve scattered days.**
`system-jumps`, `system-kills` and `industry-cost-indices` each record exactly
twelve `skipped_partitions` on identical dates; all three sovereignty archives
miss the same twelve, and the tar era below the folder boundary is complete:

| Era | Range | Result |
|---|---|---|
| tar (`sovereignty-map-2021.tar.bz2`) | 2021-07-01 .. 12-31 | 184 / 184 days |
| tar (`sovereignty-map-2022.tar.bz2`) | 2022-01-01 .. 12-15 | 349 / 349 days |
| folder `index.json` | 2022-12-16 .. 2026-09-01 | 12 days absent |

The twelve are `2023-01-27..01-31` (5 days) and `2023-11-21..11-27` (7 days) —
one EVE Ref-wide outage, identical across all six datasets checked.

Clustering decides two things. It makes the binary all-or-nothing rule right: a
5- or 7-day hole in a 30-day window is a 17–23 % undercount, which is the
"silently short window" ADR-0066 refused — scattered single days would have been
~3 % and would have argued for a coverage-weighted count instead. And it caps
the damage: a contiguous outage of any length poisons exactly 30 panel days
(`L+29` affected dates minus the `L-1` gap days inside them), so two outages
cost 60 days rather than 12 × 30.

**That count is currently zero, because a larger gate swallows it.**
`sov-ownership` and `sov-events` gate a 180-day tenure window at
`coverage_min_ratio: 1.0`, and `window_coverage` sizes the denominator on the
calendar, so a permanently-skipped day counts against the ratio forever. Each
cluster blocks both trees for 180 consecutive days — `2023-02-01..07-30` and
`2023-11-28..2024-05-25`, 360 of 1 692 candidate days (21.3 %). The panel reads
the same-day ownership partition and inherits that blackout, and both 30-day
flip-count blocks fall entirely inside it. Buildable-but-NULL panel days in the
served range: **0**. That is a separate and larger question, parked as
[2026-09-01-sov-tenure-window-permanent-gap.md](2026-09-01-sov-tenure-window-permanent-gap.md).

## What I would do

The **first** option — and the measurement is what changed that, because this
recommendation was the second option until the numbers came in.

Three things decided it.

*Option two's own precedent fails its own test.* ADR-0060 separates gates from
drift-detection with one question: is there a settled state to gate for?
Killmails have none — a day legitimately grows forever. The flip window has one:
`[D-30, D)` is finite, closed left, and every day inside it is eventually built
or permanently absent. It settles, so it is a gate problem. Under option two the
panel would mutate not because its inputs changed but because it was built too
early, which is a scheduling defect dressed up as a data contract.

*The cost is not where the option list puts it.* The expensive half — scanning
whether a sibling tree's window is sealed — is identical under both options.
Option one spends it on the gate. Option two spends it on after-the-fact
detection and then also pays for mutable partitions, a sensor term, an
orchestration row, and a `parquet_sha256` that moves under the one tree where
hash stability is load-bearing: predict logs it to MLflow as reproducibility
evidence for a training set.

*And there is nothing to repair.* The family has never been materialised, so
option two's entire value is a repair mechanism for a condition that has not
occurred — while the next thing this family does is a ~1 700-day backfill, which
option one self-orders and option two would run twice, against the single-HDD
NAS the concurrency caps exist for.

**The shape.** A *window* prerequisite alongside the same-day one, not a
generalisation of it: `same_day_gold_prerequisites` also feeds
`permanent_gap_datasets` and the ADR-0065 skip, and a permanent gap inside the
*window* must not skip the day. Panel D is ready when the three same-day
partitions are sealed **and** every `d ∈ [D-30, D)` is either built in
`sovereignty-changes` Gold or a recorded gap on `sovereignty-map` Silver. That
second disjunct is not optional — without it the 60 days above stall forever.
`PrerequisiteState` already carries exactly those two sets per prerequisite, so
the planner change is small. The builder does not change at all; its per-column
NULL rule then fires only where the flip counts are genuinely unknowable, which
is what `flip_window_complete` should have meant all along.

**Orchestration owes nothing.** No follow-up row, no repair sensor, no
mutable-Gold record — only the deletion of the docstring paragraph in
`_build_sovereignty_gold_sensor` that points here. And `deps=` and the runtime
gate would finally agree, which is what ADR-0066 asked for.

**Priority: this is the second job.** In the served range the degradation this
question describes cannot occur, because the tenure blackout hides all 60 days.
Settle the 180-day gate first.

## Answer

<!-- The person writes here and sets `status: answered`. -->
