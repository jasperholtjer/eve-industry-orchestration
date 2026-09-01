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

## What I would do

The second option. The panel is a derived convenience table and degrading it is
better than stalling it, which is the whole reason the row's two-gate rule exists
in the first place; what is wrong today is not that a short window yields NULLs
but that the NULLs are permanent. Rebuildability also has precedent here —
`sde-changelog` already works this way, and ADR-0001 is the record of why. The
first option inverts the two-gate rule the row was built around, and the third
leaves a defect that is invisible until someone queries flip counts for a month
that looks fine.

Either of the first two is a corpus row, not an orchestration one. If the second
is chosen, orchestration will need a follow-up row: the panel sensor would have to
stop treating "already built" as final, which is a change to
`_build_sovereignty_gold_sensor`.

## Answer

<!-- The person writes here and sets `status: answered`. -->
