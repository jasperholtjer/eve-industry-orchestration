---
status: open
row: sovereignty-gold-panel
---

# Should a permanently-absent Silver day count against a `coverage_min_ratio: 1.0` window?

## Why this is blocked

`sovereignty-ownership` and `sovereignty-changes` both carry a 180-day tenure
window at `coverage_min_ratio: 1.0`, and the dataset YAML says why: *"A hole
anywhere in the window hides a flip, which would report a falsely short tenure
rather than a missing one: demand the whole window."* That reasoning is sound.
Its consequence was never computed.

`corpus_core::window_coverage` sizes the denominator on the calendar —
`silvers_expected = horizon` — so a day that upstream will *never* publish
counts against the ratio for as long as it sits inside the window. EVE Ref took
two outages inside the served range, and both are recorded as ADR-0028 permanent
skips. Each one therefore blocks both trees for 180 consecutive days:

```
sov-ownership / sov-changes unbuildable:
  2023-02-01 .. 2023-07-30   (180 days)
  2023-11-28 .. 2024-05-25   (180 days)
  = 360 of 1 692 candidate days (21.3 %)
```

`sovereignty-panel` reads the same-day ownership partition, so it inherits the
blackout in full. A fifth of the sovereignty Gold range is unbuildable, forever,
and nothing about it is a bug in the sovereignty code — it is what a calendar
denominator means when upstream has a permanent hole.

Two things make it worse than the number suggests.

It is **silent**. `scan_candidates` only pushes a date into `ready` or `blocked`
when the coverage gate passes; a date that fails it appears in neither. So
`gold ready-dates --json` reports nothing at all for those 360 days — no
`blocked` entry, no `waiting_on`, no stderr line. An operator sees a sensor that
never proposes half of 2023 and finds nothing that says why.

And nothing has exercised this path yet. `structure-population-history` gates at
`0.5` over 30 days, so its eleven `structures` gaps never bite; `market-history`
is the only other strict gate and its Silver has no holes at all. The
sovereignty tenure pair would be the first `1.0` window in production over a
tree with permanent gaps — and the ~1 700-day backfill that lands it is the next
thing this family does.

## The measurement

Verified 2026-09-01, not inferred. Run-state (`<nas>/state/corpus-state.db`) is
the source for the recorded skips; `data.everef.net` per-year `index.json`
listings and the tar-era year archives are the source for what upstream
actually published.

`system-jumps`, `system-kills` and `industry-cost-indices` each record exactly
twelve `skipped_partitions`, on identical dates. All three sovereignty archives
miss the same twelve, and the tar era below the folder boundary is complete:

| Era | Range | Result |
|---|---|---|
| tar (`sovereignty-map-2021.tar.bz2`) | 2021-07-01 .. 12-31 | 184 / 184 days |
| tar (`sovereignty-map-2022.tar.bz2`) | 2022-01-01 .. 12-15 | 349 / 349 days |
| folder `index.json` | 2022-12-16 .. 2026-09-01 | 12 days absent |

The twelve are `2023-01-27..01-31` (5 days) and `2023-11-21..11-27` (7 days) —
one EVE Ref-wide outage, byte-identical across all six datasets checked.

The outages have ramps. Snapshots per day at the edges are `01-26: 8`,
`02-01: 10`, `11-20: 19`, `11-28: 3`, against 24 either side. Those days are
*present*, so the gate reads them as full and the arithmetic above is unchanged.
They do shift attribution: the fold takes the last snapshot of the day, so a
late flip on 2023-11-28 lands on 11-29. That is visible in `n_snapshots` and is
not what this question is about.

## The options

- **Exclude recorded gaps from the denominator.** A window is complete when it
  holds every day upstream will ever publish. Cheapest, and it makes the ratio
  mean what an operator assumes it means. But it silently accepts a window that
  genuinely hides flips — precisely what the YAML comment refused — and it
  changes the meaning of `coverage_min_ratio` for every dataset that uses it.
- **Keep `1.0`, and make the hole a column rather than a block.** The tenure
  columns already carry a censoring concept: `tenure_censored` says a holding
  was already in place at the window's left edge. A sibling flag — the window
  this row was observed over had a permanent hole — says the same kind of thing
  about the same kind of uncertainty, and lets all 360 days build while staying
  honest about which are degraded. It is also what ADR-0066 decision 8 already
  chose one level up, for the panel's flip counts.
- **Lower `coverage_min_ratio` below `1.0`.** Blunt: a ratio cannot tell a
  permanent gap from a day that has not landed yet, which is the exact
  distinction ADR-0065 exists to draw. A threshold loose enough to clear a
  7-day hole also clears a 7-day lag.
- **Accept the 360-day hole and document it.** Costs nothing to build. Leaves a
  fifth of the range missing with no surface that explains it, discovered by
  whoever runs the backfill.

Whichever is chosen, `scan_candidates` should stop dropping coverage-failed
dates from both `ready` and `blocked`. A date held back by a gate the operator
cannot see is the part of this that is indefensible under any option.

## What I would do

The second option. The uncertainty is real — a 5-day hole inside a 180-day
tenure window can hide a flip and shorten a `tenure_days` — so the honest move
is to publish it as uncertainty rather than to either hide it (option one) or
withhold a fifth of the dataset because of it (option four). The machinery is
already there: this family computes censored tenure and reports it per row, and
ADR-0066 made the same call for the panel's flip counts, so the pattern is
established rather than invented here.

Option one is defensible and much cheaper, and if it is chosen the change should
be narrow — the sovereignty tenure shapes only, not `window_coverage` for every
caller, because the trade differs per dataset.

This is a corpus row. It should land before the sovereignty backfill runs, since
that backfill is where the 360-day hole becomes visible the hard way. It is also
upstream of
[2026-09-01-sov-panel-flip-window-gate.md](2026-09-01-sov-panel-flip-window-gate.md):
while this gate stands, every day that question is about is already unbuildable.

## Answer

<!-- The person writes here and sets `status: answered`. -->
