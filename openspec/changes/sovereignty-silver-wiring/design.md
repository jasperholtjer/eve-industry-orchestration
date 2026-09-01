# Design

## Context

See `proposal.md` — Why. The constraints that shape the approach:

- `defs/config.py` resolves a dataset's Silver partition start as the earliest
  reach-back across **all** of that dataset's Gold derivatives, clamped up to the
  Silver coverage floor. `_lookback_for_shape` raises `PartitionConfigError` on an
  unknown shape, so one unresolvable derivative makes its whole dataset
  unresolvable. That is why a Silver-only row has to teach all five `sov-*`
  shapes, four of whose Gold trees are the next row.
- The three datasets are `hourly-folder-tar` and cross a folder/tar era boundary
  at `2022-12-16` while serving Silver from `2021-07-01`. The dataset YAML
  describes the boundary and `corpus ingest` resolves it; nothing about it may
  reach Python.
- `everef_download` is declared in `deploy/dagster.yaml` as a politeness cap on
  fetches from `data.everef.net`, explicitly *not* a memory bound. The
  memory-bearing pools (`heavy`, `market_orders`, `news_embed`) hold windowed Gold
  builds.

## Goals / Non-Goals

**Goals**

- Every `sov-*` shape resolves to a reach-back, so `resolve_partition_starts`
  answers for all three datasets.
- Three Silver assets and three availability sensors indistinguishable in shape
  from the landed `system-jumps` / `system-kills` pair, so the family carries no
  bespoke machinery.

**Non-Goals**

- No `corpus gold build` asset, no `--derivative` threading, no cross-dataset
  dependency. That is `sovereignty-gold-panel`.
- No new concurrency pool, and no change to `deploy/dagster.yaml` or
  `tests/test_concurrency_pools.py`.
- No backfill is launched, and no peak RSS is measured. Nothing is materialised by
  this change.

## Decisions

### One module per dataset, not one sovereignty module

Three files — `defs/sovereignty_map.py`, `defs/sovereignty_structures.py`,
`defs/sovereignty_campaigns.py` — following the one-file-per-dataset convention
every other family already uses.

*Alternative considered:* a single `defs/sovereignty.py` holding all three, since
they are one family and their assets are near-identical. Rejected: the family is
one only at the Gold layer, where the panel assembles siblings. At Silver they are
three independent EVE Ref datasets with independent availability, and the next row
adds five Gold derivatives across them — a shared module would have to be split
again immediately. The convention is also what makes a module discoverable from a
dataset name.

### A new `_tenure_lookback` helper, mirroring `_flat_lookback`

`sov-ownership` and `sov-events` are the first shapes that read a `tenure` block
(`tenure.tenure_lookback_days`). They get a helper shaped exactly like the
existing `_flat_lookback`: read the block, fail loudly with `PartitionConfigError`
if it is absent or malformed, return the day count.

*Alternative considered:* reading `tenure_lookback_days` inline in the two
branches. Rejected for the reason `_flat_lookback` exists — two branches sharing
one malformed-config error path should share one function, and the next row adds
no further tenure shapes but the corpus family may.

### `sov-adm` reuses `_flat_lookback` rather than getting its own rule

`sov-adm` declares `flat.horizons: [7, 30, 90]` — the same block, the same
`max(horizons)` rule, under a different shape discriminator. The branch is added
to the existing `flat-multi-horizon` arm rather than duplicating its body.

### `sov-contests` and `sov-panel` resolve to a zero reach-back

Both declare no look-back. `sov-contests` has no coverage gate at all;
`sov-panel`'s inputs are three sibling **Gold** trees plus a trailing `sov-events`
window over Gold, never a Silver window, so it constrains no Silver start. They
take the rule `structures-snapshot` already carries.

This is worth stating because it is the one place a plausible mistake changes a
number rather than raising: giving `sov-panel` its `panel.flip_window_days: 30` as
a Silver reach-back would pull `sovereignty-map`'s Silver start 30 days earlier
than the configuration asks for. The clamp to the `2021-07-01` coverage floor
happens to hide it today, which is exactly why it needs a test rather than a
reader's confidence.

### `pool="everef_download"`, and no measurement

`deploy/dagster.yaml` documents `everef_download` as politeness to
`data.everef.net` with negligible memory. A Silver ingest of hourly JSON snapshots
is the same operation the eight members already there perform. The CLAUDE.md rule
"membership of a memory-bearing pool is by measured peak" governs entry to a
memory-bearing pool; `everef_download` is not one, so the row's note to measure
before joining one is a constraint on the **Gold** row, not this one. No number is
needed here, and inventing one would be worse than omitting it.

### The `incomplete` branch is not copied

`market_history_silver` branches on an `incomplete` ingest status under ADR-0041,
which is specific to the daily-file layout where a day can be published partially.
The sovereignty datasets are `hourly-folder-tar`; the `system-jumps` Silver asset
is the closer precedent and carries no such branch. Copying it would add a Python
decision about a state the binary does not report for these datasets.

## Risks / Trade-offs

- **A wrong `tenure` key name silently resolves to the wrong start.** →
  `_tenure_lookback` raises `PartitionConfigError` on an absent or non-integer
  block rather than defaulting to zero, and the config tests assert the resolved
  start per dataset against the dates the YAML implies, not just that it resolves.
- **The era boundary tempts a Python special case** on the first real backfill,
  because 2021-07-01 through 2022-12-15 is a different on-disk layout. → The spec
  states the asset requests every date identically; a test materialises a date on
  each side of the boundary and asserts the same command shape.
- **Three sensors triple the per-tick fan-out against one NAS spindle.** → Each
  sensor uses the shared `sensor_util.MAX_PARTITIONS_PER_TICK` cap, and every run
  they queue is bounded by both the global `max_concurrent_runs` and
  `everef_download`. The backlog is carried, not dropped.
- **The whole history is the first backfill** (nothing materialised, ~4 years x 3
  datasets). → Out of scope by design: this change defines the assets, it does not
  launch a backfill. Whoever runs it does so through a UI backfill that the same
  two concurrency layers bound.

## Migration Plan

None. Three datasets that raised on partition-start resolution now resolve; no
existing dataset's resolution, asset or sensor changes. Rollback is reverting the
merge — no state is written and nothing downstream depends on the new assets until
`sovereignty-gold-panel`.

## Open Questions

None.
