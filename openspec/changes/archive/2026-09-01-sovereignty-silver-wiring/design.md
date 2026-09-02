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

This is worth pinning with a test rather than a reader's confidence, because the
mistake it guards against is invisible in the resolved dates. Reading
`sov-panel`'s `panel.flip_window_days: 30` as a Silver reach-back would give it a
preload of `2022-01-31 - 30 = 2022-01-01`, and `_silver_start` takes the **minimum**
preload across the dataset's derivatives — `sovereignty-map`'s tenure pair already
reaches back to `2022-01-01 - 180 = 2021-07-05`, so the wrong panel value is
discarded by the `min` and moves nothing. The shape rule therefore has to be
asserted directly, on the lookback, not inferred from a start date that would look
identical either way.

### `sovereignty-map` needs a derivative selector; the other two do not

`resolve_partition_starts` resolves a Gold start as well as a Silver one, and
`_select_derivative` raises unless a multi-derivative dataset is given a name.
`sovereignty-map` declares three (`sovereignty-ownership`, `sovereignty-changes`,
`sovereignty-panel`), so its Silver asset must pass one —
`sovereignty-ownership`, the tenure derivative that sets the Silver reach-back —
and use only `.silver` from the result, exactly as `system_jumps.py` passes
`HISTORY_DERIVATIVE`. `sovereignty-structures` and `sovereignty-campaigns` declare
one derivative each and need no selector.

The choice of selector does not affect the Silver start: `_silver_start` runs over
every derivative regardless of which one was selected. It is named for the Gold
half of the return value, which this row does not use.

The resolved Silver starts this row pins are therefore `2021-07-05` for
`sovereignty-map` (`2022-01-01 - 180`, above the `2021-07-01` floor, so the clamp
does not bite), `2021-10-03` for `sovereignty-structures` (`2022-01-01 - 90`) and
`2022-01-01` for `sovereignty-campaigns` (no reach-back). All three are before the
`2022-12-16` layout-era boundary, so the boundary test has real dates on both
sides.

### `pool="everef_download"`, and no measurement

`deploy/dagster.yaml` documents `everef_download` as politeness to
`data.everef.net` with negligible memory. A Silver ingest of hourly JSON snapshots
is the same operation the eight members already there perform. The AGENTS.md rule
"membership of a memory-bearing pool is by measured peak" governs entry to a
memory-bearing pool; `everef_download` is not one, so the row's note to measure
before joining one is a constraint on the **Gold** row, not this one. No number is
needed here, and inventing one would be worse than omitting it.

### The `skipped` branch is copied; the `incomplete` branch is not

Two different absent-day stories, and only one of them applies here.

`system_jumps_silver` carries `output_required=False` and branches on an ingest
that exits zero with `status: "skipped"` — an interior day EVE Ref never published
(corpus ADR-0028). It skips the verify, which would fail against a partition that
was deliberately not written, yields an `AssetObservation` recording why, and
leaves the partition Missing rather than materialising an empty one. The
sovereignty datasets are the same `hourly-folder-tar` shape over the same
archive, and their first backfill is four years wide, so gap days are not an edge
case — an asset without this branch fails on the first one. All three copy it.

`market_history_silver` branches on `incomplete` under ADR-0041, which is specific
to the daily-file layout where a single day's file can be published partially.
That state does not arise for `hourly-folder-tar`, and copying the branch would add
a Python decision about something the binary does not report for these datasets.

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
  `everef_download`. Within a sensor's horizon the backlog is carried, not
  dropped.
- **The sensors do not drain the history**, and are not meant to. `corpus everef
  missing-partitions` defaults to a 30-day window and none of the three passes
  `window_days`, so the sensors keep the trailing edge current and never reach
  2021–2024. That history is a UI backfill, which the same two concurrency layers
  bound. Both are the landed behaviour of every other everef availability sensor;
  the row changes neither.
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
