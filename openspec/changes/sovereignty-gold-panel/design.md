## Context

See `proposal.md` — Why. The constraints that shape the approach:

- The three sovereignty Silver assets already exist, one module per dataset
  (`defs/sovereignty_map.py`, `defs/sovereignty_structures.py`,
  `defs/sovereignty_campaigns.py`), and none of the three imports another.
- `resolve_partition_starts(dataset, derivative)` already returns a per-derivative
  `.gold` start; a single-derivative dataset resolves its derivative on its own.
  The panel's later served start therefore already falls out of configuration —
  nothing in `defs/config.py` needs to change.
- `CorpusResource.gold_ready_dates(dataset, derivative=...)` already takes the
  derivative selector, and `defs/sensors.py` already carries a per-derivative Gold
  sensor factory for the market-orders family.
- `defs/killmails.py#killmails_consumption_gold` is the closest existing asset: a
  Gold build with cross-dataset `deps=`, `output_required=False`, a skipped-day
  branch and run-state metadata keyed on the derivative rather than the dataset.
- Corpus is read-only from here, and both gates in the spec are the binary's to
  decide.

## Goals / Non-Goals

**Goals:**

- Five Gold assets and five readiness sensors that are, individually, the shape
  the repository already uses — so that the only genuinely new thing in the row is
  the panel's cross-dataset dependency edge.
- Per-derivative partition definitions, so the panel's later start is a
  consequence of configuration rather than of a branch.

**Non-Goals:**

- Measuring peak RSS for any of the five builds. Without a measurement none may
  join a memory-bearing pool, and taking the measurement needs the Rust binary and
  the NAS, neither of which this row has. `deploy/dagster.yaml` is untouched.
- Surfacing corpus's `blocked[].permanent` field. `gold_ready_dates` reads only
  `ready` today, and the skipped-day branch is reported by the build itself, so
  nothing in this row needs the blocked list.
- Backfilling anything. The sovereignty family has never been materialised; the
  first backfill stays UI-driven, as `sovereignty-silver-wiring` left it.

## Decisions

**Assets live in the module of the dataset that owns the derivative name, and the
panel lives in `sovereignty_map.py`.** `sovereignty-panel` is declared under
`sovereignty-map`'s `gold:` block, so that module owns it, exactly as
`killmails.py` owns `killmails_consumption_gold` despite that build reading two
other datasets. `sovereignty_map.py` then imports `sovereignty_structures` and
`sovereignty_campaigns` for the panel's `deps=`; neither imports back, so the
graph stays acyclic. *Alternative:* a sixth `sovereignty_panel.py` module. It
would avoid the import edge, but it separates a derivative from the dataset
configuration that declares it and breaks the one-module-per-dataset rule the
family already follows for a cost the import edge does not actually impose.

**One partitions definition per derivative, each from its own resolve.** Not the
market-orders pattern of one shared `gold_partitions` across a dataset's
derivatives: sovereignty-map's two Gold trees and its panel do not share a start.
Each asset takes `dg.DailyPartitionsDefinition(start_date=resolve_partition_starts(
DATASET, <derivative>).gold)`. The 2022-01-31 panel start is then never written
down here, which is what the spec requires. *Alternative:* share one definition per
dataset and let the panel start early. Rejected — it would offer partitions the
binary can only ever skip.

**A shared Gold sensor factory for the family, parameterised on dataset as well as
derivative.** `_build_orderbook_gold_sensor` is closed over `mo.DATASET` and
`mo.gold_partitions`; the sovereignty equivalent spans three datasets and five
partition definitions, so it takes `(dataset, derivative, asset, partitions)`.
Five near-identical sensor bodies is the alternative, and the divergence risk
across five copies of a cap-and-dedup loop is exactly what the market-orders
factory already exists to avoid.

**Every asset mirrors `killmails_consumption_gold`'s branch structure, including
the four that cannot currently skip.** `output_required=False`, the
`status == "skipped"` → `AssetObservation` branch, then verify, then a
`MaterializeResult` whose run-state read is keyed on the derivative. Only the
panel has a same-day prerequisite that can be permanently absent today, but ADR-0065
is a property of the Gold build contract rather than of one shape, and a per-asset
judgment about which builds may report a skip is exactly the pre-validation the
thin-orchestration rule forbids. The uniform branch also costs nothing to test.

**Nothing distinguishes an incomplete flip window.** It is not a branch that
returns early — it is the *absence* of a branch: a build reporting a written
partition is materialised whatever it did with its window. The spec states this as
a requirement because the tempting mistake is to add the branch, and a test asserts
the ordinary path for a build that reports a written partition alongside an
incomplete window.

**No `pool=` on any of the five.** Membership of a memory-bearing pool is by
measured peak, and none of these has one. The four per-dataset trees resemble the
narrow windowed builds (`system-jumps`, `system-kills`, `cost-indices`) that stayed
off `heavy`; the panel reads three same-day Gold partitions, a 30-day Gold window
and one reference snapshot, which is narrower still. The global cap applies to all
five regardless. Revisiting this needs a `/usr/bin/time -v` number and belongs to
whichever row can take one.

## Risks / Trade-offs

- **The panel's `deps=` on four siblings makes it the family's most constrained
  asset, and a sibling that never materialises stalls it.** → That is the intended
  reading of ADR-0066 decision 8, and the stall is visible: the panel's readiness
  sensor reports nothing ready, rather than the panel building from a partial
  input set. The alternative — sequencing by schedule — hides the same stall.
- **`sovereignty_map.py` importing two sibling modules is a new edge in a family
  that had none.** → It is one direction only and the two imported modules import
  nothing from the family; a test that loads the definitions catches a cycle
  immediately.
- **Five sensors on one family multiplies sensor ticks against one corpus binary.**
  → Each is an hourly `corpus gold ready-dates` call, the same rate the
  market-orders family already runs three of, and the shared per-tick fan-out cap
  bounds what any one tick requests.
- **The four non-panel assets carry a skipped-day branch that their builds may
  never exercise in production.** → Cheap to carry, and it is covered by the fake
  binary rather than by waiting for a real skip.

## Migration Plan

None. Five new assets and five new sensors, all `DefaultSensorStatus.STOPPED` like
every other sensor here; nothing existing changes behaviour, and the family has no
materialised history to migrate. Rollback is removing the definitions.
