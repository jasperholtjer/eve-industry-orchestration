---
status: open
row: sde-gold-sensor-stall
---

# Should a hole in the SDE build sequence block the next changelog, or diff across it?

## Why this is blocked

`sde-gold-sensor-stall` fixed the SDE Gold sensor's stall: it now subtracts the
builds whose changelog Gold is committed before applying the per-tick cap, and
requests the rest through `request_partitions`. Reviewing that change surfaced a
second, older defect underneath it, which the row deliberately did not widen to
cover because the answer is a policy choice rather than a bug fix.

The binary's predecessor is "the largest **committed** Silver build below the
target" (`corpus-cli/src/sde.rs::build_changelog`). The sensor asks for a build
as soon as *any* smaller build has committed Silver. Those two are not the same
question whenever the build sequence has a hole, and it can:

- `sde_silver` carries `pool="everef_download"` at `default_limit: 2`, so two
  builds ingest at once and 300 can commit before 200. The Gold tick in between
  sees `{100, 300}`, asks for 300, and the binary diffs 300 against **100**.
  When 200 lands it is diffed against 100 as well. Gold then holds an overlapping
  100→300 and 100→200, and the 200→300 diff never exists.
- `sde_build_discovery_sensor` still has exactly the bug this row fixed for Gold:
  a static `run_key` per build, filtered against the dynamic partitions it
  registered in the same tick. From the next tick the build counts as known, so a
  failed `sde_silver` run is never re-proposed and the hole is permanent.

Subtracting committed Gold makes the first one terminal — once 300's changelog is
committed it leaves the outstanding set for good. The old static `run_key` made
it terminal too, by dedup, so the row did not make this worse; it did make it the
explicit stop condition, which is why it is worth settling now.

## The options

- **Block on the sequence.** A build is outstanding only when the largest
  *registered* build below it has committed Silver. Cheap — the sensor already
  holds both sets — and it makes the sensor's readiness rule the binary's rule.
  The cost: a build whose Silver never commits blocks every later changelog until
  someone intervenes, and there is no suppression path (`skipped_partitions`,
  corpus ADR-0028, answers "this Silver will never exist", which is a different
  question and is asked only of a Silver dataset).
- **Diff across the hole, and repair later.** Keep today's rule and add a repair
  sensor, the way killmails already does for drifted Gold
  (`killmails_consumption_gold_repair_sensor` asks run-state which Gold predates
  its own Silver). Here it would ask which changelog partitions were built
  against a predecessor that is no longer the largest committed one below them,
  and rebuild those. Needs a corpus query or subcommand that can answer it — the
  changelog's recorded predecessor is not something this repo may compute.
- **Remove the hole instead.** Give `sde_build_discovery_sensor` the same
  treatment this row gave the Gold sensor: key Silver readiness on run-state
  (registered builds minus committed Silver) rather than on "not yet registered",
  and let `request_partitions` rotate the key. Then a failed ingest is retried,
  holes close on their own, and the ordering race narrows to the window where two
  pooled runs are genuinely in flight — which the in-flight guard already covers
  per partition, but not across partitions.

## What I would do

The third and then the first, as one follow-up row in that order. Fixing the
discovery sensor is the same fix this row just made, in the sibling sensor, and
it removes most of the exposure on its own — a hole that heals cannot become a
permanent wrong diff. Blocking on the sequence then closes the concurrency
window, and its "a stuck build stalls the rest" cost is acceptable precisely
because holes now heal: the stall becomes temporary rather than terminal. The
repair-sensor option is more machinery than the problem earns, and it needs
corpus work this repository cannot do for itself.

I would not fold either into `sde-gold-sensor-stall`. That row's spec is
`sde-gold-readiness` and its goal was the stall; blocking rules and a second
sensor are a second capability, and the row is already a strict improvement on
what `develop` had.

## Answer

<!-- The person writes here and sets `status: answered`. -->
