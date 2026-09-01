## Context

See proposal.md — Why. The constraints that shape the approach:

- `sensor_util` owns the retry-safe `run_key` and the in-flight guard "so they
  stay identical across datasets". A second copy in `sensors.py` defeats the
  module's stated reason to exist.
- Run-state is reachable only through `corpus state query`. Both facts this
  sensor needs are already rows in the `partitions` table, confirmed in
  `corpus-cli/src/sde.rs`: Silver commits `dataset = 'sde'`,
  `partition_key = 'build=<n>'`; the changelog commits
  `dataset = 'sde-changelog'`, `tier = 'gold'`, the same `build=<n>` key.
- `sde_changelog_gold` is `output_required=False`. A baseline build finishes
  green without materialising, by design (ADR-0032).

## Goals / Non-Goals

**Goals:**

- The per-tick cap bounds work outstanding, not work ever done.
- One implementation of the retry-safe request loop, shared with every other
  Gold sensor.

**Non-Goals:**

- The non-partitioned SDE Gold assets (snapshot, industry-products, facilities,
  hubs). They are schedule-driven and unaffected.
- Any new corpus subcommand or JSON shape. Nothing here needs one.
- Changing what `sde_changelog_gold` shells out to or records.

## Decisions

**Subtract committed Gold, do not track requests.** The sensor could remember
what it requested — in the cursor, or by trusting `run_key` dedup as it does
today. It should not: a remembered request is a claim that the run succeeded,
and the run may have failed, been cancelled, or completed as a no-op. Committed
Gold in run-state is the same source of truth every other sensor uses, and it is
the only one that is true after a failure. This also makes the sensor's own
history irrelevant to its behaviour, so a rebuilt instance behaves identically.

**Identify the baseline in the sensor, do not queue it.** Rejected alternative:
request it and let the binary skip. With a static `run_key` that costs one no-op
run, which is what the code does today — but a static key is the bug. Once keys
rotate, the baseline (which by construction never commits Gold) is re-requested
every tick, forever. Rejected alternative: a corpus `ready-builds` subcommand
mirroring `gold ready-dates`. That is the principled answer, and it is a corpus
row, not this one; nothing else here needs upstream work, and parking this row
behind another repository to avoid one `min()` is not proportionate. What the
sensor computes is not the binary's predecessor lookup: it declines to queue the
lowest committed build, using the same run-state set it already reads. The
binary still decides, and still writes nothing, if the sensor is ever wrong.

Expressed as the predecessor rule rather than as `min()` — a build is
outstanding only if some smaller build has committed Silver — so the code reads
as the binary's rule (`largest committed Silver build < target`) rather than as
an unexplained "drop the smallest".

**Extend `request_partitions` with an optional `sort_key` rather than sorting
before the call.** The helper re-sorts internally, so a pre-sorted list would be
silently re-ordered lexically and `"99"` would land after `"100"` — the cap would
then take the wrong ten. `sorted(..., key=None)` is exactly the current
behaviour, so every date-keyed caller is unaffected and needs no edit.

**Keep `label` distinct.** The log line stays `sde-gold`, not the shared
`gold-readiness`, because SDE's deferral count is about builds and every other
sensor's is about dates.

## Risks / Trade-offs

- **The sensor's baseline rule drifts from the binary's.** → They read the same
  set (committed `sde` Silver), and the binary is authoritative either way: a
  build the sensor wrongly skips is a missing changelog, not a corrupt one, and
  a build it wrongly queues is a no-op run. A regression test pins the rule.
- **Rotating run keys mean a build can be requested on consecutive ticks.** →
  That is why `request_partitions` carries the in-flight guard; adopting the
  helper is what makes the rotation safe. The guard is best-effort without run
  storage, which only affects unit contexts that launch no runs.
- **A build that fails repeatedly is retried every tick.** → Self-limiting in the
  same way as every other dataset: once corpus commits the partition the build
  leaves the outstanding set. A permanently failing build is a visible run
  failure, which is better than today's silent stall. There is no suppression
  path, and this row does not add one: `skipped_partitions` (corpus ADR-0028) is
  asked only of a *Silver* dataset — "this partition will never exist" — and a
  changelog that fails over a Silver build that does exist is not that. Every
  other Gold sensor in this repository has the same property, so SDE is now
  consistent with them rather than newly exposed.

Two gaps this row leaves standing, both present on `develop` and neither made
worse here. They are one follow-up row, parked as
`docs/questions/2026-09-01-sde-build-sequence-holes.md`:

- **Out-of-order Silver commits diff against the wrong predecessor.**
  `sde_silver` sits in the `everef_download` pool at `default_limit: 2`, so two
  builds ingest concurrently and 300 can commit before 200. The sensor then
  reports 300 outstanding and the binary diffs it against 100, because its
  predecessor rule reads *committed* Silver. Subtracting committed Gold makes
  that terminal — 300 leaves the outstanding set for good — where the old static
  `run_key` made it terminal by dedup instead. The fix is to hold a build back
  until the largest *registered* build below it is committed, which trades a
  silently wrong diff for a visible stall; that trade is the person's to make.
- **`sde_build_discovery_sensor` still has this row's bug.** It keys Silver on a
  static `run_key` and filters on partitions it registered in the same tick, so
  a failed `sde_silver` run is never re-proposed — which is how a hole in the
  build sequence appears in the first place.
