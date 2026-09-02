## Why

`public-contracts` — the history tier of public contracts, corpus ADR-0067/0068,
corpus row `public-contracts-silver`, landed 2026-09-01 — is built in corpus and
has no asset here. Its live twin `public-contracts-live` is already wired and is
a separate dataset over the same upstream; this row is the history half and
touches nothing of the twin.

Two things make this a row rather than a fix, and they are what the change has
to settle.

**The dataset cannot be resolved at all today.** `public-contracts.yaml`
declares no `gold:` block — the derivatives belong to corpus's
`public-contracts-gold`, which is still open — and only a
`silver.served_start: 2021-06-17` coverage floor. Every partition start in this
repo is derived from a dataset's Gold derivatives, so a dataset with none has no
resolvable Silver start and raises. `public-contracts` is the first Silver-only
history dataset, and the resolution path it needs is the change's contract
surface. It is a path under the existing coverage-floor decision (ADR-0027), not
a new decision: no ADR is added.

**The backfill has to be planned before it can be run.** 1 892 day-partitions,
447.85 GiB of source, ~8.2 h at the upstream politeness bound of 2 concurrent
transfers — an ingest that holds a slot across several daily schedules. Corpus's
own measurements (`tmp/contracts/measurements-2026-09-01.md` §7.5) name it as a
run-planning question and answer neither half of it. This change answers it the
only way the evidence allows: no measured peak RSS exists for this build
anywhere — not in that file, not in the run-state on the NAS, which holds zero
rows for either public-contracts dataset — so the asset joins the upstream-fetch
politeness bound and no memory-bearing one, and the backfill is gated on a
measurement rather than on a guess. The existing budget already carries two
members admitted without one; this row does not add a third.

## What Changes

- **CONTRACT** — `defs/config.py` gains a Silver-only resolution path: a dataset
  that declares no Gold derivatives resolves its Silver partition start from its
  declared Silver coverage floor. The existing Gold-derivative path is
  untouched, and no start date is hardcoded.
- `defs/public_contracts.py` (new): one day-partitioned `public_contracts_silver`
  asset over `corpus ingest` then `corpus verify --tier silver`, in the
  `everef_download` pool, with the upstream-gap skip branch, on the
  `system_jumps_silver` mould.
- `defs/sensors.py`: `public_contracts_availability_sensor`, off `corpus everef
  missing-partitions` keyed on run-state, capped per tick, default-stopped like
  its siblings.
- **CONTRACT** — `deploy/dagster.yaml`: `public-contracts` Silver is added to the
  `everef_download` member list with no memory claim, and the arithmetic block
  records that the multi-hour backfill must not be launched against the box
  until a peak is measured, together with what would produce one. No pool is
  added and no limit moves, so the pinned pool set is unchanged.
- `tests/fake_corpus.py` and `tests/test_public_contracts.py`: the dataset's
  ingest, verify and `missing-partitions` cases, and the asset and sensor tests.

The row runs the `add-dataset-to-orchestration` skill for the wiring
touchpoints; it does not restate them here.

## Capabilities

### New Capabilities
- `public-contracts-silver`

### Modified Capabilities
- `concurrency-pools` — one added requirement on how a multi-hour backfill is
  planned. `public-contracts-live` is untouched.

## Impact

- Modules: `defs/config.py` (new resolution path), `defs/public_contracts.py`
  (new), `defs/sensors.py` (new sensor). No change to `corpus_resource.py`.
- Corpus surface shelled: `corpus ingest --dataset public-contracts --date <d>`,
  `corpus verify --tier silver`, `corpus everef missing-partitions`, `corpus
  state query`. All four already exist and are exercised by other datasets; this
  row adds no upstream dependency.
- Concurrency: `everef_download` only. The pinned pool set in
  `tests/test_concurrency_pools.py` does not move.
- Not in this row: any Gold asset. Corpus's `public-contracts-gold` is still
  `todo`, and an asset for a tree that does not exist would be dead code.
