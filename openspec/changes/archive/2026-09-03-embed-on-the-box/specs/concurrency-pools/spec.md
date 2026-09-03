# concurrency-pools

## MODIFIED Requirements

### Requirement: Membership of a memory-bearing pool is by measured peak

An asset SHALL join a memory-bearing pool on a measured peak RSS, recorded
beside the membership. An asset admitted on expectation rather than measurement
SHALL be marked as provisional together with what it is waiting to be measured
against.

An asset MAY instead join a memory-bearing pool to guarantee mutual exclusion
with that pool's other holders, when the exclusion cannot be expressed any other
way. Dagster has no construct for "pool X never beside pool Y" — an asset
carries one pool — so being the pool is the only statement of it available. Such
a membership SHALL be recorded in an ADR naming what it excludes and why the
exclusion is needed, because the membership is not derivable from the peak
beside it, and the pool's per-holder peak in the budget SHALL be restated to the
heaviest holder the pool has gained.

#### Scenario: A windowed build with a narrow snapshot

- **WHEN** a Gold build scans a wide window but over narrow daily snapshots, and
  its measured peak lands with the lightweight builds
- **THEN** it declares no memory-bearing pool and is bounded by the global cap
  alone, rather than taking a scarce slot from the builds that need one

#### Scenario: An asset that must never run beside another pool's holders

- **WHEN** an asset's peak is such that it must never overlap the holders of an
  existing memory-bearing pool, and no separate pool of its own can express that
- **THEN** it joins that pool rather than keeping one of its own, an ADR records
  the exclusion as the reason for the membership, and the budget carries the
  pool's new per-holder peak
