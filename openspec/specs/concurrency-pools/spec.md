# concurrency-pools Specification

## Purpose

Bounds what the code location may run at once against the memory of the single
box it runs on: which concurrency pools exist, which of them carry memory, and
where the one budget that adds them up is stated.

## Requirements

### Requirement: The declared pool set is pinned

The set of concurrency pool names the code location declares SHALL be pinned by
a test. A pool name that is not in the pinned set SHALL fail the suite, so a new
memory-bearing pool cannot enter the deployment before the budget accounts for
it. The pinned set SHALL be checked against the pools the loaded definitions
actually declare, not against a hand-maintained list of assets, because the
drift being guarded against is a pool declared where nobody looked.

#### Scenario: A new pool arrives unaccounted

- **WHEN** an asset declares a `pool=` whose name is outside the pinned set
- **THEN** the test suite fails and names the unaccounted pool

#### Scenario: A pool name drifts

- **WHEN** an asset's pool name is misspelled, so it silently gets a pool of its
  own with its own separate limit
- **THEN** the test suite fails, because the misspelling is not in the pinned set

#### Scenario: The declared set is unchanged

- **WHEN** every declared pool name is in the pinned set
- **THEN** the suite passes

### Requirement: One place states the box budget

The pool arithmetic — every pool, its limit, the peak of one holder of a
memory-bearing pool, and the worst case those sum to against the box — SHALL be
stated in `deploy/dagster.yaml` and nowhere else. Every memory-bearing pool
SHALL be counted in it, including pools whose limit is 1. Other documents SHALL
carry the invariant and a pointer to that file rather than a second copy of the
numbers.

#### Scenario: A maintainer needs the memory ceiling

- **WHEN** a maintainer asks what the deployment can peak at
- **THEN** `deploy/dagster.yaml` names every memory-bearing pool, its limit, its
  per-holder peak and the worst-case sum against the box size

#### Scenario: A pool limit is bounded only against itself

- **WHEN** a pool has limit 1 and is therefore bounded against a second run of
  its own members
- **THEN** the budget still counts it, because a limit-1 pool bounds nothing
  about its overlap with the other pools

### Requirement: Membership of a memory-bearing pool is by measured peak

An asset SHALL join a memory-bearing pool on a measured peak RSS, recorded
beside the membership. An asset admitted on expectation rather than measurement
SHALL be marked as provisional together with what it is waiting to be measured
against.

#### Scenario: A windowed build with a narrow snapshot

- **WHEN** a Gold build scans a wide window but over narrow daily snapshots, and
  its measured peak lands with the lightweight builds
- **THEN** it declares no memory-bearing pool and is bounded by the global cap
  alone, rather than taking a scarce slot from the builds that need one

### Requirement: The market-orders Silver resident window is pinned, not discovered

Every unit that launches `corpus` runs SHALL set `CORPUS_PARSE_CONCURRENCY`
explicitly, so that market-orders Silver's resident-snapshot window — and
therefore its peak — is a chosen constant rather than a function of the host
core count.

#### Scenario: The host core count changes

- **WHEN** the number of cores on the box is changed
- **THEN** market-orders Silver's resident-snapshot window and peak are
  unchanged, because the environment pins the window rather than deriving it
  from the available parallelism

#### Scenario: A run is launched from the UI

- **WHEN** a market-orders Silver run is launched from the webserver launchpad
  rather than by the daemon
- **THEN** the `corpus` subprocess it spawns runs with the same pinned window,
  because both units carry the setting
