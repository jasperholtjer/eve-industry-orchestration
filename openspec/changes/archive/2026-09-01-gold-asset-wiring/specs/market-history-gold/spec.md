## Purpose

Defines how a market-history Gold partition is produced: which corpus operations
the orchestrator invokes and in what order, where the rolling-window coverage
gate is enforced, what the availability sensor is allowed to decide on its own,
and the concurrency bound a build runs under. The point of the capability is that
the orchestrator triggers and records the build without ever re-deciding whether
the build is allowed.

## ADDED Requirements

### Requirement: A Gold partition is built, then verified against the contract

For a given Gold partition date the orchestrator SHALL invoke the corpus Gold
build for that date and, only after it has succeeded and written a partition,
invoke the corpus contract verification for the Gold tier of the same date.
Verification SHALL NOT be attempted for a build that failed, nor for a build
that exited zero reporting it wrote no partition, and the materialisation SHALL
be reported successful only when both operations exit zero.

#### Scenario: Build and verification both succeed

- **WHEN** a Gold partition is materialised for a date whose Silver window is
  complete
- **THEN** the Gold build runs for that date
- **AND** the Gold-tier verification runs for the same date afterwards
- **AND** the materialisation is reported successful, carrying the dataset, the
  tier and the partition date

#### Scenario: The build fails

- **WHEN** the Gold build exits non-zero for the target date
- **THEN** the run fails
- **AND** the Gold-tier verification is not attempted

#### Scenario: Verification fails after a successful build

- **WHEN** the Gold build succeeds and the Gold-tier verification exits non-zero
- **THEN** the run fails rather than reporting a successful materialisation

#### Scenario: The build skips a date recorded as an upstream gap

- **WHEN** the Gold build exits zero reporting that it skipped the target date
  because that day's Silver is a recorded upstream gap, writing no partition
- **THEN** the Gold-tier verification is not attempted
- **AND** nothing is reported as materialised, leaving the partition missing
- **AND** the run does not fail

### Requirement: The coverage gate belongs to the binary

The rolling-window coverage gate SHALL be enforced by the corpus binary, which
reads the full `[date - max_horizon, date]` Silver window and applies the
dataset's `coverage_min_ratio`. The orchestrator SHALL NOT evaluate window
coverage itself, SHALL NOT open a partition to inspect it, and SHALL NOT
suppress or override a non-zero exit from the build. An incomplete window is a
failed run, never a degraded partition.

#### Scenario: An incomplete window reaches the build

- **WHEN** a Gold build is launched for a date whose Silver window does not meet
  `coverage_min_ratio`
- **THEN** the build exits non-zero and the run fails
- **AND** no Gold partition is recorded as materialised

#### Scenario: Coverage is never recomputed in the orchestrator

- **WHEN** a Gold partition is materialised
- **THEN** the orchestrator performs no coverage arithmetic of its own and reads
  no partition contents; the window decision is taken only by the binary

### Requirement: Readiness is read from corpus, never recomputed

The availability sensor SHALL obtain the set of buildable dates from the corpus
Gold readiness report, which resolves — from corpus run-state — that the target
day's Silver is present, that the look-back window meets `coverage_min_ratio`,
and that the Gold partition is not yet built. The sensor SHALL NOT derive
readiness from the filesystem, SHALL NOT glob the storage tree, and SHALL NOT
reimplement any part of that decision.

#### Scenario: A date corpus does not report ready

- **WHEN** a tick runs and a date is not in the readiness report
- **THEN** no Gold run is requested for that date

#### Scenario: A date corpus reports ready

- **WHEN** a tick runs and a date is reported ready and is a valid Gold partition
  key
- **THEN** a Gold run is requested for that date

#### Scenario: A reported date outside the partition range

- **WHEN** the readiness report names a date that is not a valid Gold partition
  key
- **THEN** it is ignored rather than requested

### Requirement: The pre-check is an optimisation, not the gate

The sensor's readiness pre-check exists to avoid queuing runs that are certain to
fail; it SHALL NOT be treated as the authority on whether a build may proceed. A
date the sensor requested SHALL still be gated by the binary at build time, and a
build that fails the gate SHALL fail the run.

#### Scenario: Readiness goes stale between tick and run

- **WHEN** a date reported ready is requested, and the window no longer satisfies
  the gate by the time the build runs
- **THEN** the build exits non-zero and the run fails
- **AND** the orchestrator does not write, claim or repair a partition in response

### Requirement: Requests are ordered, capped and retry-safe per tick

Requested dates SHALL be ordered oldest first and limited to a fixed maximum per
tick, so a backlog drains over successive ticks rather than stalling. A date
whose run is already in flight for the Gold asset SHALL NOT be requested again in
the same tick. Run keys SHALL rotate per tick so that a date still reported ready
after a failed or lost run is requested again on a later tick rather than
suppressed permanently by deduplication.

#### Scenario: A backlog longer than one tick

- **WHEN** more dates are reported ready than the per-tick cap allows
- **THEN** the oldest capped-many are requested this tick
- **AND** the number deferred is logged
- **AND** a later tick requests the next ones

#### Scenario: A run that did not materialise

- **WHEN** a date was requested on an earlier tick and is still reported ready on
  a later tick
- **THEN** it is requested again as a distinct run

#### Scenario: A run already in flight

- **WHEN** a date is reported ready and a run for that Gold partition is already
  in flight
- **THEN** it is not requested again on this tick

### Requirement: Concurrent Gold builds are bounded on every launch path

Because a market-history Gold build streams its full look-back window and peaks
at a measured ~3-4 GiB resident, the number of such builds running at once SHALL
be bounded by a concurrency pool declared on the asset. The bound SHALL apply to
every launch path — sensor, backfill and manual materialisation alike — rather
than only to sensor-launched runs.

#### Scenario: More builds requested than the pool allows

- **WHEN** more Gold builds are launched at once than the pool permits, by any
  combination of launch paths
- **THEN** the excess runs queue until a pool slot frees rather than running
  concurrently

### Requirement: The orchestrator selects placement and never constructs layout

The orchestrator SHALL pass the storage root to each corpus operation as a flag
and SHALL leave the partition layout, the file names, the checksums and the
completion markers entirely to the binary. It SHALL NOT build a path from the
partition date and SHALL NOT move contract bytes.

#### Scenario: Root selection

- **WHEN** the Gold build and the verification are invoked
- **THEN** both receive the configured storage root
- **AND** neither is given a path the orchestrator assembled from the date

#### Scenario: Partition start date comes from configuration

- **WHEN** the set of valid Gold partition keys is determined
- **THEN** it derives from the dataset configuration's Gold start date rather
  than from a literal in the orchestrator
