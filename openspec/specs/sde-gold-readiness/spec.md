# sde-gold-readiness Specification

## Purpose

Decides which SDE builds the orchestrator asks for an `sde-changelog` Gold run,
so that every build that can produce a changelog eventually gets one exactly
once, and no build is stranded by a run that failed or by a backlog longer than
one tick.

## Requirements

### Requirement: Only outstanding builds are requested

Readiness SHALL be read from corpus run-state: a build is outstanding when its
`sde` Silver partition is committed and its `sde-changelog` Gold partition is
not. A build whose changelog Gold is already committed SHALL NOT be requested
again.

#### Scenario: A build whose Gold is already committed

- **WHEN** a tick runs and every build with committed Silver also has a committed
  `sde-changelog` Gold partition
- **THEN** no Gold run is requested

#### Scenario: One build behind

- **WHEN** builds 100, 200 and 300 have committed Silver and only 200's changelog
  Gold is committed
- **THEN** exactly one Gold run is requested, for build 300
- **AND** build 100 is not requested, because it is the baseline

### Requirement: The baseline build is not requested

The lowest build with committed Silver has no committed predecessor, so the
binary skips it and writes no Gold partition. The orchestrator SHALL leave that
build out of the outstanding set rather than queue a run that is guaranteed to
write nothing. The skip decision itself remains the binary's: the orchestrator
only declines to queue the run.

#### Scenario: A cold corpus with one build

- **WHEN** exactly one build has committed Silver
- **THEN** no Gold run is requested

#### Scenario: A build ingested below the previous baseline

- **WHEN** build 200 was the baseline and build 100 is ingested afterwards
- **THEN** build 200 becomes outstanding and is requested
- **AND** build 100 is now the baseline and is not requested

### Requirement: The per-tick cap bounds the outstanding work

The number of Gold runs requested per tick SHALL be capped, and the cap SHALL be
applied to the outstanding builds, oldest build first. Builds SHALL be ordered
numerically, not lexically, so build 99 precedes build 100. A backlog larger than
the cap SHALL drain over successive ticks rather than stall.

#### Scenario: A backlog longer than one tick

- **WHEN** more builds are outstanding than the per-tick cap allows
- **THEN** the oldest capped-many are requested this tick
- **AND** the number deferred is logged
- **AND** a later tick, after those partitions commit, requests the next ones

#### Scenario: Numeric build ordering

- **WHEN** builds 99 and 100 are both outstanding and the cap is 1
- **THEN** build 99 is requested first

### Requirement: A run that did not materialise is retried

The changelog asset may complete without materialising, and a run may fail. Where
a build is still outstanding on a later tick, the orchestrator SHALL request it
again; a request SHALL NOT be suppressed on the grounds that the same build was
requested on an earlier tick.

#### Scenario: A build still outstanding after its run

- **WHEN** a build was requested on an earlier tick and its changelog Gold is
  still not committed
- **THEN** it is requested again on the next tick, as a distinct run

### Requirement: One run per build at a time

Because a build can be requested on more than one tick, the orchestrator SHALL
NOT request a build that already has a run in flight for the changelog asset, so
that two `corpus` processes never write the same contract directory at once.

#### Scenario: A slow run still in flight

- **WHEN** a build's Gold run has not yet reached a terminal state and the next
  tick fires
- **THEN** that build is not requested again on that tick
