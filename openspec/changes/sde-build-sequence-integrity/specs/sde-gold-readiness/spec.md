## Purpose

Decides which SDE builds the orchestrator asks corpus to ingest, and which it
asks for an `sde-changelog` Gold run, so that every build that can produce a
changelog eventually gets one — against the predecessor the binary will actually
pick — and no build is stranded by a run that failed, by a backlog longer than
one tick, or by a hole in the build sequence.

## ADDED Requirements

### Requirement: A build whose Silver did not commit is proposed again

Silver readiness SHALL be read from corpus run-state: a discovered build is
outstanding for ingest when it has no committed `sde` Silver partition,
regardless of whether the orchestrator registered its partition key on an earlier
tick. A request SHALL NOT be suppressed on the grounds that the same build was
requested before, so that a failed ingest is retried and a hole in the build
sequence heals on its own.

The eligible set SHALL be the builds already registered as partitions together
with those newly discovered on this tick, so that a build registered earlier
whose ingest failed is reconsidered.

#### Scenario: A registered build whose ingest failed

- **WHEN** a build's partition key was registered on an earlier tick and its `sde`
  Silver partition is still not committed
- **THEN** it is proposed again on the next tick, as a distinct run

#### Scenario: A newly discovered build

- **WHEN** a build appears upstream that has no registered partition key
- **THEN** its partition key is registered and its ingest is requested on the same
  tick

#### Scenario: A build whose Silver is committed

- **WHEN** a build's `sde` Silver partition is committed
- **THEN** it is not proposed for ingest again

#### Scenario: Numeric build ordering under the cap

- **WHEN** builds 99 and 100 are both outstanding for ingest and the cap is 1
- **THEN** build 99 is requested first

### Requirement: A changelog built against a superseded predecessor is rebuilt

A changelog is built against the largest committed `sde` Silver build below its
own. Where a lower build's Silver commits after that changelog was built, the
changelog no longer describes the diff the binary would produce now. The
orchestrator SHALL detect this from run-state and return the affected build to
the outstanding set, so the wrong diff is repaired on a later tick rather than
persisting.

Detection SHALL compare only the **nearest** lower committed Silver build against
the changelog's own commit time, so that re-ingesting an old build does not flag
every changelog above it. Detection SHALL read the run-state `partitions` table
through the sanctioned `corpus state query` seam.

The repair SHALL be an ordinary rematerialise of the same partition. The
orchestrator SHALL NOT compute the predecessor, the diff, or which rows changed.

#### Scenario: A lower Silver commits after the changelog was built

- **WHEN** build 300's changelog Gold was committed while only build 100 had
  committed Silver, and build 200's Silver commits afterwards
- **THEN** build 300 is outstanding again and a Gold run is requested for it

#### Scenario: A changelog whose predecessor is unchanged

- **WHEN** every committed Silver build below a committed changelog was committed
  before that changelog
- **THEN** that build is not requested again

#### Scenario: The baseline build has no predecessor

- **WHEN** a committed changelog has no committed Silver build below it
- **THEN** it is not reported stale

#### Scenario: A build re-ingested far below an existing changelog

- **WHEN** builds 100, 200 and 300 have changelogs built in order and build 50's
  Silver is ingested afterwards
- **THEN** only build 100, whose nearest lower Silver build is now 50, is
  requested again

### Requirement: A Gold build is deferred while a lower Silver run is in flight

Where a lower build's `sde` Silver run is queued or in flight, the changelog for a
higher build would be diffed against a predecessor that is about to change. The
orchestrator SHALL hold that build back for the tick rather than request it.

The deferral SHALL be bounded by the in-flight run's own lifetime: a deferred
build SHALL remain in the outstanding set and be requested on a later tick. The
orchestrator SHALL NOT make readiness depend on the registered build sequence, so
that a build whose Silver never commits can never stop a later changelog.

#### Scenario: A lower Silver run in flight

- **WHEN** build 300 is outstanding and build 200's `sde_silver` run has not
  reached a terminal state
- **THEN** build 300 is not requested on that tick
- **AND** the deferral is logged

#### Scenario: The lower run reaches a terminal state

- **WHEN** that run finishes and the next tick fires
- **THEN** build 300 is requested

#### Scenario: A higher Silver run in flight

- **WHEN** build 300 is outstanding and build 400's `sde_silver` run is in flight
- **THEN** build 300 is still requested

#### Scenario: A build whose Silver never commits

- **WHEN** a build is permanently excluded from ingest and never commits Silver
- **THEN** builds above it are still requested, and the changelog stream does not
  stall

### Requirement: The build number is the key and the release date is a label

Ordering, predecessor selection, deferral and stale detection SHALL all key on
the build number. A build has more than one release date and they disagree, and a
date describes upstream publication rather than commit order, which is the axis
this capability turns on.

The release date SHALL be carried as a label only — in log lines and on the run
request — and SHALL be taken from the upstream listing the sensor already
fetches, never parsed from the corpus `done_path` layout.

#### Scenario: A build requested for ingest

- **WHEN** an ingest is requested for a build
- **THEN** the log line names the build number and its release date
- **AND** the release date is attached to the run request as a label

#### Scenario: A stale changelog reported

- **WHEN** a changelog is reported stale
- **THEN** the log line names the build number

## MODIFIED Requirements

### Requirement: One run per build at a time

Because a build can be requested on more than one tick, the orchestrator SHALL
NOT request a build that already has a run in flight for the asset being
requested, so that two `corpus` processes never write the same contract directory
at once. This applies to the `sde` Silver ingest and to the `sde-changelog` Gold
build alike.

#### Scenario: A slow run still in flight

- **WHEN** a build's Gold run has not yet reached a terminal state and the next
  tick fires
- **THEN** that build is not requested again on that tick

#### Scenario: A slow ingest still in flight

- **WHEN** a build's `sde_silver` run has not yet reached a terminal state and the
  next tick fires
- **THEN** that build is not proposed for ingest again on that tick
