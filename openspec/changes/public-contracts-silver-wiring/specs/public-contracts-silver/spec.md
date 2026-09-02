## Purpose

Defines how a Silver partition of the public-contracts history tier is produced
and offered: how the partition start of a dataset that declares no Gold
derivative is derived, which corpus operations the asset invokes and in what
order, what its availability sensor may decide on its own, and the concurrency
bound its ingests run under.

## ADDED Requirements

### Requirement: A dataset with no Gold derivative resolves its Silver start from its coverage floor

The orchestrator SHALL derive the Silver partition start of a dataset that
declares no Gold derivative from the Silver coverage floor that dataset's
configuration declares, and SHALL NOT carry a literal start date for it. A
dataset that declares neither a Gold derivative nor a Silver coverage floor SHALL
have no resolvable Silver start and SHALL raise rather than fall back to a
default.

Resolution for a dataset that does declare Gold derivatives SHALL be unchanged:
its Silver start remains the earliest reach-back across those derivatives,
clamped up to its coverage floor where it declares one.

#### Scenario: A Silver-only dataset resolves from its floor

- **WHEN** the Silver partition start is resolved for a dataset whose
  configuration declares a Silver coverage floor and no Gold derivative
- **THEN** the coverage floor is returned as the Silver partition start

#### Scenario: Neither a derivative nor a floor is an error

- **WHEN** the Silver partition start is resolved for a dataset whose
  configuration declares no Gold derivative and no Silver coverage floor
- **THEN** the resolution fails with a configuration error and no start date is
  returned

#### Scenario: A dataset with derivatives is unaffected

- **WHEN** the Silver partition start is resolved for a dataset that declares one
  or more Gold derivatives
- **THEN** it is derived from those derivatives exactly as before, and the
  Silver-only path is not taken

#### Scenario: The public-contracts history matrix starts at its floor

- **WHEN** the public-contracts Silver partition definition is built
- **THEN** its start is the coverage floor the corpus dataset configuration
  declares, and no date literal appears in the asset module

### Requirement: A public-contracts Silver partition is ingested, then verified

For a given date the orchestrator SHALL invoke the corpus Silver ingest for
`public-contracts` and that date and, only after it has succeeded, invoke the
corpus contract verification for the Silver tier of the same dataset and date.
Verification SHALL NOT be attempted for an ingest that failed. A failing ingest
or a failing verification SHALL fail the materialisation.

The orchestrator SHALL NOT open, parse or validate the ingested payload, SHALL
NOT decide anything about how the day's snapshots are merged into one partition,
and SHALL NOT branch on the packaging era of the date it requests.

#### Scenario: A successful ingest is verified

- **WHEN** the asset materialises a date and the ingest succeeds
- **THEN** contract verification for the Silver tier of that dataset and date is
  invoked, and the materialisation succeeds only if it also succeeds

#### Scenario: A failing ingest is not verified

- **WHEN** the ingest for a date exits non-zero
- **THEN** verification is not invoked and the materialisation fails

#### Scenario: Every date is requested the same way

- **WHEN** any date within the resolved partition range is materialised
- **THEN** it is requested with the same operation and arguments as any other
  date, and the asset does not branch on how many snapshots that day holds or
  how they are packaged

### Requirement: A day the upstream never published leaves its partition missing

When the ingest for a date succeeds while reporting that it wrote no partition
because the upstream published nothing for that day, the orchestrator SHALL NOT
verify that date, SHALL NOT record a materialisation for it, and SHALL NOT fail
the run. The partition SHALL be left unmaterialised, and the run SHALL record an
observation naming the reason, so an absent day is distinguishable from a
partition that was never attempted.

The orchestrator SHALL NOT itself decide that a day is absent upstream; it acts
only on what the ingest reports.

#### Scenario: An absent upstream day is observed, not materialised

- **WHEN** the ingest for a date succeeds and reports that it wrote no partition
  because the upstream day is absent
- **THEN** verification is not invoked, the partition is left unmaterialised, the
  run succeeds, and an observation records the reason

#### Scenario: An absent day does not fail a backfill

- **WHEN** a range of dates is materialised and one interior date is absent
  upstream
- **THEN** that date's run succeeds without materialising it, and the remaining
  dates materialise as normal

### Requirement: A public-contracts materialisation records what corpus measured

A successful materialisation SHALL record the facts corpus registered for the
partition it wrote — the row count, the retention class and the partition
checksum — read from the corpus run-state rather than from anything the asset
computed. That read SHALL be advisory: when the run-state has no row for the
partition, or the read fails, the materialisation SHALL still succeed and record
its identifying fields alone.

#### Scenario: Run-state facts are recorded

- **WHEN** a partition materialises successfully and corpus has registered a
  run-state row for it
- **THEN** the materialisation records that row's row count, retention class and
  partition checksum alongside its identifying fields

#### Scenario: A missing run-state row does not fail the run

- **WHEN** a partition materialises successfully but the run-state read returns
  no row or fails
- **THEN** the materialisation succeeds, records its identifying fields, and
  warns

### Requirement: Public-contracts availability is decided from the run-state

The dataset SHALL have an availability sensor that decides which partitions to
request by asking corpus which partitions are missing, comparing upstream
availability against the corpus run-state. The sensor SHALL NOT decide
availability by inspecting the storage tree, and SHALL NOT request a partition
outside the dataset's resolved partition range.

The sensor SHALL request no more partitions in one tick than the shared per-tick
fan-out cap allows, and SHALL leave the remainder for a later tick rather than
dropping them. It SHALL NOT be the mechanism by which the history is backfilled.

#### Scenario: Missing partitions are requested

- **WHEN** the sensor ticks and corpus reports partitions that are available
  upstream but absent from the run-state
- **THEN** the sensor requests a run for those partitions

#### Scenario: Nothing missing requests nothing

- **WHEN** the sensor ticks and corpus reports no missing partitions
- **THEN** the sensor requests no runs

#### Scenario: A long backlog is capped and carried

- **WHEN** corpus reports more missing partitions than the per-tick fan-out cap
- **THEN** the sensor requests at most the cap in that tick, and the remaining
  partitions are still reported missing on the following tick

#### Scenario: Availability is not read from the storage tree

- **WHEN** the sensor ticks
- **THEN** it reaches the corpus run-state through the corpus binary and performs
  no listing of the storage tree

### Requirement: The history tier is wired independently of its live twin

The public-contracts history tier and the public-contracts live current-overwrite
tier SHALL be separate datasets with separate assets, and neither SHALL be a
dependency of the other. Wiring the history tier SHALL NOT change what the live
tier records, when it fires, or which concurrency bound it runs under.

#### Scenario: The live twin is unchanged

- **WHEN** the history tier's asset, sensor and tests are added
- **THEN** the live tier's asset, its schedule and its recorded fields are
  unchanged, and no dependency is declared between the two

### Requirement: Public-contracts Silver ingests run under the upstream politeness bound

The Silver asset SHALL run under the same concurrency bound that limits
concurrent fetches from the upstream host, so the dataset cannot exceed it
however a run is launched. It SHALL NOT be placed under a memory-bearing bound
while no measured peak exists for it, and this capability SHALL NOT introduce a
new bound.

#### Scenario: The Silver asset carries the upstream fetch bound

- **WHEN** the Silver asset is defined
- **THEN** it declares the upstream-fetch concurrency bound and no memory-bearing
  bound

#### Scenario: No new bound is introduced

- **WHEN** this capability's asset is added
- **THEN** the set of declared concurrency bounds is unchanged
