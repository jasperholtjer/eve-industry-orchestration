## Purpose

Defines how a Silver partition for the sovereignty dataset family is produced and
offered: how the partition start of a dataset whose Gold trees use the `sov-*`
shapes is derived, which corpus operations a Silver asset invokes and in what
order, what its availability sensor is allowed to decide on its own, and the
concurrency bound the ingests run under.

## ADDED Requirements

### Requirement: A sovereignty dataset's partition start is derived from its configuration

The orchestrator SHALL derive the Silver partition start of `sovereignty-map`,
`sovereignty-structures` and `sovereignty-campaigns` from the corpus dataset
configuration and SHALL NOT carry a literal start date for any of them. The
derivation SHALL account for every Gold derivative the dataset declares: a Silver
partition start is the earliest date any of that dataset's derivatives reaches
back to, so a dataset with an unresolvable derivative has no resolvable Silver
start.

Each sovereignty Gold shape SHALL contribute the reach-back its own configuration
declares:

- a tenure shape SHALL reach back by the tenure look-back its configuration
  declares;
- a flat multi-horizon shape SHALL reach back by its widest declared horizon;
- a shape that declares no reach-back SHALL contribute none, including the
  assembled panel, whose inputs are sibling Gold trees rather than a Silver
  window.

A derived start earlier than the Silver coverage floor the dataset declares SHALL
be clamped up to that floor.

#### Scenario: A tenure-shaped derivative sets the reach-back

- **WHEN** the partition start is resolved for a dataset whose Gold derivative
  declares a tenure shape with a look-back of N days and a served start of D
- **THEN** that derivative contributes a Silver reach-back of D minus N days

#### Scenario: A flat multi-horizon derivative reaches back by its widest horizon

- **WHEN** the partition start is resolved for a dataset whose Gold derivative
  declares a flat multi-horizon shape with horizons of 7, 30 and 90 days
- **THEN** that derivative contributes a Silver reach-back of 90 days before its
  served start

#### Scenario: A shape with no reach-back contributes none

- **WHEN** the partition start is resolved for a dataset whose Gold derivative
  declares a shape with no look-back and no coverage gate
- **THEN** that derivative contributes a reach-back of zero days, and its served
  start alone is what it offers

#### Scenario: The panel contributes no Silver reach-back

- **WHEN** the partition start is resolved for a dataset that declares the
  assembled sovereignty panel among its Gold derivatives
- **THEN** the panel contributes a reach-back of zero days, because it reads
  sibling Gold trees rather than a Silver window

#### Scenario: The derived start is clamped to the declared coverage floor

- **WHEN** the earliest reach-back across a dataset's derivatives falls before
  the Silver coverage floor the dataset declares
- **THEN** the resolved Silver partition start is the coverage floor

#### Scenario: Every sovereignty dataset resolves

- **WHEN** a partition start is resolved for `sovereignty-map`,
  `sovereignty-structures` or `sovereignty-campaigns`
- **THEN** a start date is returned and no unknown-shape error is raised

### Requirement: A sovereignty Silver partition is ingested, then verified

For a given date the orchestrator SHALL invoke the corpus Silver ingest for that
dataset and date and, only after it has succeeded, invoke the corpus contract
verification for the Silver tier of the same dataset and date. Verification SHALL
NOT be attempted for an ingest that failed. A failing ingest or a failing
verification SHALL fail the materialisation.

The orchestrator SHALL NOT open, parse or validate the ingested payload, and SHALL
NOT decide anything about the storage layout of the dates it requests — including
that these datasets span a change of on-disk layout partway through their history,
which the binary resolves from the dataset configuration.

#### Scenario: A successful ingest is verified

- **WHEN** the Silver asset for a sovereignty dataset materialises a date and the
  ingest succeeds
- **THEN** contract verification for the Silver tier of that dataset and date is
  invoked, and the materialisation succeeds only if it also succeeds

#### Scenario: A failing ingest is not verified

- **WHEN** the ingest for a date exits non-zero
- **THEN** verification is not invoked and the materialisation fails

#### Scenario: A date before the layout change is requested like any other

- **WHEN** a date earlier than the dataset's on-disk layout change is
  materialised
- **THEN** the orchestrator requests it with the same operation and arguments as
  any later date, and does not branch on the layout era

### Requirement: A sovereignty materialisation records what corpus measured

A successful sovereignty Silver materialisation SHALL record the facts corpus
registered for the partition it wrote — the row count, the retention class and
the partition checksum — read from the corpus run-state rather than from anything
the asset computed. That read SHALL be advisory: when the run-state has no row for
the partition, or the read fails, the materialisation SHALL still succeed and
record its identifying fields alone.

#### Scenario: Run-state facts are recorded

- **WHEN** a sovereignty Silver partition materialises successfully and corpus has
  registered a run-state row for it
- **THEN** the materialisation records that row's row count, retention class and
  partition checksum alongside its identifying fields

#### Scenario: A missing run-state row does not fail the run

- **WHEN** a sovereignty Silver partition materialises successfully but the
  run-state read returns no row or fails
- **THEN** the materialisation succeeds, records its identifying fields, and warns

### Requirement: Sovereignty availability is decided from the run-state

Each sovereignty dataset SHALL have an availability sensor that decides which
partitions to request by asking corpus which partitions are missing, comparing
upstream availability against the corpus run-state. A sensor SHALL NOT decide
availability by inspecting the storage tree, and SHALL NOT request a partition
outside the dataset's resolved partition range.

A sensor SHALL request no more partitions in one tick than the shared per-tick
fan-out cap allows, and SHALL leave the remainder for a later tick rather than
dropping them.

#### Scenario: Missing partitions are requested

- **WHEN** a sovereignty availability sensor ticks and corpus reports partitions
  that are available upstream but absent from the run-state
- **THEN** the sensor requests a run for those partitions

#### Scenario: Nothing missing requests nothing

- **WHEN** a sovereignty availability sensor ticks and corpus reports no missing
  partitions
- **THEN** the sensor requests no runs

#### Scenario: A long backlog is capped and carried

- **WHEN** corpus reports more missing partitions than the per-tick fan-out cap
- **THEN** the sensor requests at most the cap in that tick, and the remaining
  partitions are still reported missing on the following tick

#### Scenario: Availability is not read from the storage tree

- **WHEN** a sovereignty availability sensor ticks
- **THEN** it reaches the corpus run-state through the corpus binary and performs
  no listing of the storage tree

### Requirement: Sovereignty Silver ingests run under the upstream politeness bound

The three sovereignty Silver assets SHALL run under the same concurrency bound
that limits concurrent fetches from the upstream EVE Ref host, so the family
cannot exceed it however a run is launched. They SHALL NOT be placed under a
memory-bearing bound, and this capability SHALL NOT introduce a new one.

#### Scenario: Each Silver asset carries the upstream fetch bound

- **WHEN** the sovereignty Silver assets are defined
- **THEN** each declares the upstream-fetch concurrency bound, and none declares a
  memory-bearing bound

#### Scenario: No new bound is introduced

- **WHEN** this capability's assets are added
- **THEN** the set of declared concurrency bounds is unchanged
