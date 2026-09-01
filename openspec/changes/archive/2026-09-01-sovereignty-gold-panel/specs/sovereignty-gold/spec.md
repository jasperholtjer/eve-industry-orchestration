## Purpose

Defines how the sovereignty family's Gold derivatives are built, offered and
recorded: how each derivative's partition start is derived, which corpus
operations a Gold asset invokes and in what order, the two distinct gates a build
may report and what each one means for the partition, how the assembled panel's
Gold-over-Gold inputs are expressed as dependencies, what each derivative's
readiness sensor may decide on its own, and the concurrency bound the builds run
under.

## ADDED Requirements

### Requirement: Each sovereignty Gold derivative is built by name

The orchestrator SHALL build every sovereignty Gold derivative — the ownership
tree, the events tree, the ADM tree, the contests tree and the assembled panel —
through its own build invocation naming that derivative, and SHALL NOT build a
sovereignty dataset's Gold trees with a single dataset-wide invocation. Two
derivatives of the same dataset SHALL be separate materialisable units.

For a given date the orchestrator SHALL invoke the corpus Gold build for that
derivative and date and, only after it has succeeded and reported that it wrote a
partition, invoke the corpus contract verification for the Gold tier of the same
derivative and date. A failing build or a failing verification SHALL fail the
materialisation.

The orchestrator SHALL NOT open, parse or validate the built payload, and SHALL
NOT evaluate any coverage or completeness condition before invoking the build.
Whether a date can be built is the build's own answer.

#### Scenario: A derivative is built and then verified

- **WHEN** a sovereignty Gold asset materialises a date and the build succeeds
  reporting a written partition
- **THEN** contract verification for the Gold tier of that derivative and date is
  invoked, and the materialisation succeeds only if it also succeeds

#### Scenario: A failing build is not verified

- **WHEN** the build for a date exits non-zero
- **THEN** verification is not invoked and the materialisation fails

#### Scenario: Two derivatives of one dataset are built separately

- **WHEN** the two derivatives that share a source dataset are materialised for
  the same date
- **THEN** each is built by an invocation naming only itself, and neither build
  produces the other's partition

#### Scenario: No completeness check precedes a build

- **WHEN** a sovereignty Gold asset materialises a date
- **THEN** it invokes the build without first inspecting the availability,
  coverage or completeness of that build's inputs

### Requirement: A sovereignty Gold partition start is the derivative's own

The orchestrator SHALL derive each sovereignty Gold derivative's partition start
from the served start that derivative declares in the corpus dataset
configuration, and SHALL NOT carry a literal start date for any of them. Two
derivatives of the same dataset MAY have different partition starts, and the
orchestrator SHALL honour that difference rather than sharing one start across a
dataset.

The assembled panel's start SHALL come from the configuration like any other. The
orchestrator SHALL NOT compute it from its siblings' starts and the flip window,
and SHALL NOT encode the relationship between them.

#### Scenario: Each derivative resolves its own start

- **WHEN** the Gold partition start is resolved for each of the five sovereignty
  derivatives
- **THEN** each returns the served start its own configuration declares

#### Scenario: The panel starts later than its siblings

- **WHEN** the Gold partition start is resolved for the assembled panel and for a
  sibling derivative of the same dataset
- **THEN** the panel's start is the later of the two, and it is read from
  configuration rather than derived from the sibling's start

#### Scenario: No start date is written into the definitions

- **WHEN** the sovereignty Gold assets are defined
- **THEN** no literal start date appears among them, and every partition
  definition takes its start from the resolver

### Requirement: A day whose prerequisite is permanently absent is observed, not materialised

When a build succeeds while reporting that it wrote no partition because a
prerequisite for that day can never arrive, the orchestrator SHALL NOT verify that
date, SHALL NOT record a materialisation for it, and SHALL NOT fail the run. The
partition SHALL be left unmaterialised, and the run SHALL record an observation
naming the reason so the absence is distinguishable from a partition that was
never attempted.

The orchestrator SHALL NOT itself decide that a prerequisite is absent or that its
absence is permanent; it acts only on what the build reports.

#### Scenario: A skipped day is observed

- **WHEN** the build for a date succeeds and reports that it wrote no partition
  because a prerequisite is permanently absent
- **THEN** verification is not invoked, the partition is left unmaterialised, the
  run succeeds, and an observation records the reason

#### Scenario: A skipped day does not fail a backfill

- **WHEN** a range of dates is materialised and one interior date is skipped
- **THEN** that date's run succeeds without materialising it, and the remaining
  dates materialise as normal

#### Scenario: Permanence is not decided in the orchestrator

- **WHEN** a sovereignty Gold asset materialises a date
- **THEN** it classifies the day only from what the build reported, and performs
  no check of its own on whether a prerequisite may still arrive

### Requirement: An incomplete trailing window is not a skipped day

A build that succeeds and reports a written partition SHALL be materialised and
verified as an ordinary success, including when the trailing window feeding a
derived column was incomplete and the build published that column as null. The
orchestrator SHALL NOT treat an incomplete window as a reason to skip, to withhold
verification, or to fail the run, and SHALL NOT inspect the window's coverage to
decide.

This gate and the permanently-absent-prerequisite gate SHALL remain distinct: the
first yields a partition whose derived column is null, the second yields no
partition at all.

#### Scenario: A partition built over an incomplete window materialises normally

- **WHEN** the build for a date succeeds reporting a written partition, having
  found the trailing window that feeds a derived column incomplete
- **THEN** verification is invoked, the partition materialises, and the run
  succeeds

#### Scenario: The window is not inspected

- **WHEN** the assembled panel materialises a date
- **THEN** it performs no check on the coverage of the trailing window, and no
  branch in the asset distinguishes a complete window from an incomplete one

#### Scenario: The two gates are not conflated

- **WHEN** one date reports a skipped day and another reports a written partition
  over an incomplete window
- **THEN** the first is observed without a materialisation and the second
  materialises, and neither outcome is produced for the other's report

### Requirement: The assembled panel's inputs are declared dependencies

The assembled panel SHALL declare a dependency on every sibling Gold derivative it
reads and on the reference-data Gold snapshot it reads, so that its build order is
part of the asset graph rather than a consequence of when sensors or schedules
happen to fire.

A dependency on a non-partitioned asset SHALL carry lineage only and SHALL NOT
place that asset in the panel's partition matrix.

#### Scenario: Every sibling tree is a declared dependency

- **WHEN** the assembled panel asset is defined
- **THEN** it declares a dependency on each of the four sibling sovereignty Gold
  derivatives it reads

#### Scenario: The reference-data snapshot is a declared dependency

- **WHEN** the assembled panel asset is defined
- **THEN** it declares a dependency on the reference-data Gold snapshot asset

#### Scenario: The build order is declared, not scheduled

- **WHEN** the panel's build order is inspected
- **THEN** it is expressed by the panel's declared dependencies rather than by an
  ordering between schedules or sensor intervals

#### Scenario: Runtime sequencing is the binary's gate, not a check here

- **WHEN** the panel's readiness for a date is decided
- **THEN** it is decided by corpus reporting that date ready, which gates on the
  same-day sibling partitions it requires
- **AND** the orchestrator adds no sequencing check of its own, so a sibling input
  that corpus does not gate on is not gated here either

#### Scenario: The non-partitioned dependency stays out of the partition matrix

- **WHEN** the panel materialises a date
- **THEN** the dependency on the non-partitioned reference-data snapshot
  contributes no partition mapping and no partition of it is required

### Requirement: Sovereignty Gold availability is decided from corpus-reported readiness

Each sovereignty Gold derivative SHALL have an availability sensor that decides
which partitions to request by asking corpus which dates that derivative reports
ready. A sensor SHALL be keyed on a single derivative, SHALL NOT infer readiness
from its source Silver run, SHALL NOT decide readiness by inspecting the storage
tree, and SHALL NOT request a partition outside that derivative's resolved
partition range.

A sensor SHALL request no more partitions in one tick than the shared per-tick
fan-out cap allows, and SHALL leave the remainder for a later tick rather than
dropping them.

#### Scenario: Ready dates are requested

- **WHEN** a sovereignty Gold sensor ticks and corpus reports dates ready for that
  derivative
- **THEN** the sensor requests a run for those partitions of that derivative's
  asset alone

#### Scenario: Nothing ready requests nothing

- **WHEN** a sovereignty Gold sensor ticks and corpus reports no ready dates
- **THEN** the sensor requests no runs

#### Scenario: A date outside the derivative's range is not requested

- **WHEN** corpus reports a ready date earlier than the derivative's resolved
  partition start
- **THEN** the sensor does not request it

#### Scenario: A long backlog is capped and carried

- **WHEN** corpus reports more ready dates than the per-tick fan-out cap
- **THEN** the sensor requests at most the cap in that tick, and the remaining
  dates are still reported ready on the following tick

#### Scenario: Readiness is not read from the storage tree

- **WHEN** a sovereignty Gold sensor ticks
- **THEN** it reaches corpus through the binary and performs no listing of the
  storage tree

### Requirement: A sovereignty Gold partition is identified by its derivative

What a materialisation records, where those facts come from and what happens when
they cannot be read are governed by the `materialisation-metadata` capability and
are not restated here. This capability constrains only which partition is looked
up: corpus registers a Gold partition under the derivative that produced it, not
under the source dataset, so a sovereignty Gold materialisation SHALL address
run-state by its own derivative's name. Two derivatives of one dataset SHALL
therefore record different facts for the same date.

#### Scenario: The lookup is keyed on the derivative

- **WHEN** a sovereignty Gold partition materialises successfully
- **THEN** the run-state lookup addresses the partition under the derivative's own
  name rather than under its source dataset's

#### Scenario: Two derivatives of one dataset do not share a record

- **WHEN** the two derivatives sharing a source dataset both materialise the same
  date and corpus has registered different facts for each
- **THEN** each materialisation records the facts registered for its own
  derivative

### Requirement: Sovereignty Gold builds introduce no new concurrency bound

The five sovereignty Gold assets SHALL NOT declare a memory-bearing concurrency
bound, because no measured peak memory has been recorded for any of them and
membership of such a bound is by measurement. They SHALL run under the global
concurrency cap alone, and this capability SHALL NOT introduce a new bound.

#### Scenario: No sovereignty Gold asset declares a memory-bearing bound

- **WHEN** the sovereignty Gold assets are defined
- **THEN** none of them declares a memory-bearing concurrency bound

#### Scenario: No new bound is introduced

- **WHEN** this capability's assets are added
- **THEN** the set of declared concurrency bounds is unchanged
