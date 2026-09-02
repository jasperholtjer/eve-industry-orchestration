## ADDED Requirements

### Requirement: A multi-hour backfill is planned before it is launched

A backfill whose ingests would hold a concurrency slot for hours SHALL be planned
in the same place the box budget is stated, before it is launched against the
box. The plan SHALL name what is to be done about the daily schedules it would
overlap — that they are paused for its duration, or that the backfill takes a
bound of its own — so the choice is made before the runs queue rather than
discovered from a failure.

The length of a backfill SHALL NOT by itself decide which bound its runs hold.
Membership is settled by the existing measured-peak requirement over the
per-partition work; a long run of a light build is still a light build.

#### Scenario: A backfill overlapping the daily schedules

- **WHEN** a backfill would hold a slot across the window in which the daily
  schedules fire
- **THEN** the budget states how that overlap is handled before the backfill is
  launched

#### Scenario: Length is not memory

- **WHEN** a dataset's full backfill would run for hours while its per-partition
  build holds a bounded, light working set
- **THEN** it declares no memory-bearing bound on account of the backfill's
  length, and is bounded by the upstream-fetch and global caps alone

#### Scenario: The wiring is not the launch

- **WHEN** a dataset's asset and availability sensor are added
- **THEN** the historical backfill is not thereby started: the sensor covers the
  trailing edge only, and the full range is an operator action taken against the
  plan
