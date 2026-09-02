## ADDED Requirements

### Requirement: A multi-hour backfill is planned before it is launched

A backfill whose ingests would hold a concurrency slot for hours SHALL be planned
in the same place the box budget is stated, before it is launched against the
box. The plan SHALL name, for that dataset, whether a peak RSS has been measured;
which bound its runs hold; and what is to be done about the daily schedules it
would overlap — either that they are paused for its duration, or that it takes a
bound of its own once a peak is known.

A dataset whose peak has not been measured SHALL NOT be given a memory-bearing
bound on the strength of the backfill's length alone, because length is not
memory. It SHALL run under the bound its per-partition work actually justifies,
and the launch of the long backfill SHALL be recorded as gated on the
measurement rather than on the wiring being present.

#### Scenario: A long backfill has no measured peak

- **WHEN** a dataset is wired whose full backfill would run for hours and no peak
  RSS has been measured for its build
- **THEN** it declares no memory-bearing bound, the budget records that no
  measurement exists and what would produce one, and the backfill's launch is
  gated on that measurement

#### Scenario: The wiring is not the launch

- **WHEN** a dataset's asset and availability sensor are added
- **THEN** the historical backfill is not thereby started: the sensor covers the
  trailing edge only, and the full range is an operator action taken against the
  plan

#### Scenario: A backfill overlapping the daily schedules

- **WHEN** a backfill would hold a slot across the window in which the daily
  schedules fire
- **THEN** the budget states how that overlap is handled, so the choice is made
  before the runs queue rather than discovered from a failure
