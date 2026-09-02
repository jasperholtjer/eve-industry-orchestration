# silver-upstream-absence Specification

## Purpose

Governs how every Silver asset in the code location treats a day the ingest
declined to write: which absence statuses it must act on, how the permanent and
the retryable case are told apart, and the discipline that decides which assets
carry a branch at all rather than dead code.

## Requirements

### Requirement: A Silver asset acts on every absence status its dataset can produce

A Silver asset SHALL branch on each status the ingest can return for its own
dataset without writing a partition, and SHALL NOT fall through to contract
verification on any of them. Verification of a partition that was never written
fails, so an unhandled absence status turns a run the ingest completed
successfully into a failed one.

The two absences SHALL be kept distinct, because their remedies differ. A day
the upstream never published is settled: it is left unmaterialised and is not
expected to be reattempted. A day whose publication has not landed yet is
retryable: it is left unmaterialised and is re-proposed on a later tick, so it
materialises once the upstream settles.

#### Scenario: A still-publishing day completes green

- **WHEN** the ingest for a date exits successfully reporting that it wrote no
  partition because the day's publication has not landed yet
- **THEN** verification is not invoked, the partition is left unmaterialised, the
  run succeeds, and an observation records the reason as retryable

#### Scenario: A still-publishing day does not fail once per tick

- **WHEN** a date is re-proposed on each availability tick while its upstream
  publication remains unlanded
- **THEN** every one of those runs completes green without materialising, rather
  than failing on a verification of a partition that was never written

#### Scenario: The two absences are recorded differently

- **WHEN** one date is absent upstream permanently and another is still
  publishing
- **THEN** the observations recorded for them carry different reasons, so an
  operator can tell a settled gap from one that will heal itself

### Requirement: A branch is carried only where the status is reachable

A Silver asset SHALL carry a branch for an absence status only where the ingest
can actually emit that status for its dataset. Reachability is decided by the
data plane's fetch layer and the dataset's own configuration, not by this
repository, and SHALL be established before a branch is added rather than
assumed from a sibling asset that has one.

Where a status is not reachable for a dataset, the asset SHALL NOT carry a
branch for it, and its module SHALL record why it does not — naming the
configuration or layout that makes the status unreachable — so that the absence
reads as a decision rather than as an omission a later reader should repair.

#### Scenario: An unreachable status gets no branch

- **WHEN** a dataset's fetch layer has no path that emits a given absence status
- **THEN** its Silver asset carries no branch for that status, and the module
  states the reason the path cannot be reached

#### Scenario: A reachable status gets a branch

- **WHEN** a dataset's configuration opens a fetch-layer path that emits an
  absence status
- **THEN** its Silver asset branches on that status, and the module names the
  path that makes it reachable

#### Scenario: A sibling's branch is not evidence

- **WHEN** one dataset's Silver asset carries a branch for an absence status
- **THEN** that alone does not establish the status is reachable for a sibling
  dataset, and the sibling's own configuration decides whether it carries one
