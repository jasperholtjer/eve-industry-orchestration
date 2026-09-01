# public-contracts-live Specification

## Purpose

Defines how the live public-contracts Gold tree is refreshed and recorded: the
current-overwrite lifecycle expressed as a single non-partitioned asset, the
fixed cadence it runs on and why availability is not sensed, the concurrency
bound the fetch runs under, the freshness the materialisation carries, and the
run-state read it must not attempt.

## Requirements

### Requirement: The live tree is refreshed by shelling the corpus binary

The code location SHALL refresh `gold/public-contracts-live/current/` by
invoking the corpus binary's live build operation for the
`public-contracts-live` dataset, passing the configured sink root, and SHALL do
nothing else with the tree. It SHALL NOT construct the partition path, open the
parquet, parse the snapshot archive, or pre-validate the result: listing the
newest snapshot, fetching it, collapsing it to one row per open contract and
overwriting the partition atomically are all the binary's, and a non-zero exit
SHALL fail the run.

#### Scenario: A refresh writes the current partition

- **WHEN** the live public-contracts asset is materialised
- **THEN** the corpus binary is invoked with the live build operation for the
  `public-contracts-live` dataset and the configured sink root
- **AND** the partition the run wrote is reported from the binary's own status
  output, not from a path the code location composed

#### Scenario: A failed build fails the run

- **WHEN** the corpus binary exits non-zero for a live build
- **THEN** the materialisation fails, and no partition state is inferred or
  repaired from Python

### Requirement: The live tree has no partition matrix

The live asset SHALL be non-partitioned. The dataset has no Silver tier, no
`year=/month=/day=` tree and no history — every run targets the same mutable
partition and the newest write wins — so the code location SHALL NOT define a
partition matrix for it, and SHALL NOT resolve or hardcode a start date for it.

#### Scenario: No partitions are defined

- **WHEN** the live public-contracts asset is inspected
- **THEN** it carries no partitions definition

### Requirement: Cadence is a fixed schedule, not an availability sensor

Refresh SHALL be driven by a fixed-cadence schedule rather than by the
availability sensing this repository uses for day-partitioned datasets. A
current-overwrite tree exposes no per-date availability to diff — the only
question is "what is newest now" — and upstream publishes roughly one snapshot
every thirty minutes. The schedule SHALL match that upstream rhythm and SHALL be
default-stopped, so enabling it is an operator decision like its sibling live
datasets.

#### Scenario: The schedule matches the upstream rhythm

- **WHEN** the live public-contracts schedule is inspected
- **THEN** it targets the live public-contracts asset on a half-hourly cadence
- **AND** it is default-stopped

### Requirement: The refresh runs under the EVE Ref politeness bound

The live asset SHALL run in the concurrency pool that bounds parallel transfers
to EVE Ref, so its cadence can never push the total fan-out past the courtesy
cap alongside the day-partitioned fetches. It SHALL NOT join a memory-bearing
pool: the build holds one snapshot archive of a few mebibytes, and taking a
scarce memory slot would starve the wide-window Gold backfills for no reason.

#### Scenario: The asset is bounded by the download pool

- **WHEN** the live public-contracts asset is inspected
- **THEN** its pool is the EVE Ref download politeness pool
- **AND** no new pool is declared for it

### Requirement: The materialisation records the payload's own scrape instant

The materialisation SHALL record, from the binary's status output, the dataset,
the tier and partition it wrote, and the freshness of the snapshot it collapsed
— including the payload's own scrape instant, which is authoritative over the
snapshot filename because the filename's seconds field drifts from the scrape.
Fields the binary does not report for a given run SHALL be omitted rather than
defaulted.

#### Scenario: Freshness is carried from the status output

- **WHEN** a live public-contracts refresh succeeds
- **THEN** the materialisation metadata identifies the dataset, the Gold tier and
  the `current` partition
- **AND** it carries the snapshot's scrape instant, the snapshot file it came
  from, the snapshot's date and the row count the binary reported

#### Scenario: A field the binary omits is not invented

- **WHEN** the binary's status output omits one of those freshness fields
- **THEN** the metadata omits that field, and the materialisation still succeeds

### Requirement: The live tree is not queried for run-state

The code location SHALL NOT query the corpus run-state for the live tree. A live
build registers no run-state row — the snapshot file is the traceable provenance
— so a lookup would match nothing and warn on every run. The facts a run-state
read would carry are already in the status output.

#### Scenario: No run-state query is issued

- **WHEN** the live public-contracts asset is materialised
- **THEN** no corpus run-state query is issued for it
