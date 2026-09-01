# materialisation-metadata Specification

## Purpose

Defines what an asset records about a partition it has just materialised — the
facts corpus measured while writing it, rather than the arguments the asset was
called with — and where those facts are allowed to come from, so that the
materialisation log is an account of what happened instead of an echo of what
was asked.

## Requirements

### Requirement: Recorded metadata describes the partition, not the request

An asset that materialises a partition SHALL record, alongside the identifying
fields it already carries, the facts corpus registered for that partition in
run-state: the number of rows written, the retention class it was written
under, and the sha256 of the parquet file. Where run-state holds a row for the
partition, the record SHALL carry those facts and SHALL NOT consist solely of
values the caller supplied.

#### Scenario: A partition with run-state facts

- **WHEN** an asset materialises a partition and corpus has registered a
  run-state row for that dataset, tier and partition key
- **THEN** the materialisation record carries the row count, the retention
  class and the parquet sha256 taken from that row
- **AND** it still carries the dataset, tier and partition key it already
  carried

#### Scenario: Row count of zero is a fact, not an absence

- **WHEN** the registered run-state row reports zero rows
- **THEN** the materialisation record reports zero rows
- **AND** it is distinguishable from a partition whose facts could not be read

### Requirement: The partition is identified in run-state's own vocabulary

Run-state identifies a partition by a scheme-prefixed key, which is not the
same string as the orchestrator's partition key. The lookup SHALL address the
partition in the form run-state records it, so that a partition corpus
registered is found. A lookup that silently matches nothing for a partition
that exists is a defect, not an absent row.

#### Scenario: Every scheme corpus writes is addressable

- **WHEN** a partition is registered under any scheme corpus uses — a date, a
  build number, a month, or the single latest snapshot
- **THEN** the lookup for that partition finds its row
- **AND** the facts recorded are that row's

#### Scenario: A scheme mismatch is not mistaken for an absent row

- **WHEN** an asset's partition is registered in run-state
- **THEN** its materialisation record carries the run-state facts
- **AND** it does not fall back to the identifying fields alone

### Requirement: Run-state is the only source of recorded facts

The recorded facts SHALL be obtained by querying corpus run-state through the
corpus CLI. The orchestrator SHALL NOT construct a path into the partition
layout, and SHALL NOT open `_INDEX.json`, the parquet file or any other
contract byte to obtain them.

#### Scenario: Facts are read through the binary

- **WHEN** an asset needs the facts for a partition it has materialised
- **THEN** it asks the corpus binary for them by dataset, tier and partition key
- **AND** no filesystem path below the sink root is constructed in the
  orchestrator to answer the question

#### Scenario: A fact outside run-state is not fetched

- **WHEN** a fact about the partition exists only inside `_INDEX.json` and has
  no corresponding run-state field
- **THEN** it is not recorded
- **AND** obtaining it is treated as needing new corpus surface, not a
  filesystem read from the orchestrator

### Requirement: Enrichment never fails a successful run

Recording the facts SHALL be advisory. When run-state has no row for the
partition, or the query fails, or its result cannot be interpreted, the
materialisation SHALL still succeed and SHALL still record the identifying
fields.

#### Scenario: No run-state row for the partition

- **WHEN** corpus reports the materialisation succeeded but run-state holds no
  row for that dataset, tier and partition key
- **THEN** the asset materialises successfully
- **AND** the record carries the identifying fields without the run-state facts

#### Scenario: The query itself fails

- **WHEN** the run-state query exits non-zero, times out, or returns output
  that cannot be interpreted
- **THEN** the asset materialises successfully
- **AND** the failure is visible in the run's logs rather than raised

### Requirement: Enrichment changes no scheduling behaviour

Recording these facts SHALL NOT alter which partitions are materialised or how
they are scheduled: no partition definition, sensor decision, schedule or
concurrency pool membership depends on it.

#### Scenario: The same partitions are still requested

- **WHEN** metadata enrichment is in place
- **THEN** sensors request the same partitions they requested before
- **AND** the set of declared concurrency pools is unchanged
- **AND** no asset gains or loses a pool because of it
