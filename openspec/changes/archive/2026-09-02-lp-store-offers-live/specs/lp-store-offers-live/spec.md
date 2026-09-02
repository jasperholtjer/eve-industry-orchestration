## ADDED Requirements

### Requirement: One fan-out is one asset

The code location SHALL refresh both live LP-store Gold trees —
`gold/lp-store-offers/current/` and `gold/lp-store-offer-items/current/` — with
a **single** non-partitioned asset invoking the corpus binary's live build
operation once for the `lp-store-offers-live` dataset. It SHALL NOT define one
asset per Gold tree: one build performs one ESI fan-out over every NPC
corporation and writes both trees from it before either is committed, so a
second asset would re-fetch the whole payload and could leave one tree fresh
against a stale other.

#### Scenario: A refresh writes both trees from one invocation

- **WHEN** the live LP-store asset is materialised
- **THEN** the corpus binary is invoked exactly once, with the live build
  operation for the `lp-store-offers-live` dataset and the configured sink root
- **AND** both Gold trees the binary reports are recorded from its own status
  output, not from paths the code location composed

#### Scenario: A failed build fails the run

- **WHEN** the corpus binary exits non-zero for the live build
- **THEN** the materialisation fails, and no partition state is inferred or
  repaired from Python

### Requirement: The live trees have no partition matrix

The live asset SHALL be non-partitioned and SHALL declare no dependencies. The
dataset has no Bronze cache, no Silver tier, no `year=/month=/day=` tree and no
history — every run overwrites the same mutable partitions and the newest write
wins — so the code location SHALL NOT define a partition matrix for it and
SHALL NOT resolve or hardcode a start date for it.

#### Scenario: No partitions are defined

- **WHEN** the live LP-store asset is inspected
- **THEN** it carries no partitions definition

### Requirement: Cadence is a daily schedule, not an availability sensor

Refresh SHALL be driven by a fixed-cadence schedule rather than by availability
sensing: a current-overwrite tree exposes no per-date availability to diff. The
cadence SHALL be daily and SHALL fall after the upstream cache generation rolls
— every store's response expires at the same instant each day, so one run past
that instant fetches one clean generation and a tighter poll would only repeat
the fan-out against an unchanged payload. The schedule SHALL be default-stopped,
so enabling it is an operator decision as with its live siblings.

#### Scenario: The schedule runs once a day past the cache roll

- **WHEN** the live LP-store schedule is inspected
- **THEN** it targets the live LP-store asset on a once-daily cadence falling
  after the upstream expiry instant
- **AND** it is default-stopped

### Requirement: The refresh joins no concurrency pool

The live asset SHALL NOT declare a concurrency pool. The fetch is ESI, so it
shares no endpoint with the EVE Ref transfers and cannot starve them through
the download politeness pool; the payload is a few mebibytes of JSON, so it is
not memory-bearing and taking a scarce memory slot would starve the wide-window
Gold backfills for no reason. It SHALL be bounded by the global concurrency cap
alone, and no pool declaration or memory budget SHALL be added for it.

#### Scenario: The asset declares no pool

- **WHEN** the live LP-store asset is inspected
- **THEN** it declares no pool
- **AND** the set of declared concurrency pools is unchanged

### Requirement: The materialisation records a row count per Gold tree

The materialisation SHALL record, from the binary's status output, the dataset,
the tier, the `current` partition, the snapshot instant and the source, and a
row count **for each Gold tree the run wrote**, keyed on the derivative name the
binary reports. The status output for this dataset is multi-partition, unlike
every other live dataset's, so a single top-level row count SHALL NOT be
assumed. Fields the binary does not report for a given run SHALL be omitted
rather than defaulted.

#### Scenario: Both trees' row counts are carried

- **WHEN** a live LP-store refresh succeeds
- **THEN** the materialisation metadata identifies the dataset, the Gold tier
  and the `current` partition
- **AND** it carries one row count per derivative the status output lists

#### Scenario: A field the binary omits is not invented

- **WHEN** the binary's status output omits one of those fields
- **THEN** the metadata omits that field, and the materialisation still succeeds

### Requirement: The live trees are neither verified nor queried for run-state

The code location SHALL NOT query the corpus run-state for the live trees and
SHALL NOT invoke the binary's verify operation on them. A live build registers
no run-state row, so a lookup would match nothing and warn on every run; verify
resolves a day-partitioned path, which these non-partitioned `current/` trees do
not have. The binary's non-zero exit is the failure signal, and the facts a
run-state read would carry are already in the status output.

#### Scenario: No run-state query and no verify

- **WHEN** the live LP-store asset is materialised
- **THEN** no corpus run-state query is issued for it
- **AND** no verify operation is invoked for it
