# public-contracts-gold Specification

## Purpose

Defines how the four public-contracts Gold derivatives are offered, built and
recorded: how each one's partition start and Silver reach-back are derived from
the corpus dataset configuration, which corpus operations a Gold asset invokes
and in what order, and what each derivative's readiness sensor may decide on
its own.

## ADDED Requirements

### Requirement: Every public-contracts Gold derivative is built by name

The orchestrator SHALL build each public-contracts Gold derivative — the
contract fold, the item fold, the item price series and the courier rate series
— through its own build invocation naming that derivative, and SHALL NOT build
the dataset's Gold trees with a single dataset-wide invocation. Each derivative
SHALL be a separate materialisable unit with its own partition matrix.

For a given date the orchestrator SHALL invoke the corpus Gold build for that
derivative and date and, only after it has succeeded and reported that it wrote
a partition, invoke the corpus contract verification for the Gold tier of the
same derivative and date. A failing build or a failing verification SHALL fail
the materialisation.

The orchestrator SHALL NOT open, parse or validate the built payload, and SHALL
NOT evaluate any coverage or completeness condition before invoking the build.
Whether a date can be built is the build's own answer.

#### Scenario: A derivative is built and then verified

- **WHEN** a public-contracts Gold asset materialises a date and the build
  succeeds reporting a written partition
- **THEN** contract verification for the Gold tier of that derivative and date
  is invoked, and the materialisation succeeds only if it also succeeds

#### Scenario: A failing build is not verified

- **WHEN** the build for a date exits non-zero
- **THEN** verification is not invoked and the materialisation fails

#### Scenario: Four derivatives of one dataset are built separately

- **WHEN** the four derivatives that share the public-contracts dataset are
  materialised for the same date
- **THEN** each is built by an invocation naming only itself, and no build
  produces another derivative's partition

#### Scenario: No completeness check precedes a build

- **WHEN** a public-contracts Gold asset materialises a date
- **THEN** it invokes the build without first inspecting the availability,
  coverage or completeness of that build's inputs

### Requirement: A public-contracts Gold derivative reaches back no days

The orchestrator SHALL derive each public-contracts Gold derivative's partition
start from the served start that derivative declares in the corpus dataset
configuration, and SHALL NOT carry a literal start date for any of them.

Each of the four derivatives folds one day of Silver into one day of Gold and
holds no state across days, so the orchestrator SHALL resolve a Silver
reach-back of zero days for all four: the Silver a derivative needs starts on
the derivative's own served start and no earlier. The orchestrator SHALL NOT
treat a zero reach-back as an absent one — the derivative still anchors the
dataset's Silver partition start.

#### Scenario: Each derivative resolves its own start

- **WHEN** the Gold partition start is resolved for each of the four
  public-contracts derivatives
- **THEN** each returns the served start its own configuration declares

#### Scenario: The Silver start is not pulled earlier by a Gold window

- **WHEN** the Silver partition start is resolved for the public-contracts
  dataset now that it declares Gold derivatives
- **THEN** it is the dataset's declared Silver coverage floor, because no
  derivative reaches back beyond its own served start

### Requirement: An unresolvable Gold shape fails the code location loudly

The orchestrator SHALL reject a Gold derivative whose declared shape it cannot
resolve a reach-back for, rather than assuming a default window. A dataset
configuration naming an unknown shape SHALL raise a partition configuration
error identifying both the derivative and the shape.

The test fixtures that stand in for the corpus dataset configuration SHALL
declare the same Gold derivatives and shapes the real configuration declares
for that dataset, so that a shape the orchestrator cannot resolve is caught by
the suite rather than at code-location load.

#### Scenario: An unknown shape is named in the error

- **WHEN** a dataset configuration declares a Gold derivative whose shape the
  orchestrator has no reach-back rule for
- **THEN** resolution raises a partition configuration error naming the
  derivative and the shape, and no partition start is returned

#### Scenario: A fixture carries the derivatives the real configuration carries

- **WHEN** the suite resolves partition starts for public-contracts from its
  fixture
- **THEN** the fixture declares all four Gold derivatives with the shapes and
  served starts the corpus dataset configuration declares

### Requirement: Public-contracts Gold readiness is decided from the run-state

The orchestrator SHALL propose a public-contracts Gold partition for a date
only when the run-state records that date's public-contracts Silver partition
as built, and SHALL read that status through the corpus run-state rather than
by inspecting the storage tree.

A readiness sensor SHALL propose only dates inside its own derivative's
partition matrix, and SHALL NOT propose a date already materialised for that
derivative.

#### Scenario: A date whose Silver is sealed is proposed

- **WHEN** a readiness sensor ticks and the run-state records the day's Silver
  as built while the derivative's Gold partition for that date is not
- **THEN** the sensor requests that partition

#### Scenario: A date whose Silver is absent is not proposed

- **WHEN** a readiness sensor ticks and the run-state records no built Silver
  partition for a date
- **THEN** the sensor requests nothing for that date

#### Scenario: A date outside the derivative's matrix is not proposed

- **WHEN** the run-state records a built Silver partition for a date earlier
  than the derivative's own Gold partition start
- **THEN** the sensor requests nothing for that date
