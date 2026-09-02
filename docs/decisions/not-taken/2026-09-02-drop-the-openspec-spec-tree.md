# Drop `openspec/specs/`, as corpus did

## What it would have been

Corpus's shape, adopted here for symmetry: the `row` schema shrinks to
proposal → tasks, no spec delta, and `openspec/specs/` is deleted. Its argument
travels well — a prose tree drifts against the artefacts that actually enforce a
contract, and nothing tells you when it has. One fewer tree to write per row,
one fewer thing to keep true, and the two repositories would then work a change
identically, which is worth something on its own when the same person moves
between them in an afternoon.

## Why not

The thing this repository guarantees is not enforced anywhere else. Corpus can
drop its specs because four artefacts carry the contract and each one is
load-bearing: the `datasets/*.yaml` the binary validates at runtime, the JSON
Schema export, the golden fixtures and the ADRs. Here the equivalent statements
— *this sensor proposes only partitions the run-state reports missing*, *this
asset records `rows`, `retention_class` and `parquet_sha256`*, *Silver's
absence branch is per dataset and reachable* — live in Python that no schema
validates and in ADRs this repo has exactly one of. A test pins the behaviour it
was written for; nothing pins the intent, and the eleven capabilities under
`openspec/specs/` are the only place it is written down as a requirement rather
than as an implementation.

The drift argument is real but it lands differently here. A spec that goes stale
against `defs/` is caught by the row that next touches that capability, because
the `row` schema makes it write the delta before the code. That is not enforcement,
but it is a scheduled read, and a scheduled read is what corpus's four artefacts
buy with validation instead.

## What would change our mind

The specs going stale twice against `defs/` without a row noticing — that would
mean the scheduled read does not happen and the tree is costing writes for
nothing. The other trigger is this repo growing the enforcing artefact it lacks:
a machine-checked description of what each asset records and each sensor
proposes. Then the tree is duplicating something validated, and corpus's
argument applies here unchanged.
