## Why

Implements roadmap row `materialisation-metadata`. This repository exists to
decide which partitions are materialised and to **record that they were**, but
the recording half is a placeholder: every asset yields a `MaterializeResult`
whose metadata restates what the caller already knew — the dataset name, the
tier and the partition key it was asked for. Two `# TODO: enrich metadata from
_INDEX.json / corpus state query` markers sit directly above those calls in
`market_history.py`. Nothing in the Dagster UI or the materialisation log
distinguishes a partition that wrote nine million rows from one that wrote
none, so the overview this repo is supposed to give does not exist.

Corpus already records those facts. The run-state `partitions` table carries
`rows`, `retention_class` and `parquet_sha256` per `(dataset, tier,
partition_key)`, and `corpus state query --format json` already reads it
through `CorpusResource.state_query`. The row is to spend that.

## What Changes

- Add one read on `CorpusResource` that, given a dataset, tier and partition
  key, returns the run-state facts corpus recorded for that partition — `rows`,
  `retention_class`, `parquet_sha256` — as a metadata mapping. It is a single
  `corpus state query --format json` call; no new corpus subcommand, no new
  resource, no parsing of partition bytes.
- Have every asset in `src/eve_industry_orchestration/defs/` that currently
  yields a static `MaterializeResult` merge that mapping over its existing
  static fields, after the `run()`/`verify` call that produced the partition.
- Make a missing or unreadable run-state row non-fatal: the asset still
  materialises and still records the static fields. Metadata enrichment never
  fails a run that corpus reported as successful.
- Extend the fake `corpus` binary (`tests/fake_corpus.py`) so its
  `state query` branch answers the new SQL from per-partition fixture state,
  which is what lets the fake-binary suite exercise the enriched metadata.
- Remove the two `# TODO` markers the row retires.

**Not** in scope, deliberately: Python opening `_INDEX.json`. The roadmap goal
names that file, but reading it from here means constructing
`<sink>/<tier>/<dataset>/year=/month=/day=/_INDEX.json`, which is precisely the
path layout the storage boundary reserves to corpus. The two fields the goal
asks for by name — `rows` and `retention_class` — are columns of the run-state
table, so the sanctioned read returns them without touching the contract path.
The `_INDEX.json`-only fields (`window_coverage`, `run_id`,
`dependency_fingerprint`, `generation_rev`) are out of reach of `state query`
and stay out of this change; wanting one of them is a corpus row, not a Python
file open.

## Capabilities

### New Capabilities
- `materialisation-metadata`: what an asset records about a partition it
  materialised, where those values come from, and what happens when they cannot
  be read.

### Modified Capabilities

None. No existing requirement changes: the assets shell out to the same
subcommands, on the same partitions, under the same pools.

## Impact

- `src/eve_industry_orchestration/defs/corpus_resource.py` — one new read method.
- `src/eve_industry_orchestration/defs/` — every module with a static
  `MaterializeResult`: `market_history.py`, `killmails.py`, `market_orders.py`,
  `mer.py`, `news.py`, `sde.py`, `structures.py`, `system_jumps.py`,
  `system_kills.py`, `transcripts.py`.
- `tests/fake_corpus.py` — per-partition fixture state and a `state query`
  branch that answers from it; new tests over the enriched metadata.
- No partition definition, sensor, schedule or pool changes. The declared pool
  set pinned by `tests/test_concurrency_pools.py` is untouched: this is
  metadata enrichment on an existing call, not a new memory profile.
- No new dependency. No compute, no validation and no parquet in Python.
