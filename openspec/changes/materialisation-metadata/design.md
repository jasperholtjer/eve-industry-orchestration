## Context

See proposal.md — Why. Two facts shape the approach.

First, the shape at the call site is already uniform. Every asset that
materialises a corpus partition ends with a `dg.MaterializeResult` whose
metadata is some variation of `{"dataset": ..., "tier": ..., "partition": ...}`
— roughly thirty such sites across fourteen modules under `defs/`. They differ
in whether the dict is written inline or built into a local `metadata` variable
first, and in the extra keys some add, but the identifying triple is always
there and is always the same triple that keys the run-state `partitions` table.

Second, the read already exists. `CorpusResource.state_query(sql)` shells
`corpus state query --sql ... --format json` and returns parsed rows. Nothing
new has to be built to reach `rows`, `retention_class` and `parquet_sha256`.

The constraint that decides everything else is the storage boundary: this
process may not construct a path into the partition layout. That rules out the
literal reading of the roadmap goal — opening `_INDEX.json` — and leaves the
run-state table, which happens to carry the two fields the goal names.

## Goals / Non-Goals

**Goals:**

- One place that turns `(dataset, tier, partition_key)` into a metadata
  mapping, so thirty call sites gain one line each rather than a query each.
- Enrichment that cannot fail a materialisation corpus already reported as
  successful.
- A fake-binary seam that can answer the new query, so the enriched metadata is
  covered by the existing suite without a Rust build or the NAS.

**Non-Goals:**

- Any `_INDEX.json`-only field (`window_coverage`, `run_id`,
  `dependency_fingerprint`, `generation_rev`). They are not run-state columns;
  wanting one is a corpus row.
- Sites that do not describe a partition corpus just wrote: the serving-load
  assets in `serving.py`, which load into Postgres and already report their own
  `{"action", "rows"}`, and the `{"built": False}` early returns that record a
  build deliberately skipped.
- Any change to what is materialised or when.

## Decisions

**Run-state, not `_INDEX.json`, and not the run's stdout.**

Three sources could carry these facts. `_INDEX.json` is the one the roadmap
names, and it is the richest, but reading it means building
`<sink>/<tier>/<dataset>/year=/month=/day=/_INDEX.json` in Python — the path
layout the storage boundary reserves to corpus. Rejected on the invariant, not
on convenience.

`run()`'s already-parsed status line is the cheapest: the subprocess output is
scanned anyway, so any field corpus printed there is free. Rejected as the
primary source because it binds the metadata to whatever the binary happens to
print for each subcommand, which differs per dataset and is not part of the CLI
contract this repo relies on. A stable, queryable table beats a log line.

`corpus state query` is chosen: it is a documented read-only surface, the
resource method already exists, the three fields are columns, and the query is
keyed on exactly the triple the call site already holds. The cost is one extra
subprocess per materialisation — a SQLite `SELECT` against a local file,
negligible beside the ingest that just ran.

**One resource method, returning a mapping ready to merge.**

`CorpusResource` gains a single method taking dataset, tier and partition key
and returning a `dict[str, Any]` of the facts it found — empty when it found
none. Call sites merge it over their existing dict rather than replacing it, so
every site keeps its identifying fields and its dataset-specific extras, and a
site is one line longer than it was.

The alternative — a free function in a new `defs/metadata.py` taking the
resource — was rejected because the SQL belongs next to the other SQL. Every
other run-state query in this repo is already phrased at a `state_query` call;
a second home for run-state SQL is the copy that drifts.

**Failure is swallowed, and says so in the log.**

The method catches the failure modes of the underlying call — non-zero exit,
timeout, output that will not parse, no matching row — logs at warning, and
returns an empty mapping. A materialisation that corpus reported as successful
must not then fail because a cosmetic read did not work. This is the one place
where swallowing is right, and it is bounded to this method: nothing else in
this repo treats a corpus failure as advisory.

**Partition key is passed, never derived.**

The call site already holds the key corpus was invoked with — `date`,
`context.partition_key`, a build number, `latest`. It is passed through. The
method does not compute, normalise or reformat it, which keeps
"config is the source of truth" intact: a key that is wrong is wrong at the
invocation, not silently repaired at the record.

## Risks / Trade-offs

- **One extra subprocess per materialisation.** → It is a local SQLite read
  against a file the binary just wrote, against ingest runs measured in
  minutes. It is added after the work, never on the sensor path, so it cannot
  slow a tick or queue a run.
- **A swallowed error hides a broken run-state.** → It is logged at warning
  with the dataset, tier and key, so a systematically empty enrichment is
  visible in the run log rather than silent. Making it fatal would trade a
  cosmetic gap for a failed pipeline, which is the wrong way round.
- **Thirty edited call sites is thirty chances to touch the wrong line.** →
  The edit is additive and uniform, `git diff` per module is small, and the
  existing suite already asserts the identifying fields at many of these sites;
  a site that lost one fails a test that exists today.
- **The fake binary drifts from the real one.** → Already true of every
  `state query` branch in `tests/fake_corpus.py`; this change extends that seam
  in the shape it already has rather than inventing a second one.
