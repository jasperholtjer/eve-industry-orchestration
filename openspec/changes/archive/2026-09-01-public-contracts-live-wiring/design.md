## Context

See proposal.md — Why. The shape is settled before this row starts: three live
datasets already run this lifecycle here (`market-orders-live`,
`market-prices-live`, `industry-cost-indices-live`), each one module, one
non-partitioned asset and one fixed schedule. What is left to decide is where
this row stops, because the mould it copies has two small gaps and the dataset
it wires has two siblings that are not ready.

## Goals / Non-Goals

**Goals:**

- Land the fourth live dataset on the existing mould with no new mechanism.
- Record the freshness the binary actually reports for this shape, including the
  payload's own scrape instant.

**Non-Goals:**

- The history half of the family. Corpus `public-contracts-silver` and
  `public-contracts-gold` are `todo`, silver blocked on
  `everef-fetch-completeness-guards` and on an open corpus question about
  storing four Silver tables in one partition. No asset, no sensor and no
  `config.py` shape for either.
- Retrofitting the sibling live assets (see Decisions).
- Enabling the schedule on the LXC. The default-stopped schedule and the deploy
  gate are separate facts and both hold.

## Decisions

**One module per live dataset, not a shared factory.** The three existing live
modules duplicate perhaps fifteen lines each and differ in what they record and
why — the prices asset carries `source`/`snapshot_at` from an ESI response, the
orderbook asset carries a snapshot filename. A factory would have to be
parameterised on exactly the part that carries each dataset's reasoning, and the
comment explaining why a given asset skips `partition_metadata` is worth more
than the duplication it costs. Mirror `market_orders_live.py`.

**`snapshot_at` is copied here and not backfilled into the siblings.**
`market_orders_live_gold` copies only `snapshot_file`, `date` and `rows`,
although the binary prints `snapshot_at` for every live shape. Adding it there
is a one-word change to another dataset's recorded metadata, which is a second
row's worth of justification (does the orderbook shape publish the column at
all? its own test pins the keys it records). This row records what its own
dataset's status output carries and leaves the siblings alone. Alternative
considered and rejected: fix all three now — it widens the row past one
capability for a field nobody has asked those trees for.

**The status keys are read defensively, not asserted.** The metadata loop copies
the freshness keys that are present and omits the ones that are not, mirroring
the sibling. `snapshot_at` is documented in corpus as "absent for a shape that
publishes no `snapshot_at` column", and a shim that raises on a field the binary
chose not to print would fail a run the binary passed — the same advisory rule
the run-state enrichment already follows.

**No run-state read, and the reason is recorded in place.** `corpus live build`
is never handed the state DB, so it registers no `partitions` row; a
`state query` would match nothing and warn every thirty minutes. The sibling
already carries that paragraph and a test pins it, so this module carries both
too rather than leaving the omission to look like an oversight.

**ROADMAP.md's confirmed-CLI-surface list gains `corpus live build`.** The list
omits it although three assets already shell it, so the contract for this row's
only corpus call currently exists nowhere but in a sibling's docstring. One line,
in the adoption commit, because this row is the fourth caller and the gap is now
load-bearing rather than cosmetic.

## Risks / Trade-offs

- **The schedule fires against a binary that cannot build this dataset.** The
  LXC installs corpus from a GitHub Release and the newest tag predates the
  dataset, so an enabled schedule would fail every thirty minutes until a
  release lands. → The schedule is default-stopped, exactly as its siblings are,
  so enabling it is a deliberate act after the redeploy. The same gap already
  holds for the whole sovereignty family.
- **A half-hourly cadence against an upstream that publishes ~47 a day will
  sometimes re-fetch a snapshot already collapsed.** → That is the lifecycle: the
  write is an atomic overwrite of one partition and the newest write wins, so a
  redundant run costs one 4.3–6.2 MiB transfer under the politeness pool and
  leaves the tree identical.
- **The fake binary's live handler grows a third per-dataset branch**, and the
  branching is on dataset name. → It already branches for `market-prices-live`
  and the comment there says why; a fourth dataset would be the point to
  generalise, not this one.
