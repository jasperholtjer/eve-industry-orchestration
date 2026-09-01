## Why

Implements roadmap row `public-contracts-live-wiring`. Corpus has landed
`public-contracts-live` (ADR-0068 decision 4) — the current-overwrite inventory
twin that collapses the newest EVE Ref public-contract snapshot to one row per
open contract — and this code location has no asset for it, so the tree is never
refreshed. The named consumers of that tree are `serving` and `map` ("what is on
offer, where, now"), and they read a `current/` partition that nothing currently
writes on a cadence.

The history half of the family is deliberately not in scope: corpus
`public-contracts-silver` is blocked on `everef-fetch-completeness-guards` and
on an open corpus question about how one day stores four Silver tables, and
`public-contracts-gold` waits on that. An asset for either would be dead code.

## What Changes

- A new non-partitioned Gold asset `public_contracts_live_gold` in
  `defs/public_contracts_live.py` that shells `corpus live build --dataset
  public-contracts-live --sink-path <sink>` and records the run. It opens no
  parquet, resolves no path and pre-validates nothing: the binary lists the
  newest per-date folder, fetches one `.v2.tar.bz2`, and overwrites
  `gold/public-contracts-live/current/` atomically.
- The asset joins the existing `everef_download` pool — network politeness for
  the single EVE Ref fetch, not memory. **No new pool**, and none is owed a
  measured peak: the build holds one 4.3–6.2 MiB archive, so it does not join
  `heavy` and does not move any pool's budget.
- A fixed-cadence `dg.ScheduleDefinition` at `*/30 * * * *`, default-stopped,
  mirroring `market_orders_live_schedule`. This is the documented departure from
  "sensor over cron": a current-overwrite tree has no per-date availability to
  diff, only "take whatever is newest now", and upstream publishes ~47 snapshots
  a day.
- The materialisation records `snapshot_at` — the payload's own `scrape_start`,
  which the snapshot filename's drifting seconds field does not give — alongside
  `snapshot_file`, `date` and `rows` from the binary's status object.
- `tests/fake_corpus.py` gains a `public-contracts-live` branch in `_do_live`
  emitting that status shape, and a new `tests/test_public_contracts_live.py`
  covers the asset, the schedule and the absence of a run-state query.

No compute, parsing or validation lands in Python. No partition matrix, so
`defs/config.py` is untouched and no start date is introduced here.

## Capabilities

### New Capabilities
- `public-contracts-live`: how the live public-contracts Gold tree is refreshed
  and recorded — the current-overwrite lifecycle expressed as a non-partitioned
  asset, its fixed cadence and why it is not a sensor, the concurrency pool it
  runs in, the freshness fields the materialisation carries, and the run-state
  read it must not perform.

### Modified Capabilities
<!-- None. No existing spec constrains the live datasets. -->

## Impact

- New: `src/eve_industry_orchestration/defs/public_contracts_live.py`,
  `tests/test_public_contracts_live.py`.
- Modified: `src/eve_industry_orchestration/defs/sensors.py` (one schedule),
  `tests/fake_corpus.py` (one status branch), `README.md` and `ROADMAP.md` where
  they enumerate what this code location drives.
- Corpus is read-only and unchanged; the CLI surface used already exists and is
  already exercised by three sibling live datasets.
- Deployment: materialising this on the LXC waits on a corpus release that
  contains the dataset — `deploy/redeploy.sh` installs the binary from a GitHub
  Release, and the latest tag predates both this dataset and the sovereignty
  family. The row is fully testable against the fake binary regardless, as the
  sovereignty rows were.
