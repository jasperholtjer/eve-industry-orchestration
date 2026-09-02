## Why

Implements roadmap row `lp-store-offers-live`. Corpus landed the dataset on
2026-09-02 (ADR-0070, `datasets/lp-store-offers-live.yaml` on `develop`), so
the dependency `corpus:lp-store-offers-live` is done and this code location has
no asset for it — the two Gold trees are never refreshed. The named consumers
are `predict` and `serving`: what an LP payout is worth is an ISK/LP number
only if the offers behind it are current.

The row's `notes:` in `roadmap.yaml` predate the landing and say three Gold
trees (`lp-npc-corps`, `lp-store-offers`, `lp-store-offer-items`). What shipped
is **two** — the corporation list is the fan-out root, not a derivative — and
the status object is multi-partition rather than the single-partition
`market-prices-live` shape. This change is written against what landed; the
adoption commit corrects the row.

## What Changes

- A new non-partitioned Gold asset `lp_store_offers_live_gold` in
  `defs/lp_store_offers_live.py` that shells `corpus live build --dataset
  lp-store-offers-live --sink-path <sink>` and records the run. Note the
  binary's shape: `--sink-path` is an option of `live build`, not a global
  flag, which is what the sibling live assets already pass.
- **One asset, not two.** One binary call performs one ESI fan-out — 284
  requests — and overwrites both `gold/lp-store-offers/current/` and
  `gold/lp-store-offer-items/current/` before either is committed. Two assets
  would mean two processes re-fetching the same payload, and would let a
  reader see one tree fresh against a stale other.
- A fixed-cadence `dg.ScheduleDefinition` at `30 11 * * *`, default-stopped,
  beside its live siblings in `defs/sensors.py`. Daily, not hourly: measured
  2026-09-02, all 283 stores return `Expires: 11:05:00 UTC` the following day,
  so the caches roll together once a day and a run at 11:30 fetches one clean
  generation. Hourly would be 284 requests an hour against a payload that only
  moves on deployments.
- **No pool**, and nothing added to `deploy/dagster.yaml`. The fetch is ESI,
  not EVE Ref, so it shares no endpoint with the Silver transfers and cannot
  starve them through `everef_download`; ~6 MB of JSON is not memory-bearing,
  so not `heavy` either. The global `max_concurrent_runs` cap alone applies.
- The materialisation records the run's freshness and fan-out — `snapshot_at`,
  `source`, and the corporation counts the binary reports — plus a row count
  **per derivative**, read from the status object's `partitions` list. This is
  the one status shape in this repository that is not single-partition, so the
  asset does not reach for a top-level `rows` key that is not there.
- `tests/fake_corpus.py` gains an `lp-store-offers-live` branch in `_do_live`
  that writes both `current/` trees and prints the multi-partition status, and
  `tests/test_lp_store_offers_live.py` covers the asset, the schedule, the
  absent pool and the run-state read that must not happen.

No compute, parsing or validation lands in Python. There is no partition
matrix and no start date, so `defs/config.py` is untouched. No `corpus verify`
call: it resolves a day-partitioned path and this tree is the non-partitioned
`current/`. No `partition_metadata` enrichment: `corpus live build` writes no
run-state row, so a `state query` would match nothing and warn every run.

The 284-request fan-out is not this repo's problem to solve: the retries, the
four-in-flight bound and the failure rule (a corporation whose GET exhausts its
retries fails the run; a `200 []` is a real empty store, 102 of 283) all live in
the Rust fetch arm, and a short table cannot be published as a success.

## Capabilities

### New Capabilities
- `lp-store-offers-live`: how the two live LP-store Gold trees are refreshed and
  recorded — one asset for one fan-out, the daily cadence and why it is not a
  sensor, the absence of a pool, the per-derivative row counts the
  materialisation carries, and the run-state read and verify call it must not
  perform.

### Modified Capabilities
<!-- None. No existing spec constrains this dataset. -->

## Impact

- New: `src/eve_industry_orchestration/defs/lp_store_offers_live.py`,
  `tests/test_lp_store_offers_live.py`.
- Modified: `src/eve_industry_orchestration/defs/sensors.py` (one schedule),
  `tests/fake_corpus.py` (one status branch), `README.md` and
  `openspec/config.yaml` where they enumerate the live family.
- `deploy/dagster.yaml` untouched: no pool is added and no budget moves, so
  `tests/test_concurrency_pools.py` stays as it is.
- Corpus is read-only and unchanged; `corpus live build` is already the CLI
  surface four sibling datasets use.
- Deployment: materialising this on the LXC waits on a corpus release that
  contains the dataset — `deploy/redeploy.sh` installs the binary from a GitHub
  Release. The row is testable against the fake binary regardless.
