# embed-on-the-box

## Why

Row `embed-on-the-box`: Dagster cannot express "pool X never beside pool Y" —
an asset carries one pool, and a run claims a slot in every pool its ops name.
"One embed, never beside a windowed Gold build" is only sayable by making the
embed assets *be* the `heavy` pool. Retire `news_embed`, join
`news_embeddings_bronze` and `transcripts_embeddings_bronze` to `heavy`, drop
`heavy` from limit 2 to 1, raise `max_concurrent_runs` 4 → 6, and move
`news_daily_schedule`/`transcripts_daily_schedule` an hour apart in the late-
UTC window. `tmp/brief.md` sharpens two things: `heavy`'s per-pool CLI override
does not exist yet (only `default_limit` does; `redeploy.sh` needs a third
`concurrency set heavy 1` call, on the pattern already used for
`market_orders`), and the goal's ~9.2 GiB does not fall out of the current
table without also picking embed's measured 4.4 GiB as `heavy`'s new
per-holder peak — the embed step is now the heaviest thing that can occupy it.
Both are settled here rather than asked: the override is the mechanism
`deploy/dagster.yaml`'s own comment already prescribes for a sub-default limit,
and the peak follows from which holder is heaviest. `everef_download` keeps
`default_limit: 2`; its 2 is EVE Ref politeness fan-out and is not this row's
to touch.

## What Changes

- Retire the `news_embed` pool. `news_embeddings_bronze`
  (`defs/news.py`) and `transcripts_embeddings_bronze` (`defs/transcripts.py`)
  join `heavy` instead.
- **CONTRACT**: `heavy` drops from `default_limit: 2` to a per-pool override of
  1, set via a new `dagster instance concurrency set heavy 1` in
  `redeploy.sh`, mirroring the existing `market_orders` override. Writes
  ADR-0002 — the first pool membership bought for mutual exclusion rather than
  a measured peak.
- Rewrite the memory budget in `deploy/dagster.yaml`: three pools instead of
  four, `heavy`'s per-holder peak restated as embed's measured 4.4 GiB (the
  pool's new ceiling), worst case ~9.2 GiB against the 12 GiB LXC.
- `max_concurrent_runs`: 4 → 6. Documented as the NAS spindle's I/O cap, no
  longer an accidental memory backstop.
- `news_daily_schedule` cron `"0 22 * * *"` → `"10 22 * * *"`;
  `transcripts_daily_schedule` cron `"30 22 * * *"` → `"10 23 * * *"`. Rewrite
  the doc-comment above each: the stagger is now an hour, sized to let one full
  embed generation hold `heavy` without the other schedule queuing mid-run.
- `tests/test_concurrency_pools.py`: drop `"news_embed"` from `EXPECTED_POOLS`.
- `redeploy.sh`: drop the `concurrency set news_embed 1` call, add
  `concurrency set heavy 1`, correct the "four pools" comment to three.
- `market_orders.py`'s comment referencing `news_embed` by name is corrected to
  `heavy` (its own `pool=` assignments are unchanged).

## Capabilities

- `concurrency-pools` (modified): "Membership of a memory-bearing pool is by
  measured peak" gains a second admissible ground — membership bought to
  guarantee mutual exclusion with a pool's other holders, when Dagster can
  express the exclusion no other way — with an ADR required to record it and
  the pool's per-holder peak restated to its heaviest new holder. "One place
  states the box budget" is satisfied by the rewritten table, not changed in
  wording.

## Impact

- `defs/news.py`, `defs/transcripts.py`: `pool=` reassigned `news_embed` →
  `heavy`, no other change to either asset.
- `defs/market_orders.py`: comment only.
- `defs/sensors.py`: two `cron_schedule` strings and their doc-comments.
- `deploy/dagster.yaml`: `default_limit` unchanged at 2 (still covers
  `everef_download`); `max_concurrent_runs` 4 → 6; the memory-budget comment
  block rewritten for three pools.
- `deploy/redeploy.sh`: one `concurrency set` call replaced with another.
- `tests/test_concurrency_pools.py`: `EXPECTED_POOLS` updated.
- `docs/adr/0002-heavy-pool-membership-for-exclusion-not-peak.md`: new.
- Corpus is not touched. The embed step running at all still depends on the
  corpus row named in the answered question
  (`docs/questions/answered/2026-09-02-embed-engine-missing-from-the-deployed-binary.md`)
  — the glibc release asset and the model-dir provisioning — which this row
  does not block on and does not include.
