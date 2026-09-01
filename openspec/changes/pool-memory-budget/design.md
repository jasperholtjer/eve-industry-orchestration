## Context

See proposal.md — Why. Two facts shape the approach.

**The declared pools are exactly four.** `everef_download` (politeness, not
memory), `heavy`, `market_orders`, `news_embed`. `_LIVE_POOL` in
`industry_cost_indices_live.py:36` and `market_orders_live.py:35` resolves to
`"everef_download"`, so there is no fifth pool hiding behind a different
constant name.

**`CORPUS_PARSE_CONCURRENCY` needs no Python change to reach the binary.**
`CorpusResource._env()` builds `{**os.environ, ...}`
(`defs/corpus_resource.py:69`), so a variable set in the systemd unit is
inherited by the `corpus` subprocess exactly the way `RAYON_NUM_THREADS`
already is. The seam is the unit file, not the resource.

## Goals / Non-Goals

**Goals:**

- One readable place that answers "what can this box peak at".
- A failing test when a new pool enters the deployment unaccounted.
- A market-orders Silver peak that does not move when the host is resized.

**Non-Goals:**

- Changing any pool limit, `max_concurrent_runs`, or any asset's `pool=`.
- Sizing the LXC. No measurement of the current configuration exists yet.
- Automating the cross-check between `redeploy.sh`'s override list and the
  declared pools. The pinned-set test catches a new pool before deploy; making
  the shell script derive its list from `defs/` is a different row.

## Decisions

**The arithmetic lives in `deploy/dagster.yaml`, and only there.** It has to be
correct for the box to run at all, which is the property a source of truth
needs. `CLAUDE.md` and `ROADMAP.md` keep the invariant — membership by measured
peak, every memory-bearing pool counts against one box budget — and point at
the file. *Alternative considered:* replace `CLAUDE.md`'s stale formula with a
correct, richer one. Rejected: the root cause is not a wrong number but four
copies of a number, and a better fourth copy still drifts.

**The pinned-set test discovers pools from the loaded definitions**, not from a
hand-written list of imported asset symbols. `tests/test_market_history.py:282`
uses direct imports to pin pool names *against each other*, which is right for
that job. It is wrong for this one: a hand-written list cannot catch a pool
declared in a module nobody remembered to add, and that is exactly how
`news_embed` arrived. The test goes in its own module,
`tests/test_concurrency_pools.py` — it is about the code location as a whole,
not about market-history. *Alternative considered:* extend the existing test in
place. Rejected for both reasons above.

**`CORPUS_PARSE_CONCURRENCY=6`, matching `RAYON_NUM_THREADS=6`.** In corpus,
`parse_window()` (`ingestor-market-orders/src/parse.rs:144`) returns
`available_parallelism` clamped to `[2, 8]`, and that window is *both* the
resident-snapshot cap and the `chunks()` batch size for a `par_iter` over the
rayon pool (`parse.rs:133`). On the 8-core LXC the window is therefore 8 while
only 6 threads parse: each batch runs a 6-wide round then a 2-wide one, and two
extra snapshots sit resident for no throughput. Aligning the window to the
thread count gives fully-occupied batches and a strictly lower peak.
*Alternatives considered:* pin 8, preserving today's behaviour exactly — honest,
but keeps two idle resident snapshots and the poor batch tail; pin 4, lowering
the peak further — rejected, it throttles the heaviest Silver below its own
thread pool.

**No ADR.** This row exists because one decision was written down in four
places. Recording it a fifth time in `docs/adr/` would recreate the failure the
row is closing. The invariant is in `CLAUDE.md`, the arithmetic in
`deploy/dagster.yaml`, the mechanism in the test.

**The worst case is stated, not solved.** The rewritten comment says plainly
that 2 x `heavy` + `market_orders` + `news_embed` can exceed the box, and why
that is tolerated: all six OOM kills predate corpus v0.7.0/v0.9.0 and the pool
split, 68 days have run clean since, and `memory.peak` counts since boot so the
11.9 GiB high-water mark belongs to the superseded configuration. It records
the next step — reset `memory.peak` (kernel >= 6.9), let the current
configuration run, read it back — rather than spending RAM on a paper number.

## Risks / Trade-offs

- **The pinned set fails when someone legitimately adds a pool.** → That is the
  point. The assertion message names `deploy/dagster.yaml` as the file to update
  in the same change, so the test converts silent drift into one extra edit.
- **Loading the definitions in a test is heavier than importing two symbols,
  and may need the fake-binary environment the other tests use.** → The suite
  already runs against `tests/fake_corpus.py` with `CORPUS_BINARY_PATH` and
  `CORPUS_SINK_PATH` set; reuse the existing fixtures. If loading proves to
  need NAS or network, fall back to walking the `defs` package for `pool=`
  declarations — still discovery, still catches an unlisted module.
- **`CORPUS_PARSE_CONCURRENCY=6` changes behaviour on a box that has run 68
  days clean.** → It lowers residency (8 → 6) and improves batch occupancy; it
  cannot raise the peak. The change is in the safe direction and is reversible
  by editing one line in each unit.
- **`redeploy.sh` still hardcodes which pools need a limit-1 override.** → Its
  comment is reconciled with the rewritten arithmetic in this row; the pinned
  set catches a new pool at test time, which is before the script would need
  the extra call.
