---
status: open
row: gold-asset-wiring
---

# Is market-orders Silver still meant to share the `heavy` memory budget?

## Why this is blocked

Row `gold-asset-wiring` added a test pinning that every asset sharing the
`heavy` pool names the same literal, because a drifted name silently gives an
asset its own pool and doubles the real memory ceiling. Writing it surfaced a
three-way disagreement about who is in `heavy`, which the row could not settle
because it turns on a measurement rather than on a reading.

`CLAUDE.md` (lines 55-57) states that `market-orders` Silver joins `heavy` — "it
streams ~78M rows/day one row-group per snapshot (corpus >= v0.7.0) and peaks the
same ~3-4 GiB, the only Silver heavy enough to need a memory bound." The code
disagrees: `defs/market_orders.py:79` reads `_SILVER_POOL = "market_orders"`.
`deploy/dagster.yaml` agrees with the code and gives a reason — line 32 declares
`market_orders` at limit 1 for CPU, and line 40 argues "limit-1 pool bounds that
memory too."

So the code and the deployment agree with each other, and `CLAUDE.md` is the odd
one out. That much is settled. What is not settled is whether the *budget* still
adds up, because the two pools do not know about each other: at `heavy`'s default
limit of 2, two wide-window Gold builds at ~4 GiB can run alongside one
market-orders Silver at ~4 GiB. That is ~12 GiB of simultaneous peaks against the
~8 GiB `CLAUDE.md` budgets and the >= 12 GiB RAM it asks the LXC to have — no
headroom left, where the formula was written to keep ~4 GiB of it.

This did not block the row, which is merged. It is filed because the invariant it
touches is `CLAUDE.md`'s, and no session should quietly rewrite that.

## The options

- **CLAUDE.md is simply stale — correct the sentence.** Drop market-orders Silver
  from the `heavy` membership list and rewrite the RAM formula to account for a
  second, independent memory-bearing pool: peak ≈ `heavy limit × ~4 GiB` +
  `market_orders limit × ~4 GiB`. Cheapest, and it matches what is deployed. It
  concedes that the total ceiling is higher than the current text implies, so the
  >= 12 GiB recommendation probably has to rise to ~16 GiB.
- **Move market-orders Silver back into `heavy`.** Restores one shared budget and
  makes the existing formula true again, but costs the dedicated limit-1 CPU
  bound that `deploy/dagster.yaml:32` says it needs — `heavy` is limit 2, so two
  market-orders Silver runs could then overlap on CPU, which is the thing that
  pool was created to stop.
- **Give market-orders Silver a limit-1 pool *and* count it in the budget** by
  lowering `heavy` to 1 on the current 12 GiB box. Keeps both bounds honest at the
  cost of serialising the big Gold backfills, which is exactly what the
  `heavy`-membership pruning recorded in `CLAUDE.md` was trying to avoid.

## What I would do

The first. The code and the deployment already agree, and both were changed
deliberately — `deploy/dagster.yaml:40` reasons about the memory explicitly
rather than by oversight, so this reads as a decision `CLAUDE.md` was never
updated for, not as drift in the code. The honest fix is to correct the
membership sentence and to make the RAM formula sum both memory-bearing pools
instead of only `heavy`, then re-check the LXC's 12 GiB against the new total.

What I cannot do from here is confirm the ~3-4 GiB figure for market-orders
Silver under corpus >= v0.7.0. If that number is stale too, the arithmetic
changes and so might the answer. `CLAUDE.md` asks for `/usr/bin/time -v` with
`CORPUS_DATASETS_DIR` set before a build's pool membership is changed; that
measurement is the input this question is really waiting on.

## Answer

