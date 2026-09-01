---
status: open
row: gold-asset-wiring
---

# Is market-orders Silver still meant to share the `heavy` memory budget?

## Why this is blocked

Row `gold-asset-wiring` added a test pinning that every asset sharing the
`heavy` pool names the same literal, because a drifted name silently gives an
asset its own pool and doubles the real memory ceiling. Writing it surfaced a
disagreement about who is in `heavy`. `CLAUDE.md` (lines 55-57) says
`market-orders` Silver joins it; `defs/market_orders.py:79` reads
`_SILVER_POOL = "market_orders"`, and `deploy/dagster.yaml:32` agrees with the
code, declaring that pool at limit 1 for CPU.

The membership half is settled: the split was deliberate. `fc594e2` moved Silver
out on observed evidence — it is the only rayon-parsing ingestor, so one run
saturates every core and a second only oversubscribes them (loadavg `r` ~9 on
4 cores) — and simply never touched `CLAUDE.md`. What is not settled is whether
the *budget* still adds up, because the pools do not know about each other.

This did not block the row, which is merged. It is filed because the invariant
it touches is `CLAUDE.md`'s, and no session should quietly rewrite that.

## What the arithmetic actually is

This was first written as a two-pool problem. There are three memory-bearing
pools:

| Pool | Limit | Per-run peak | Members |
|---|---|---|---|
| `heavy` | 2 | ~3-4 GiB | market-history Gold, market-orders Gold, killmails Gold (provisional) |
| `market_orders` | 1 | ~3-4 GiB, **unmeasured** | market-orders Silver |
| `news_embed` | 1 | 4.4 GiB, measured | news / transcripts embed |

`max_concurrent_runs: 4` equals the number of memory-bearing slots exactly
(2 + 1 + 1), so the global cap forbids no combination of them. Worst case on
paper is ~16.4 GiB on a 12 GiB box. `CLAUDE.md`'s formula (`heavy limit ×
~4 GiB`, keeping ~4 GiB headroom) counts one of the three; `deploy/dagster.yaml`
(lines 61-63) counts two. `news_embed` arrived in `103d725` without either being
updated — the same drift the row's new test catches, one level up.

`deploy/dagster.yaml:40` also argues that market-orders Silver's limit-1 pool
"bounds that memory too". It bounds the dataset against *itself*; it says
nothing about overlap with `heavy` or `news_embed`, which is the entire gap.

## The ~3-4 GiB figure is unmeasured, and its premise is superseded

Every other number in `CLAUDE.md` carries a measurement (~90-97 MiB for the
narrow builds). This one does not, and no `/usr/bin/time -v` result for
market-orders Silver exists in either repository. Its stated premise — "streams
~78M rows/day one row-group per snapshot (corpus >= v0.7.0)" — was true at
v0.7.0 and was superseded at **v0.9.0**: `ingestor-market-orders/src/parse.rs`
parses a bounded *window* of snapshots in parallel, `available_parallelism`
clamped to `[2, 8]`, and that window is the resident-snapshot cap.

The LXC has **8 cores** — `CLAUDE.md:68` still prescribes `--cores 4`, which is
wrong — so the window sits at 8, the top of the clamp, chosen by nobody.
`RAYON_NUM_THREADS=6` in the unit files does not bound it: that is the thread
pool, not the residency. `CORPUS_PARSE_CONCURRENCY`, which does bound it, is
unset.

## What the box actually shows (measured 2026-09-01)

LXC 211: 8 cores, 12288 MiB, uptime 72 days (boot ~2026-06-20).

- `memory.peak` 11.908 GiB against a 12 GiB limit — 94 MiB of headroom.
- `memory.events`: `oom_kill 6`, `max 0`, `oom 0`. The zero counters next to six
  kills are the signature of the limit being enforced on the LXC cgroup on the
  host, outside the container's cgroup namespace — which is also why
  `journalctl -k` *inside* the container is silent. Read the host journal.
- All six kills were `corpus` subprocesses under `dagster-daemon.service`
  (`oom_memcg=/lxc/211`), five on Jun 21 (anon-rss 0.85-2.10 GiB) and one on
  Jun 25 (anon-rss 6.58 GiB). `NRestarts=0` — the daemon itself never died.

**The dating is what matters.** corpus v0.7.0 (streaming ingest, cutting the
market-orders Silver peak from ~16 GB) and v0.9.0 both shipped 2026-06-25, and
the `market_orders` pool split (`fc594e2`) landed 2026-06-26. Every kill falls
in or before the era those changes ended. The current architecture has run
**68 days without a single kill**, and the operator attributes the June kills to
NUC experiments.

The same dating disqualifies the peak: `memory.peak` counts since boot, so
11.908 GiB is drawn from that same superseded window. **There is no measurement
of the current configuration.** Its high-water mark is resettable on kernel
>= 6.9 (`echo > /sys/fs/cgroup/memory.peak`); that reset is the cheapest way to
obtain one, far cheaper than synthesising a worst case with an 835 MiB/day
ingest.

## The options

- **`CLAUDE.md` is stale — correct it.** Drop market-orders Silver from the
  `heavy` membership list and fix `--cores 4` -> 8. Matches what is deployed and
  what was deliberately decided.
- **Move market-orders Silver back into `heavy`.** Restores one shared budget,
  but discards the limit-1 CPU bound that `fc594e2` established on observation:
  `heavy` is limit 2, so two rayon-saturating Silver runs could overlap again.
- **Give it a limit-1 pool *and* count it, by lowering `heavy` to 1.** Keeps both
  bounds honest at the cost of serialising the big Gold backfills — exactly what
  the `heavy`-membership pruning was trying to avoid.

## What I would do

The first, with two amendments the original framing missed.

**Do not restate the arithmetic in `CLAUDE.md`.** The root cause here is not a
wrong number but that pool membership, limits and budget live in three places
(`CLAUDE.md` prose, `deploy/dagster.yaml` comments, `defs/*.py` literals) with no
source of truth. Replacing one stale formula with a richer one adds a third copy
to drift. Let `deploy/dagster.yaml` own the arithmetic — it has to be right for
the box to run anyway — and let `CLAUDE.md` keep only the invariant: membership
is by measured peak, every memory-bearing pool counts against one box budget,
and the sum lives in the deploy config. Correct `dagster.yaml:40`'s reasoning
while there.

**Make the residency a chosen constant, not a discovered one.** Set
`CORPUS_PARSE_CONCURRENCY` explicitly in both unit files, beside the existing
`RAYON_NUM_THREADS`. Today market-orders Silver's peak is a function of the
core count, and it changes silently the next time anyone touches
`pct set 211 --cores`. Precedent for compute-tuning env in the unit exists;
it deserves one comment line saying why orchestration sets it.

**And prose is not the mechanism.** `tests/test_market_history.py:282` pins pool
*names* to each other. The class of drift one level up — a new memory-bearing
pool that no budget accounts for — is equally testable: pin the *set* of pools
the assets declare, so a fifth one fails a test and forces the conversation
`news_embed` skipped.

On sizing: the paper worst case still exceeds the box, but 68 clean days say
either the real peaks sit well below ~4 GiB or that overlap never occurs. Reset
`memory.peak`, let the current configuration run, and read it back before
spending anything on RAM.

## Answer
