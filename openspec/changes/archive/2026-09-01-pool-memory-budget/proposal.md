## Why

Implements roadmap row `pool-memory-budget`.

Four concurrency pools are declared across `defs/`, and three of them carry
memory: `heavy` (limit 2, ~3-4 GiB per holder), `market_orders` (limit 1,
~3-4 GiB) and `news_embed` (limit 1, 4.4 GiB measured). Their slot counts sum
to 2 + 1 + 1 = 4, which is exactly `max_concurrent_runs: 4`, so the global cap
forbids no combination of them and the paper worst case is ~16.4 GiB on a
12 GiB LXC.

The budget that is supposed to prevent this is stated in four places that have
already drifted apart — `CLAUDE.md` counts one pool, `deploy/dagster.yaml`
counts two, `deploy/redeploy.sh` names the two that need an override, and
`ROADMAP.md` still describes the original two-pool decision. Nothing fails when
a new memory-bearing pool arrives unaccounted, which is precisely how
`news_embed` arrived in `103d725`.

## What Changes

- `deploy/dagster.yaml` becomes the **single source of the arithmetic**: which
  pools exist, which of them carry memory, what one holder of each peaks at,
  what the worst case sums to against the box, and why that exposure is on
  paper rather than observed.
- The reasoning at `deploy/dagster.yaml:40` is corrected. A limit-1 CPU pool
  bounds `market-orders` Silver against *itself*; it says nothing about overlap
  with `heavy` or `news_embed`, which is the entire gap.
- `CLAUDE.md`'s concurrency bullet shrinks to the invariant plus a pointer, and
  the stale two-pool sentence in `ROADMAP.md` does the same. Neither restates
  the numbers; a third and fourth copy of an arithmetic is what produced this
  row.
- A new test pins the **set** of pool names the code location declares. A fifth
  pool fails it until the budget in `deploy/dagster.yaml` accounts for the new
  member. The pools are discovered from the loaded definitions rather than from
  a hand-written import list, because a hand-written list cannot catch the one
  thing this row exists to catch: a pool declared in a module nobody remembered
  to add.
- Both systemd units set `CORPUS_PARSE_CONCURRENCY` explicitly. corpus v0.9.0
  makes `market-orders` Silver's resident-snapshot window
  `available_parallelism` clamped to `[2, 8]`, so on the 8-core LXC its peak is
  a function of the core count and changes silently the next time anyone runs
  `pct set 211 --cores`. `RAYON_NUM_THREADS` does not bound it.

**No limit changes.** No pool limit, no `max_concurrent_runs`, and no asset's
`pool=` value moves. The paper worst case stays over the box deliberately: all
six OOM kills on the LXC predate corpus v0.7.0/v0.9.0 and the pool split, the
current configuration has run 68 days clean, and `memory.peak` counts since
boot so no measurement of the current configuration exists. Sizing waits on
that measurement; this row makes the budget honest and testable, not smaller.

No asset joins `heavy`, so no new measured peak RSS is owed. No asset changes
what it shells out to or what it records — `corpus_resource.run()` already
passes the process environment through to the subprocess
(`corpus_resource.py:69`, `{**os.environ, ...}`), so the new env var reaches
the binary with no Python change. No compute, parsing or validation moves into
Python.

## Capabilities

### New Capabilities

- `concurrency-pools`: the declared set of Dagster concurrency pools, which of
  them carry memory, and where the box budget that bounds them is stated.

### Modified Capabilities

None.

## Impact

- `deploy/dagster.yaml` — header comment rewritten; config values unchanged.
- `deploy/dagster-daemon.service`, `deploy/dagster-webserver.service` — one new
  environment line each, mirrored, beside `RAYON_NUM_THREADS`.
- `deploy/redeploy.sh` — the comment naming which pools sit below
  `default_limit` is reconciled with the rewritten arithmetic.
- `CLAUDE.md`, `ROADMAP.md` — prose reduced to invariant plus pointer.
- `tests/` — one new test module pinning the declared pool set.
- No `src/` behaviour change, no partition, sensor or schedule touched, no
  corpus CLI surface involved.
