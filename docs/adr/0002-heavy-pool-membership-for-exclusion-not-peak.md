# ADR-0002: `heavy` membership may be bought for exclusion, not only for a peak

## Status

Accepted (2026-09-03). Decided by the `embed-on-the-box` row, which answered
[`docs/questions/answered/2026-09-02-embed-engine-missing-from-the-deployed-binary.md`](../questions/answered/2026-09-02-embed-engine-missing-from-the-deployed-binary.md)
— embedding runs on the box, and the pool is merged rather than kept. No prior
ADR governs pool membership; this is the first.

## Context

The rule this repo has run on is that membership of a memory-bearing pool is by
**measured peak**, never by shape: a pool exists to bound a class of runs
against itself, and you join it because your holder's RSS has been measured and
budgeted. The two `corpus enrich embed` steps (`news_embeddings_bronze`,
`transcripts_embeddings_bronze`) held their own limit-1 pool on exactly that
reading — they had a measured peak, so they got a pool sized on it.

That arrangement bounds the wrong thing. Each embed holds the pinned local ONNX
model in-process, and the wide-window `corpus` Gold builds are the other large
resident holders on the box. A pool bounds a class **only against itself**, so
"one embed at a time" and "one windowed Gold build at a time" are two true
statements that together permit one of each, simultaneously, on a 12 GiB LXC.
What the box needs is the statement neither pool makes: *an embed never runs
beside a windowed Gold build.*

Dagster has no way to say it. `OpDefinition.pool` is a single `str | None`, so
an asset carries exactly one pool and there is no expression for pool-against-pool
exclusion. The one near-miss construct is
`concurrency.runs.tag_concurrency_limits` (checked against the installed Dagster
1.13.10), and it is rejected on the same ground `AGENTS.md` already rejects run
tags for scheduling decisions: it keys on **run tags**, which a sensor or a UI
backfill or a manual launch may each set differently or not at all, so it does
not cover every launch path. A pool does, because it gates at the step.

Mutual exclusion between two classes is therefore only sayable one way: the two
classes are *the same pool*, and that pool's limit is 1.

## Decision

**Membership of a memory-bearing pool may be bought to guarantee mutual
exclusion between two asset classes, and not only by a measured peak.** Both
embed steps join `heavy`, `heavy` runs at limit 1, and its budgeted per-holder
peak becomes its heaviest possible holder rather than its typical one. The
retired `news_embed` pool is gone.

The pool arithmetic — which pools exist, their limits, each holder's peak and
the worst case against the box — stays where it has always been, in
[`deploy/dagster.yaml`](../../deploy/dagster.yaml), and this record deliberately
copies no figure out of it.

This is the first such membership and is meant to stay singular. The default
reading of the rule is unchanged: a build joins a memory-bearing pool because
its peak was measured with `/usr/bin/time -v`, and "it feels windowed" is still
not a reason. Exclusion is a reason only when the two classes cannot be allowed
to coexist on the box at all, and only when the pool that results is at limit 1
— at any higher limit the membership buys nothing, because the pool then admits
one of each again.

**`market_orders` stays its own limit-1 pool, and that is the boundary of this
decision.** It is bounded for CPU saturation, not for exclusion from `heavy`;
folding it in would park every windowed Gold build behind a multi-hour Silver
run. A pool that exists for a different reason is not evidence that the pools
should merge.

## Consequences

**The price is serialisation.** `heavy` at limit 1 means the windowed Gold
backfills — market-history Gold, market-orders Gold, killmails Gold — that could
previously run two abreast now run one at a time, and an embed generation
occupies that same single slot for its whole run. Backfills get slower. That is
what the exclusion costs, it was bought knowingly, and it is the first thing to
revisit if the box ever grows.

**The limit lives partly outside version control.** `concurrency.pools` has no
per-pool field: the yaml can state only `default_limit`, and any pool that needs
a different limit is set with `dagster instance concurrency set` into the
instance DB by `redeploy.sh`. `default_limit: 1` is chosen precisely so that
`heavy` needs no override — if an instance-DB override is ever lost, the box
falls back to the memory-safe limit everywhere and what degrades is download
speed, not the memory ceiling. The exclusion this ADR buys must be the thing
that survives; convenience is what may go missing.

**The schedules were widened to match.** The two group schedules sit an hour
apart rather than thirty minutes, so one full embed generation can hold the
shared slot without the other schedule's run queuing behind it mid-run.

### Known limits

Two figures behind this decision are not measured, and both are named here so
the next reader does not mistake them for settled.

The embed peak the `heavy` budget is now sized on is a **Windows-workstation
measurement and has never been run on the LXC**. `/usr/bin/time -v` on the first
real embed on the box (it needs `CORPUS_DATASETS_DIR`) is what corrects it, and
the budget table in `deploy/dagster.yaml` is what it corrects.

The global run cap was raised in the same row, on the reasoning that it is the
NAS spindle's I/O cap and never was a memory backstop. **Whether the new number
is the right one for a single-HDD spindle is unmeasured.** It also means the
pooled worst case no longer fills every run slot, so unpooled runs can sit on top
of it — the yaml states that as an open term rather than a closed total, and it
closes only by measuring those builds.

### What this does not include

This ADR settles where the embed step is *scheduled*, not whether it can run.
The deployed binary is a `--no-default-features` musl build with no
`embed-engine`, so every embed run still fails on the box. Fixing that is a
corpus row, **`corpus:release-gnu-embed-binary`** — publish an
`x86_64-unknown-linux-gnu` release asset with default features and provision the
model directory at `CORPUS_EMBEDDING_MODEL_DIR` — and it is explicitly not a
consequence of this decision. The pool merge, the limit and the schedules are
correct and land whether or not that row has shipped.
