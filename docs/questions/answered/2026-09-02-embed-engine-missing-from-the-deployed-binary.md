---
row: embed-on-the-box
---

# Should the LXC be able to run `corpus enrich embed`, or is embedding an operator step?

## Why this is blocked

`news_embeddings_bronze` and `transcripts_embeddings_bronze` shell out to
`corpus enrich embed`. The binary the box actually runs cannot: corpus's release
workflow builds `x86_64-unknown-linux-musl` with `--no-default-features`, which
drops `corpus-cli`'s `embed-engine` feature, because `ort` publishes no prebuilt
onnxruntime for musl (corpus CHANGELOG, "the release build no longer fails on the
musl target"). `redeploy.sh` installs exactly that asset (`CORPUS_TARGET`), so
every daily `news` and `transcripts` run fails at the embed asset and its Silver
and Gold never run:

```
corpus exited 1: /usr/local/bin/corpus enrich embed --dataset news-embeddings
  --date 2026-09-01: 11601 chunk(s), 11492 already embedded, 109 to embed
Error: this `corpus` was built without the `embed-engine` feature ... (ADR-0053)
```

The two sides disagree on where the model runs. This repo assumes the box: the
systemd units export `CORPUS_EMBEDDING_MODEL_DIR=/usr/local/share/corpus/models/bge-m3`,
`deploy/dagster.yaml` budgets a measured 4.4 GiB `news_embed` pool holder against
the 12 GiB LXC, and both bronze assets ride their group's daily schedule. Corpus
assumes the operator's machine ("runs on the operator's machine anyway"). Nothing
in `docs/adr/` here records the choice, and the answer to one option is a corpus
row, so this session cannot settle it.

## The options

- **A. Ship a glibc build.** Corpus adds an `x86_64-unknown-linux-gnu` release
  asset with default features, `redeploy.sh` points `CORPUS_TARGET` at it, and
  the ~540 MB model dir is provisioned on the box. Keeps everything already built
  and measured; costs a corpus row and couples the deployed binary to the LXC's
  glibc, which the static musl asset was chosen to avoid.
- **B. Unwire embed, like `enrich annotate`.** Both bronze assets leave Dagster;
  the operator embeds from a default-feature build and Dagster's chain starts at
  `ingest`. Cheapest, honest about where the model runs; the daily 109-chunk
  increment then lags an operator run, and Gold serves a short generation
  in between. Retires the `news_embed` pool and the model-dir env.
- **C. A second binary for embed only.** Keep the musl asset for everything and
  install a gnu `corpus` beside it as the embed binary, addressed by a new
  `CorpusResource` path. Preserves the static release; adds a second version to
  keep in step with the first, which is the drift `redeploy.sh` exists to prevent.

## What I would do

**A.** The box was already provisioned, budgeted and scheduled for embed-on-the-box
— the 4.4 GiB pool holder, the model-dir env and the group schedules are all in
place and were reviewed as such — so B is not a simplification but the removal of
a landed design, and it leaves a daily increment permanently behind a manual step
on a Windows machine writing to the NAS. The glibc coupling A introduces is real
but bounded: one target, one runner base, and the LXC is the only consumer. C
trades that coupling for two binaries that can drift, which is worse.

## Answer

**A — embedding runs on the box — but the pool is merged rather than kept.**

Corpus ships a second release asset, `x86_64-unknown-linux-gnu` with default
features, and `redeploy.sh` points `CORPUS_TARGET` at it. That is a corpus row.
The build is smaller than the option describes: `ort 2.0.0-rc.12` links a
prebuilt onnxruntime for the gnu target, so no C++ build enters the release, and
the glibc direction is the harmless one — `ubuntu-latest` (2.39) produces a
binary Debian 13 (2.41) runs. The musl asset stays for everything else.

The capacity concern behind the question does not survive the numbers. The NUC
(ADR-0020, Core Ultra 5 225H, 14 cores) is the box `INTRA_THREADS = 8` in
`crates/corpus-cli/src/enrich.rs:67` was already written for. The daily increment
is ~109 chunks — half a minute of inference. A full generation is ~11.3k chunks
for news and ~1.2k (→ ~4.1k after the video backfill) for transcripts, so a model
bump is one or two hours, once, ledgered and resumable. None of that needs a
second machine.

**The exclusion is why `news_embed` goes away.** Dagster cannot say "pool X never
beside pool Y": an asset carries exactly one pool, and with `granularity: run` a
run claims one slot in *every* pool its ops name and blocks when any of them is
full (`_core/op_concurrency_limits_counter.py:184`). "Never beside a heavy Gold
build" is therefore only sayable by *being* that pool. Both embed assets join
`heavy`, and `heavy` drops from 2 to 1. Four pools become three, and the worst
case `deploy/dagster.yaml` admits to falls from ~17.15 GiB to ~9.2 GiB — the
first configuration that fits the 12 GiB LXC on paper. `market_orders` stays
separate: its limit-1 is about saturating every core, and folding it in would
park every windowed Gold build behind a multi-hour Silver run.

That is what makes the global cap honest again. `max_concurrent_runs` stops
being the accidental memory backstop the current arithmetic leans on and becomes
what it is documented as — the NAS spindle's I/O cap — so it rises 4 → 6, the
number that file already names as the next step. The light work is what gains:
four live schedules currently fire together on the hour and fill the cap of 4 by
themselves.

Schedules move to `10 22 * * *` and `10 23 * * *` UTC. EVE Ref's publish hour is
not fixed (baseline 04:30 UTC, observed as late as 16:56), so late evening UTC is
the only window with no sensor-driven Gold work in it; the ten-minute offset
clears the `:00/:15/:30/:45` live ticks, and an hour apart instead of thirty
minutes covers a full generation holding the pool.

One measurement is owed, and it is not a question: the 4.4 GiB is a Windows
workstation figure. Take `/usr/bin/time -v` on the LXC on the first real run and
correct the budget table with it.

