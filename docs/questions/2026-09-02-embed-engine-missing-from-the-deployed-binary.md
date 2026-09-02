---
status: open
row: none (daily production failure)
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

