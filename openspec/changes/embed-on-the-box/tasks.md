## 1. Pool membership: news.py, transcripts.py, market_orders.py

- [ ] 1.1 In `defs/news.py`, change `_EMBED_POOL` (line ~265) from
      `"news_embed"` to `"heavy"`. Keep the constant and its name: the `_*_POOL`
      constants in this repo name the member's role, not the pool
      (`_SILVER_POOL = "everef_download"`, `_LIVE_POOL = "everef_download"`),
      and the `pool=` line therefore does not move. Rewrite the comment block
      above it (lines ~257-264, which currently argues for the *opposite* — an
      own pool so as not to double-peak inside `heavy`'s limit 2): `heavy` is
      now limit 1, and the membership buys exclusion from the windowed Gold
      builds, which a separate pool cannot express. Point at ADR-0002 and at
      `deploy/dagster.yaml` for the figures; do not restate the numbers.
      Consult the `dagster-expert` skill before touching the asset decorator.
      Verify: `uv run ruff check src/eve_industry_orchestration/defs/news.py`.
- [ ] 1.2 Make the matching edit in `defs/transcripts.py` for
      `transcripts_embeddings_bronze` — the same constant/comment shape at
      lines ~257-265, whose comment says the step "SHARES the `news_embed`
      pool ... with news-embeddings". It now shares `heavy`, and what it shares
      it with has widened; say that. Verify: `uv run ruff check
      src/eve_industry_orchestration/defs/transcripts.py`.
- [ ] 1.3 In `defs/market_orders.py`, correct the comment at line ~76-80 that
      names `news_embed` to name `heavy` instead; no `pool=` assignment
      changes. Grep the whole tree for `news_embed` before reporting — every
      remaining mention outside `deploy/` and `tests/` (owned by bundles 3 and
      4) belongs to this task. Verify:
      `uv run ruff check src/eve_industry_orchestration/defs/market_orders.py`.
- [ ] 1.4 Real run: with `CORPUS_BINARY_PATH` pointed at
      `tests/fake_corpus.py` and `DAGSTER_HOME`/`CORPUS_SINK_PATH` under
      `C:\tmp\orchestration-scratch\embed-on-the-box`, materialise
      `news_embeddings_bronze` for one partition and confirm in the run log
      that the op claims the `heavy` pool (not `news_embed`) and the run
      succeeds against the fake binary.

## 2. Schedules: sensors.py

- [ ] 2.1 Change `news_daily_schedule`'s `cron_schedule` from `"0 22 * * *"`
      to `"10 22 * * *"` and `transcripts_daily_schedule`'s from
      `"30 22 * * *"` to `"10 23 * * *"`. Rewrite the doc-comment above
      `transcripts_daily_schedule` (currently describing a 30-minute stagger)
      to explain the hour-wide gap: it lets one full embed generation hold the
      shared `heavy` slot without the other schedule's run queuing behind it.
      Consult `dagster-expert` first. Verify: `uv run ruff check defs/sensors.py`.
- [ ] 2.2 Real run: preview one tick of `news_daily_schedule` and one tick of
      `transcripts_daily_schedule` against a scratch Dagster instance
      (`DAGSTER_HOME` under `C:\tmp\orchestration-scratch\embed-on-the-box`)
      and confirm the previewed run requests match the new cron times.

## 3. Deploy config: dagster.yaml, redeploy.sh

- [ ] 3.1 In `deploy/dagster.yaml`: remove the `news_embed` pool from the
      per-class exceptions prose and the memory budget table; restate
      `heavy`'s row as limit 1, per-holder peak 4.4 GiB (embed's measured
      figure, now the pool's ceiling since embed is its heaviest possible
      holder); recompute the worst-case total against the 12 GiB box and show
      the arithmetic inline. The row has settled which figure `heavy`'s new
      per-holder peak is: embed's 4.4 GiB, because embed is now the heaviest
      thing that can hold the pool. Show `4.4 + 4 + 2×0.37 = ~9.15` against the
      12 GiB box rather than asserting a total. Mark the 4.4 GiB as what it is
      — a Windows-workstation measurement, never yet run on the LXC — and name
      `/usr/bin/time -v` on the first real embed there as what corrects it.
      Drop `concurrency.pools.default_limit` from 2 to 1 (this, not an
      override, is what puts `heavy` at 1) and say in the comment why the
      default is the safe one: a lost instance-DB override then slows EVE Ref
      downloads instead of restoring two concurrent embeds on a 12 GiB box.
      Raise `concurrency.runs.max_concurrent_runs` from 4 to 6 and rewrite the
      surrounding comment to describe it as the NAS spindle's I/O cap, not a
      memory backstop. The pooled worst case fills 4 of 6 slots, so the table
      MUST carry an explicit term for the two unpooled runs that can sit on top
      of ~9.15 GiB — name `public_contracts` and the three `sovereignty_*` Gold
      builds as the unmeasured occupants (their own module comments say they
      have no measured peak), and state the sum as `~9.15 GiB + 2 × unmeasured`
      rather than as a closed number. Verification is task 3.3.
- [ ] 3.2 In `deploy/redeploy.sh`: both existing
      `dagster instance concurrency set` calls (`market_orders 1`,
      `news_embed 1`) go, replaced by one —
      `dagster instance concurrency set everef_download 2` — in the same
      `run_as_user` wrapper. `heavy` and `market_orders` need no override once
      `default_limit` is 1. Rewrite the "Four pools are declared" comment: three
      pools, the default is the memory-safe limit, and the single override is
      the one pool whose limit costs no memory. Verify:
      `bash -n deploy/redeploy.sh` (syntax check).
- [ ] 3.3 Real run: reproduce `redeploy.sh`'s own `validate_instance_config`
      step locally — copy the edited `deploy/dagster.yaml` into a throwaway
      `DAGSTER_HOME` and run
      `uv run python -c "from dagster import DagsterInstance; DagsterInstance.from_config('<tmp>').dispose()"`
      — and confirm it loads without error.

## 4. Test and ADR

- [ ] 4.1 In `tests/test_concurrency_pools.py`, drop `"news_embed"` from
      `EXPECTED_POOLS` and correct the two mentions of the retired pool in the
      module docstring (line ~6) and the comment above the set (line ~22).
      Verify: `uv run pytest tests/test_concurrency_pools.py -q` passes.
- [ ] 4.4 In `tests/test_context_datasets.py`, the two assertions at ~162-165
      and ~349-352 assert `op.pool == "news_embed"`. Swap the string AND rename
      the two tests — "holds its own limit one pool" and "shares the news embed
      pool" describe an arrangement that no longer exists, and a passing test
      with a lying name is worse than a failing one. What they now assert is
      that both embed steps hold `heavy`, which is what the exclusion rests on.
      Verify: `uv run pytest tests/test_context_datasets.py -q` passes.
- [ ] 4.5 `CONTEXT.md` (~line 60, "Four exist: ...") and `README.md` (~line
      148, "Both embed steps share the single `news_embed` limit-1 pool") both
      state the retired arrangement. Correct both to three pools and to what
      the embed steps now share, with a pointer to `deploy/dagster.yaml` for
      the figures — never a second copy of the numbers.
- [ ] 4.2 Write `docs/adr/0002-heavy-pool-membership-for-exclusion-not-peak.md`
      following `docs/adr/0001-...md`'s shape (Status, Context, Decision,
      Consequences): the decision is that `heavy` membership can be bought to
      guarantee mutual exclusion between two asset classes, not only by a
      measured peak, and that this is the first and — per the row's own
      framing — deliberately singular exception. Record `market_orders` staying
      its own limit-1 pool (bounded for CPU saturation; folding it in would
      park every windowed Gold build behind a multi-hour Silver run) as the
      boundary of the decision, and a *Known limits* paragraph for the two
      unmeasured figures: embed's 4.4 GiB peak on the LXC and whether 6 is the
      right cap for the NAS spindle. Three things the pre-code review found
      belong in it and are not optional:
      (a) reject `concurrency.runs.tag_concurrency_limits` by name as the one
      near-miss construct — it keys on run tags, which do not cover every
      launch path — so "the exclusion cannot be expressed any other way" is
      falsifiable rather than a claim the next reader re-derives;
      (b) the price: `heavy` at 1 serialises the windowed Gold backfills
      (market-history, market-orders, killmails Gold) that could previously run
      two abreast — that is what the exclusion costs and it belongs in
      Consequences;
      (c) the exclusion lives in the instance DB, not in version control,
      because `concurrency.pools` has no per-pool field; `default_limit: 1` is
      chosen so that losing it fails safe.
      Record the corpus row this does not include (glibc release asset,
      model-dir provisioning) as an explicit non-consequence. The spec delta at
      `openspec/changes/embed-on-the-box/specs/concurrency-pools/spec.md` is
      already written and is not this bundle's to edit.

## 5. Full verification

- [ ] 5.1 `uv run ruff check . && uv run ruff format --check . && uv run
      pytest -q` — full suite green.
