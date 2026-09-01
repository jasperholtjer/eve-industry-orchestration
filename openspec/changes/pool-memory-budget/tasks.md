## 1. Make `deploy/dagster.yaml` the single source of the arithmetic

- [ ] 1.1 Rewrite the header comment so it names all four declared pools
      (`heavy`, `market_orders`, `news_embed`, `everef_download`), says which
      three carry memory, gives each one's limit and the peak of a single
      holder, and sums the worst case against the 12 GiB box. Verify the file
      still parses (`python -c "import yaml,sys; yaml.safe_load(open('deploy/dagster.yaml'))"`)
      and that the `concurrency:`, `run_coordinator:` and `telemetry:` values
      are unchanged (`git diff deploy/dagster.yaml` shows comment lines only).
- [ ] 1.2 Replace the claim at the old line 40 that the limit-1 `market_orders`
      pool "bounds that memory too": a limit-1 pool bounds the dataset against
      itself and says nothing about overlap with `heavy` or `news_embed`.
      Verify `grep -n "bounds that memory too" deploy/dagster.yaml` returns
      nothing.
- [ ] 1.3 State the exposure honestly in the same comment — the memory-bearing
      slots sum to `max_concurrent_runs`, so no combination is forbidden — with
      why it is tolerated (all six OOM kills predate corpus v0.7.0/v0.9.0 and
      the pool split; 68 days clean since) and the next step (reset
      `memory.peak`, kernel >= 6.9, and read the current configuration back
      before sizing). Verify the comment states a worst case, a box size and a
      measurement step.
- [ ] 1.4 Reconcile the comment at `deploy/redeploy.sh:245-247` with the
      rewritten arithmetic — it names which pools sit below `default_limit` and
      why they cannot live in `dagster.yaml`. Verify `bash -n
      deploy/redeploy.sh` passes and the two `concurrency set` calls are
      unchanged.

## 2. Pin the market-orders Silver resident-snapshot window

- [ ] 2.1 Add `CORPUS_PARSE_CONCURRENCY=6` to `deploy/dagster-daemon.service`
      beside `RAYON_NUM_THREADS=6`, with a comment saying it is the
      resident-snapshot cap *and* the parse batch size, that matching the rayon
      thread count keeps batches fully occupied, and that without it the window
      follows the host core count. Verify `grep -n CORPUS_PARSE_CONCURRENCY
      deploy/dagster-daemon.service` shows it inside the `[Service]` block.
- [ ] 2.2 Mirror it into `deploy/dagster-webserver.service` in the same
      duplicated-comment style already used there for `RAYON_NUM_THREADS`,
      because a launchpad-triggered run inherits the webserver's env. Verify
      both units carry the identical value (`grep -h CORPUS_PARSE_CONCURRENCY
      deploy/*.service | sort -u | wc -l` is 1).

## 3. Reduce the duplicate copies to invariant plus pointer

- [ ] 3.1 Shrink the Concurrency bullet in `CLAUDE.md` to the invariant —
      pools key on the assets' `pool=` and gate every launch path; membership
      of a memory-bearing pool is by measured peak; every memory-bearing pool
      counts against one box budget — plus a pointer to `deploy/dagster.yaml`
      for the numbers. Drop the stale `market-orders` Silver `heavy` membership
      and the `--cores 4` prescription. Verify no GiB figure or pool limit
      number survives in that bullet.
- [ ] 3.2 Do the same for the stale two-pool sentence at `ROADMAP.md:141-145`.
      Verify it no longer enumerates pools or limits and points at
      `deploy/dagster.yaml` instead.

## 4. Pin the declared pool set in a test

- [ ] 4.1 Add `tests/test_concurrency_pools.py` asserting that the set of pool
      names the loaded definitions declare equals exactly
      `{"everef_download", "heavy", "market_orders", "news_embed"}`, discovering
      them from the loaded definitions rather than from a hand-written list of
      imported assets. Verify `uv run pytest tests/test_concurrency_pools.py -q`
      passes.
- [ ] 4.2 Make the failure message name `deploy/dagster.yaml` as the file whose
      budget must account for a new pool. Verify by temporarily perturbing one
      asset's pool literal that the test fails with that message, then reverting.
- [ ] 4.3 Assert in the same module that the three memory-bearing pools are the
      ones the budget counts, so dropping one from the budget without dropping
      it from the code fails. Verify the test passes.

## 5. Verify the row

- [ ] 5.1 `uv run ruff check . && uv run ruff format --check . && uv run pytest -q`
      all green in the worktree.
