## 1. Back every spec scenario with a test

The spec is written from code that already runs, so the safeguard against
encoding a bug as a requirement is that each scenario is expressed as a test that
could fail. These tasks add tests only; they consult the `dagster-expert` skill
before driving any Dagster definition, since sensor and asset invocation shapes
are version-specific.

- [ ] 1.1 Audit `tests/` against the seven requirements in
      `openspec/changes/gold-asset-wiring/specs/market-history-gold/spec.md` and
      record, in `tmp/scenario-coverage.md`, one line per scenario naming the
      test that backs it or `MISSING`. Verify by reading that file: every
      scenario appears exactly once.
- [ ] 1.2 For each `MISSING` scenario on the Gold **asset** — build-then-verify
      order, no verify after a failed build, failure when verification fails,
      root passed as a flag — add a test against the fake corpus binary. Verify
      with `uv run pytest -q tests/ -k gold`.
- [ ] 1.3 For each `MISSING` scenario on the Gold **sensor** — a date not
      reported ready is not requested, a reported date outside the partition
      range is ignored, the per-tick cap takes the oldest first, a still-ready
      date is re-requested on a later tick, an in-flight date is not re-requested
      — add a test driving `market_history_gold_sensor` against the fake binary.
      Consult the `dagster-expert` skill for the sensor-invocation shape first.
      Verify with `uv run pytest -q tests/ -k sensor`.
- [ ] 1.3a Back the `heavy`-pool requirement with a static assertion that
      `market_history_gold` declares the pool, rather than a runtime test: the
      fake-binary suite runs no scheduler, so pool arbitration is not observable
      there. Assert on the asset definition's declared pool and note in the test
      why it is a config-level assertion. Verify with
      `uv run pytest -q tests/ -k pool`.
- [ ] 1.4 Confirm no scenario needed a change to
      `src/eve_industry_orchestration/defs/market_history.py` or
      `sensors.py`. Verify with `git diff --stat develop -- src/`: empty output.
      A non-empty diff means the spec was mis-written — fix the spec, not the
      code, and say so in the report.

## 2. Retire the stale claims

- [x] 2.1 Rewrite work item 2 of `ROADMAP.md` to describe the shipped path:
      `corpus gold build` then `corpus verify --tier gold`, the binary as the
      coverage authority, the sensor pre-checking through
      `corpus gold ready-dates`. Drop the "blocked upstream" framing, the
      `NotImplementedError` claim and the `corpus state query` pre-check
      sentence. Keep the "Gold on the NAS (decided)" paragraph intact — it is
      still live and recorded nowhere else. Verify with
      `grep -n 'NotImplementedError\|blocked upstream' ROADMAP.md`: no match.
- [x] 2.1a In the same file's **Decisions** section, correct the one stale
      sentence in the **Gold coverage gate** bullet: the sensor pre-checks via
      `corpus gold ready-dates`, not `corpus state query`. Change nothing else in
      that bullet - "binary authoritative" and "the orchestration check is an
      optimisation, not a correctness dependency" are both still true. Verify
      with `grep -n 'state query' ROADMAP.md`: no match inside the Gold coverage
      gate bullet.
- [x] 2.2 Correct the **State of the repository** paragraph in
      `openspec/config.yaml`: `market_history_gold` is no longer open work, so
      only the materialisation-metadata row remains in that sentence. Verify with
      `grep -n 'NotImplementedError' openspec/config.yaml`: no match.
- [x] 2.3 Check no other tracked file still claims the asset is unimplemented.
      Verify with
      `git grep -n 'NotImplementedError' -- '*.md' '*.yaml' ':!roadmap.yaml' ':!openspec/changes/gold-asset-wiring/'`:
      no match. `roadmap.yaml` is excluded deliberately - the phrase survives
      there inside this row's own `goal:` text, which records what was asked and
      is not rewritten after the fact; only the row's `status:` changes.

## 3. Verify

- [ ] 3.1 Run the repository gate in the worktree and confirm all three pass:
      `uv run ruff check .`, `uv run ruff format --check .`, `uv run pytest -q`.
      There is no type checker in this repository by design; its absence is not a
      skipped check.
- [ ] 3.2 Run `openspec validate gold-asset-wiring --strict` and confirm it
      passes.
