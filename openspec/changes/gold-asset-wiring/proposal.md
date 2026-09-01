## Why

Roadmap row `gold-asset-wiring` asks for the Silver -> Gold corpus subcommand to
be wired into `market_history_gold`. The code half of that row landed early, out
of band, in commit `6174cd2` ("Wire Gold path now corpus 0.1.4 ships the
builder"): the asset shells out to `corpus gold build` then
`corpus verify --tier gold`, and `market_history_gold_sensor` polls
`corpus gold ready-dates`. Nothing in the tree raises `NotImplementedError` any
more.

The specification half never landed. `openspec/specs/` holds one capability
(`sde-gold-readiness`), so the behaviour that the market-history Gold path now
depends on — which command runs, in which order, who owns the coverage gate, and
what the sensor is allowed to decide — is asserted nowhere. Three documents still
describe the opposite of the shipped code and would mislead the next reader:
`ROADMAP.md` work item 2 calls the row "blocked upstream" on a half-built
builder, and both it and `openspec/config.yaml` state that the asset raises
`NotImplementedError`.

This change closes the row by specifying the path as built and retiring the
stale claims, so the next row's dependency check reads a spec that matches the
binary.

## What Changes

- Add the `market-history-gold` capability: the two-step shell-out
  (`gold build`, then `verify --tier gold`), the binary as the sole authority on
  the `coverage_min_ratio` gate, the sensor's pre-check as an optimisation rather
  than the gate, and the `heavy` pool that bounds concurrent builds.
- Record the one place the shipped code diverges from the row's original wording:
  the sensor pre-checks through `corpus gold ready-dates`, not through a Python
  reading of `corpus state query`. `ready-dates` returns the readiness decision
  the binary already makes, which is what keeps the gate un-duplicated in Python;
  the row was written before that subcommand existed.
- Prune `ROADMAP.md` work item 2 to the decisions that still hold, dropping the
  "blocked upstream" framing and the `NotImplementedError` claim. The "Gold on
  the NAS" decision inside it stays: it is still live and is not restated
  anywhere else.
- Correct the same stale pre-check claim where it is stated most authoritatively:
  the **Gold coverage gate** bullet in `ROADMAP.md`'s Decisions section still says
  the sensor pre-checks through `corpus state query`. Only that sentence changes;
  the decision it belongs to is correct and is not being reopened.
- Correct the **State of the repository** paragraph in `openspec/config.yaml`,
  which still lists this asset as open work.
- Cover any spec scenario that the fake-binary suite does not already exercise.

No behavioural change: this change adds no code path and removes none. It states
what the tree does and makes the documents agree with it.

## Capabilities

### New Capabilities

- `market-history-gold`: how the market-history Gold partition is built and
  verified — the corpus subcommands the asset invokes and their order, where the
  coverage gate is enforced, what the availability sensor may and may not decide,
  and the concurrency pool the build runs under.

### Modified Capabilities

<!-- None. No existing capability's requirements change. -->

## Impact

- `openspec/specs/market-history-gold/` — new, promoted from this change's delta
  on archive.
- `ROADMAP.md` — work item 2 rewritten, and the one stale sentence in the
  **Gold coverage gate** decision corrected: it still names `corpus state query`
  as the sensor's pre-check. That bullet is the more authoritative of the two
  places, so leaving it would defeat the change. The rest of the decision —
  binary authoritative, pre-check an optimisation and not a correctness
  dependency — is correct and stands.
- `openspec/config.yaml` — the State of the repository paragraph.
- `roadmap.yaml` — row `gold-asset-wiring` to `status: done`.
- `tests/` — new cases only where a spec scenario has no test behind it.
- `src/eve_industry_orchestration/defs/market_history.py` and `sensors.py` are
  the subject of the spec but are **not** expected to change. A diff against
  either means the spec was written wrong, not the code.

The `heavy` pool membership the spec records is the existing one: market-history
Gold streams its full `[date - max_horizon, date]` Silver window through a k-way
merge and peaks at a measured ~3-4 GiB RSS, which is what earns it a pool slot.
This change measures nothing new and moves no asset in or out of the pool.
