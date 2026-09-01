## Context

See proposal.md — Why. The relevant state: the code this row asked for already
exists and passes its tests, while the specification of it does not exist and
three documents describe the pre-implementation state as current.

That makes this change unusual in one respect worth stating before the tasks are
read: the direction of fit is spec-to-code, not code-to-spec. Everywhere else in
this repository a change writes the spec and then makes the tree satisfy it.

## Goals / Non-Goals

**Goals**

- State the built Gold path as a testable contract, so the capability exists in
  `openspec/specs/` for later rows to depend on.
- Leave the documents unable to contradict the tree.

**Non-Goals**

- Changing the behaviour of `market_history_gold` or its sensor **beyond making it
  honour the exit contract of the command it invokes**. This started as a strict
  non-goal, on the assumption that specifying built code could not turn up a bug in
  it. Review found one: the asset ignored the `status: "skipped"` that
  `corpus gold build` returns for an ADR-0029 upstream gap and verified anyway,
  failing every gap day permanently, where four sibling Gold assets already guard
  it. Writing that behaviour into a spec would have codified the bug, so the guard
  was added here and the requirement states the corrected contract. The sensor is
  untouched, and `market_history_silver` — which has a related gap — is left to
  `docs/questions/2026-09-01-market-history-silver-skipped-status.md`.
- Generalising the capability to the other Gold datasets. `killmails`,
  `market_orders`, `industry_cost_indices` and `mer` share the shape, but each
  carries a `--derivative` dimension this dataset does not have (ADR-0025). One
  row, one capability; a shared spec is a later row's call, if it is anyone's.
- Re-measuring the `heavy` pool. The ~3-4 GiB figure is carried over, not
  established here.

## Decisions

**Write the spec to the code, not the code to the row's wording.**

The row says the sensor "pre-checks it via `corpus state query`". The shipped
sensor calls `corpus gold ready-dates`. The two are not equivalent: `state query`
returns run-state rows that Python would then have to interpret into a readiness
decision — which day's Silver, how much of the window, whether Gold exists —
while `ready-dates` returns the decision the binary already makes. Reading rows
and deciding readiness from them in Python is precisely what the thin-orchestration
boundary forbids, and it would put a second implementation of the coverage rule
in this repo, one that could disagree with the gate that actually runs.

The row was written before `corpus gold ready-dates` existed; the implementation
chose the better of the two once it did. Rewriting working code backwards to
match a superseded sentence in the roadmap would be a real regression in exchange
for a literal reading. So the spec records `ready-dates`, and this paragraph is
why the row's own wording is not being honoured to the letter.

*Alternative considered:* change the sensor to `state query` as written, and file
the divergence as a question. Rejected — it inverts the invariant the row itself
cites ("the binary enforces the coverage gate"), and there is no open decision
here to ask about, only a stale sentence.

**Prune `ROADMAP.md` work item 2 rather than annotate it.**

The repository's convention is that superseded records are rewritten or deleted,
not stacked with a successor, so a reader meets the current architecture and not
its history. An item that says "blocked upstream" with a note saying it is no
longer blocked is the stacked form. The "Gold on the NAS" decision inside that
item is *not* stale and is recorded nowhere else, so it survives the rewrite.

*Alternative considered:* delete work item 2 outright. Rejected — it would take
the NAS-root decision with it.

**No ADR.**

An ADR is for a cross-cutting decision. Nothing here changes the architecture:
the coverage gate, the storage boundary and the pool membership are all recorded
already, and this change only stops the documents denying that they are
implemented. The `ready-dates` choice above is a consequence of the existing
thin-orchestration invariant, not a new decision competing with it.

## Risks / Trade-offs

**A spec written from the code can encode a bug as a requirement.** → The
requirements were written from the observable contract — which command runs, who
gates, what fails — rather than transcribed from the implementation, and each
scenario has to be backed by a test that could fail. Where the fake-binary suite
does not already exercise a scenario, the task list adds a test; a scenario no
test can express is a sign the requirement describes implementation rather than
behaviour and should be cut.

**A capability that only ever describes one dataset invites a premature
generalisation later.** → Named `market-history-gold`, not `gold-build`, so the
scope is legible from the path and a future shared spec has to be argued for
rather than assumed.

## Migration Plan

None. No deployment step, no data migration, no rollback: the change adds a spec
file and edits prose. The asset and sensor it describes are already running.
