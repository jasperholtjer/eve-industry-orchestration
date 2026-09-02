# Append-only ADRs, with `Superseded by` instead of a rewrite

## What it would have been

The standard practice, and the one every ADR tutorial teaches. A record is never
edited after it is accepted: when a decision changes, the old record gets
`Status: Superseded by ADR-00NN` and stays, and the new one carries a
`Supersedes` line back. The numbers are then a genuine ledger — nothing
disappears, no number is unused, and every link that was ever written to an ADR
still resolves. It preserves *why the previous answer was reasonable*, which is
the thing that stops a session re-proposing it, and it is cheap: appending costs
nothing, and rewriting risks losing something nobody noticed was load-bearing.

## Why not

A reader should meet the current architecture, not its history — and here the
architecture is what fires when, which is the one thing that must never be read
one version stale. A superseded record describing a sensor's old trigger
condition or a pool's old limit is not a harmless archive: it is an operational
instruction that still greps, and an agent that reads it as current wires the
wrong cadence. `Status: Superseded` is one line of defence against that and it
is not enough, because the body still greps.

Git already is the ledger, and a better one — `git log -p docs/adr/` gives the
previous text with the change that replaced it and the reasoning in the commit
body, which the append-only version does not have. And the withdrawn alternative
is not lost when the rewrite is done properly: it belongs in the current
record's *Context*, where it argues against being re-proposed, rather than in a
dead record where it reads as live. This repository's decisions also live in
three places — `ROADMAP.md`'s Decisions section, `docs/serving-seam.md` and
`docs/adr/` — so a stacked history multiplies by three before anyone reads one.

## What would change our mind

A rewrite measurably losing something — a superseded record whose reasoning was
wanted, could not be recovered from `git log`, and cost a session real time.
Nobody has hit that yet, and with one ADR on file the stack has not had the
chance. The pruning rule already says a superseded number is not reused, so link
rot is bounded either way.
