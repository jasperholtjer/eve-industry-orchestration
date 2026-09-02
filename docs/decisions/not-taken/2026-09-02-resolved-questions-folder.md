# A `resolved/` folder for answered questions

## What it would have been

A third folder under `docs/questions/`, where a question goes once its answer
has landed in an ADR, a test or the code. The file is not deleted — it keeps the
question as it was asked, the options as they stood, the recommendation that was
made and the person's answer verbatim. Six months later, when someone asks why a
Gold readiness sensor re-proposes a partition whose predecessor moved, the ADR
says *what* was decided and `resolved/` says *what else was on the table and what
it would have cost* — the part an ADR compresses out. It also makes the
two-folder state a lossy step: today the richest record of a decision is
destroyed at exactly the moment the decision starts mattering.

## Why not

`ls` stops answering "is this handled". Two folders are readable at a glance and
no frontmatter key can drift from them; three means the third grows without
bound and is read by nobody, while the platform's `platform-next` skill gains a
state that is neither an inbox nor a block. The record that has to be right is the one
something enforces — the ADR, the spec, the test, the partition definition — and
keeping the question beside it means two places to read and one to forget.

The alternatives-and-costs content is not actually lost either: it belongs in
the ADR's *Context* or in the OpenSpec proposal the row already wrote, and a
record that dropped it was written badly. The deleting commit body names where
the answer landed, so `git log -- docs/questions/` is one command away when the
question behind a decision is genuinely wanted.

## What would change our mind

The deleting commit bodies turning out not to be enough, twice — a decision
whose reasoning a session had to reconstruct from scratch because the ADR, the
proposal and the commit body were all thin. The cheaper fix is a fuller ADR
*Context*, so this only re-opens if that has been tried and the records are
still losing it.
