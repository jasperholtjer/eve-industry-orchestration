---
status: open
row: gold-asset-wiring
---

# Is market-history Silver exempt from the ADR-0028 `skipped` status, or is the branch missing?

## Why this is blocked

Reviewing row `gold-asset-wiring` turned up a gap in `market_history_gold`: it
ignored the `status: "skipped"` that `corpus gold build` returns for an upstream
gap (ADR-0029) and verified anyway, failing the run permanently on such a day.
That is fixed in the row, mirroring the four sibling Gold assets.

Looking for the same shape one function up shows `market_history_silver`
(`defs/market_history.py:71`) branching on `status == "incomplete"` (ADR-0041)
and on nothing else. Every other Silver asset in the repository branches on
`status == "skipped"` (ADR-0028) instead or as well —
`industry_cost_indices.py:78`, `killmails.py:104`, `market_orders.py:113`. If
corpus ever reports `skipped` for a market-history day, the asset falls through
to `verify --tier silver` on a partition that was never written and the run
fails, with no retry path, which is the same failure the Gold fix just removed.

Whether that can actually happen is the part the row could not settle. It turns
on what the ingestor does for a market-history day EVE Ref never published, and
that is a corpus-side fact, not an orchestration one. The row fixed only the Gold
asset, which was in its scope; this one is filed rather than guessed at.

## The options

- **market-history genuinely cannot report `skipped`.** If every absent
  market-history day is a not-yet-settled day (`incomplete`, retryable) and never
  a permanent gap, the current code is correct and complete. Then what is missing
  is a sentence in the asset's docstring saying so, because four sibling assets
  establish the opposite reading and a future maintainer will "fix" this by
  copying them.
- **It can report `skipped` and the branch is simply missing.** Then this is the
  same bug the Gold asset had, in the asset one level up, and it needs the same
  guard plus `output_required=False`.
- **It can report `skipped` but that should stay fatal for this dataset.** A
  market-history gap might be worth failing loudly on, unlike a killmails one.
  Then the code is right and the reason belongs in a comment, since it is a
  deliberate departure from the house pattern.

## What I would do

Ask corpus, then pick. The deciding evidence is whether the market-history
ingestor has a code path that emits `{"status": "skipped"}` at all — a grep of
its ingestor against the one for `killmails` or `market-orders` settles it in
minutes, and `eve-industry-corpus` is read-only from here so this repository
cannot answer it itself.

My expectation is the second option. `corpus_resource.py:85-86` documents the
status line as a general contract (`"written" | "skipped"`, ADR-0028) rather than
a per-dataset one, which suggests any ingest can return it. But the asymmetry is
deliberate-looking enough — market-history is the only dataset with an
`incomplete` state at all, and ADR-0041 was written for it specifically — that
guessing here risks adding a dead branch to the one asset whose upstream
semantics differ from the rest.

Whichever way it goes, the answer is one comment or one guard, and it should end
with `market_history_silver` reading unambiguously either way.

## Answer

