"""Pins the set of concurrency pool names the code location declares.

Pools are created implicitly by an asset's ``pool=``: a module that names a new
one does not error, it just starts sharing the box with everything else while
``deploy/dagster.yaml`` keeps budgeting for the pools it knew about. That is how
the retired ``news_embed`` pool arrived unaccounted. This test discovers the pool names from the
loaded definitions rather than from a hand-written list of imported assets --- a
hand-written list cannot catch a pool declared in a module nobody remembered to
add, which is the exact drift being guarded.

Names only. *Which* pools carry memory, what each holder peaks at and how the
worst case sums against the box is stated once, in ``deploy/dagster.yaml``; a
copy of those numbers here would be the duplication this row removed.
"""

from __future__ import annotations

from eve_industry_orchestration.definitions import defs

# Every pool name any op in the code location may declare. `everef_download`
# (EVE Ref fetch politeness), `heavy` and `market_orders` (memory). The
# `_LIVE_POOL` constants in the live modules alias `everef_download` rather than
# adding a pool of their own, and an asset with no `pool=` contributes nothing
# --- it obeys only the global cap. The embed steps hold `heavy` (ADR-0002),
# not a pool of their own.
EXPECTED_POOLS = frozenset({"everef_download", "heavy", "market_orders"})


def declared_pools() -> dict[str, list[str]]:
    """Maps every pool the loaded definitions declare to the ops that name it.

    Walks the resolved job definitions rather than ``Definitions.assets`` so the
    scan covers every op that can actually be launched --- asset checks and
    explicitly defined jobs included --- not just the ops reachable from a
    statically imported asset symbol.
    """
    pools: dict[str, list[str]] = {}
    for job in defs().resolve_all_job_defs():
        for op in job.graph.iterate_op_defs():
            if op.pool is None:
                continue
            names = pools.setdefault(op.pool, [])
            if op.name not in names:
                names.append(op.name)
    return {pool: sorted(names) for pool, names in sorted(pools.items())}


def test_code_location_declares_exactly_the_budgeted_pools() -> None:
    """A new memory-bearing pool cannot arrive unaccounted for."""
    found = declared_pools()
    unbudgeted = sorted(set(found) - EXPECTED_POOLS)
    gone = sorted(EXPECTED_POOLS - set(found))

    assert not unbudgeted, (
        f"undeclared concurrency pool(s) {unbudgeted} "
        f"(declared by {[found[pool] for pool in unbudgeted]}). "
        "A pool is created implicitly by `pool=`, so nothing else fails: the "
        "run just shares the box with a budget that never counted it. Give it "
        "a measured peak and account for it in deploy/dagster.yaml, then add "
        "the name here."
    )
    assert not gone, (
        f"concurrency pool(s) {gone} are budgeted in deploy/dagster.yaml but no "
        "op declares them any more. Drop them from that budget and from here, "
        "or restore the `pool=` that went missing."
    )
