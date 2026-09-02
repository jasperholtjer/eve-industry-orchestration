"""killmails: one daily Silver source, one Gold derivative (corpus ADR-0059/0060/0061).

The consumption / demand signal — every destroyed hull, module, charge and drone
is a supply sink that must be re-manufactured and re-bought — and the corpus's
largest dataset by orders of magnitude (order 1–3M item-rows per day). EVE Ref
publishes one ``.tar.bz2`` per day holding N verbatim ESI killmail documents (the
``daily-tar-of-json`` layout); the binary explodes it into long-form item rows
plus one synthetic hull row per kill.

``killmails-consumption`` is a **backfillable historical matrix** — each date is
an independent, reproducible artifact over a 365-day ``closed="left"`` window —
so it gets the daily-partitioned asset + ``ready-dates`` sensor construct, with
the coverage gate owned by the binary.

Two genuine upstreams beyond Silver, both builder-pinned in corpus and both folded
into the partition's ``dependency_fingerprint``: the SDE snapshot Gold resolves
``solar_system_id → region_id``, and the ``market-history`` Gold supplies the
reference price (The Forge ``vwap``) for ``isk_value_destroyed``. The binary fails
loud when either is not ``_DONE``-sealed, so both are declared as deps rather than
left implicit.

**Mutable partitions.** Unlike every other everef dataset, a killmail day keeps
growing upstream long after first archival (ADR-0060). ``_DONE`` is still the only
success signal, but it is no longer a *freshness* contract: the drift sweep in
:mod:`sensors` re-proposes changed days for re-ingest, and the Gold-repair sensor
rebuilds the Gold those days feed. See ``sensors.killmails_freshness_sensor``.

Each asset is a thin shim over the ``corpus`` binary; the binary owns the compute,
the coverage gate, the cross-dataset joins, and the ``parquet + _INDEX.json +
_DONE`` contract. Partition starts come from the corpus dataset config (see
:mod:`config`), never hardcoded — Gold from 2022-01-01, Silver clamped to the
``silver.served_start`` floor of 2021-01-01 (= Gold minus the 365-day look-back).

**Gold verify keys on the derivative name, not the dataset.** ``corpus gold
build`` writes under ``gold/<derivative>/...`` and ``corpus verify --tier gold``
resolves ``gold/<--dataset>/...``, so Gold verify passes the *derivative* name as
``--dataset``. Silver verify still uses the dataset name.
"""

from collections.abc import Iterator

import dagster as dg

from eve_industry_orchestration.defs import market_history, sde
from eve_industry_orchestration.defs.config import resolve_partition_starts
from eve_industry_orchestration.defs.corpus_resource import CorpusResource, date_key

DATASET = "killmails"
CONSUMPTION_DERIVATIVE = "killmails-consumption"

_starts = resolve_partition_starts(DATASET, CONSUMPTION_DERIVATIVE)
if _starts.gold is None:
    raise ValueError(
        f"{DATASET} resolved no Gold served_start; kills-consumption is a windowed "
        "historical matrix and must declare one"
    )
silver_partitions = dg.DailyPartitionsDefinition(start_date=_starts.silver)
gold_partitions = dg.DailyPartitionsDefinition(start_date=_starts.gold)

# EVE Ref politeness, like every other Silver fetch. NOT `heavy`: the day tar is
# a few MiB and the parse holds ~1–3M narrow rows, far below the ~3–4 GiB peak the
# memory pool exists to bound.
_SILVER_POOL = "everef_download"
# Provisional `heavy` membership. The repo rule is "measure the peak first"
# (AGENTS.md), and this build is NOT yet measured — but it is the one plausible
# candidate in the dataset: a 365-day window over the corpus's largest Silver.
# Being wrong here costs one contended slot; being wrong the other way OOMs the
# LXC mid-backfill. Measure with `/usr/bin/time -v` on a real day and drop the
# pool if the peak lands with the narrow windowed builds (~100 MiB).
_GOLD_POOL = "heavy"


@dg.asset(
    partitions_def=silver_partitions,
    group_name="killmails",
    kinds={"corpus"},
    pool=_SILVER_POOL,
    # An interior upstream-gap day (EVE Ref published nothing, ADR-0028) skips:
    # corpus exits 0 with status "skipped" and writes no partition, so the asset
    # must complete without materialising — the partition stays Missing.
    #
    # A day whose daily tar has not been published yet reports status
    # "incomplete" instead — also exit 0, also no partition, but retryable
    # rather than permanent.
    output_required=False,
)
def killmails_silver(
    context: dg.AssetExecutionContext, corpus: CorpusResource
) -> Iterator[dg.MaterializeResult | dg.AssetObservation]:
    """Silver partition: explode one day's killmail tar, then verify.

    Re-runnable by design: a drifted day (ADR-0060) is repaired by exactly this
    call, which overwrites the partition, rewrites ``_DONE``, and updates the
    freshness token. The status object carries that token, so a materialisation
    records the count this partition actually holds.

    Two distinct non-materialising outcomes are left Missing, each with its own
    ``AssetObservation`` reason, instead of falling through to a verify that
    would fail on a partition deliberately never written:

    - ``status: skipped`` — an interior day EVE Ref never published and never
      will (permanent, ADR-0028/0029).
    - ``status: incomplete`` — the day is not published *yet*. This is reachable
      for killmails because the ``daily-tar-of-json`` layout resolves a 404 on
      the day tar through ``classify_absent_date`` against the year
      ``index.json``, which returns ``IndexVerdict::NotYetPublished`` at the
      publication frontier (corpus ADR-0028, Decision extended 2026-09-01 with
      the ``daily-tar-of-json`` arm) → ``IngestOutcome::SkippedIncomplete`` →
      ``finalize_incomplete``. The availability sensor rotates its ``run_key``
      per tick (see :mod:`sensor_util`), so the date is re-proposed and
      materialises once upstream settles, rather than red-looping every tick.
    """
    date = context.partition_key
    status = corpus.run(
        context,
        "ingest",
        "--dataset",
        DATASET,
        "--date",
        date,
        "--sink-path",
        corpus.sink_path,
    )
    if status is not None and status.get("status") == "skipped":
        context.log.info(
            "killmails %s: upstream absent, leaving partition missing", date
        )
        yield dg.AssetObservation(
            asset_key=context.asset_key,
            partition=date,
            metadata={
                "skip_reason": "upstream_absent",
                "detail": str(status.get("reason", "")),
            },
        )
        return
    if status is not None and status.get("status") == "incomplete":
        context.log.info(
            "killmails %s: upstream publication incomplete, leaving partition "
            "missing (retryable)",
            date,
        )
        yield dg.AssetObservation(
            asset_key=context.asset_key,
            partition=date,
            metadata={
                "skip_reason": "upstream_incomplete",
                "detail": str(status.get("reason", "")),
            },
        )
        return
    corpus.run(
        context,
        "verify",
        "--dataset",
        DATASET,
        "--date",
        date,
        "--tier",
        "silver",
        "--sink-path",
        corpus.sink_path,
    )
    metadata: dict[str, object] = {
        "dataset": DATASET,
        "tier": "silver",
        "partition": date,
    }
    # The distinct-killmail count this partition materialised (ADR-0060). Recorded
    # so the drift history is readable from the materialisation log, not only from
    # run-state.
    if status is not None and status.get("freshness_token") is not None:
        metadata["freshness_token"] = str(status["freshness_token"])
    # The run-state facts corpus just recorded (rows, retention_class,
    # parquet_sha256) merge over the identifying fields; the read is advisory and
    # yields {} rather than failing a materialisation corpus already completed.
    metadata |= corpus.partition_metadata(DATASET, "silver", date_key(date))
    yield dg.MaterializeResult(metadata=metadata)


@dg.asset(
    name="killmails_consumption_gold",
    partitions_def=gold_partitions,
    deps=[killmails_silver, sde.sde_snapshot_gold, market_history.market_history_gold],
    group_name="killmails",
    kinds={"corpus"},
    pool=_GOLD_POOL,
    # A target day whose Silver is an upstream gap can never produce a Gold row
    # (ADR-0029); corpus reports "skipped", so the asset completes without
    # materialising — the partition stays Missing rather than failing.
    output_required=False,
)
def killmails_consumption_gold(
    context: dg.AssetExecutionContext, corpus: CorpusResource
) -> Iterator[dg.MaterializeResult | dg.AssetObservation]:
    """Gold partition: destroyed volume + ISK value per ``(region_id, type_id)``.

    ``deps=`` is lineage only; the readiness sensor drives this. Both cross-dataset
    inputs are real upstreams — the build reads them and fingerprints them — so a
    missing SDE or market-history Gold fails the run in the binary, by design.
    """
    date = context.partition_key
    status = corpus.run(
        context,
        "gold",
        "build",
        "--dataset",
        DATASET,
        "--derivative",
        CONSUMPTION_DERIVATIVE,
        "--date",
        date,
        "--sink-path",
        corpus.sink_path,
    )
    if status is not None and status.get("status") == "skipped":
        context.log.info(
            "%s %s: target silver is an upstream gap, leaving partition missing",
            CONSUMPTION_DERIVATIVE,
            date,
        )
        yield dg.AssetObservation(
            asset_key=context.asset_key,
            partition=date,
            metadata={
                "skip_reason": "upstream_gap",
                "detail": str(status.get("reason", "")),
            },
        )
        return
    corpus.run(
        context,
        "verify",
        "--dataset",
        CONSUMPTION_DERIVATIVE,
        "--date",
        date,
        "--tier",
        "gold",
        "--sink-path",
        corpus.sink_path,
    )
    # `corpus gold build` writes the run-state row under the *derivative* name,
    # not the dataset, so the Gold read keys on CONSUMPTION_DERIVATIVE.
    yield dg.MaterializeResult(
        metadata={
            "dataset": DATASET,
            "derivative": CONSUMPTION_DERIVATIVE,
            "tier": "gold",
            "partition": date,
        }
        | corpus.partition_metadata(CONSUMPTION_DERIVATIVE, "gold", date_key(date))
    )
