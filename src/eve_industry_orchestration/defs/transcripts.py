"""transcripts: CCP YouTube channel → Bronze → Silver → three Gold trees + embeddings.

The market-commentary sibling of :mod:`news` (corpus ADR-0046/0048/0055). ``corpus
context fetch --dataset transcripts`` pulls the CCP YouTube channel Atom feed and
each new video's Supadata transcript JSON and archives the bytes verbatim to a
fetch-date-partitioned Bronze tree (``bronze/transcripts/year=/month=/day=/`` with
``_MANIFEST.json`` then ``_DONE`` last), skipping already-transcribed videos via
the corpus seen-ledger. ``corpus ingest`` parses that partition into single-version
Silver (ADR-0055 — no bitemporal versioning: a transcript is paid for once and
never re-fetched) and three ``corpus gold build`` derivatives fan out of it:

- ``transcripts-videos`` (``videos``) — the canonical per-video table.
- ``transcripts-sections`` (``text-sections``) — chunked speech + ``embed_text``.
- ``transcripts-entity-mentions`` (``entity-mentions``) — SDE-name matches. Its
  vocabulary is a **cross-dataset Gold input**: the ``sde-*`` snapshot trees at the
  Gold root, fingerprinted into ``_INDEX.json`` — hence the extra dep on the SDE
  snapshot Gold, exactly as ``news-entity-mentions``.

Every tier is keyed on the **fetch** date (one dense partition per daily run) and
each Gold partition derives from that day's Silver alone, so the whole chain is
non-partitioned in Dagster and runs off one daily schedule (see :mod:`sensors`); a
past fetch date is re-processed via the ``TranscriptsDateConfig`` run-config, not a
partition matrix. The corpus binary owns every byte; the assets only shell out and
record the run.

The fetch hits the YouTube feed + Supadata (neither EVE Ref nor ESI), so the assets
join no concurrency pool: request pacing (~1 req/s Supadata) lives in the binary.
Secrets come from the process env (ADR-0047, Doppler retired), passed through to the
subprocess by :class:`CorpusResource`:

- ``SUPADATA_API_KEY`` — the daily fetch **and** the backfill (Supadata transcript
  calls cost paid credits).
- ``YOUTUBE_API_KEY`` — the backfill only (enumerating the channel's full upload
  history).
"""

import datetime as dt

import dagster as dg

from eve_industry_orchestration.defs import sde
from eve_industry_orchestration.defs.corpus_resource import CorpusResource

DATASET = "transcripts"


@dg.asset(
    group_name="transcripts",
    kinds={"corpus"},
)
def transcripts_bronze(
    context: dg.AssetExecutionContext, corpus: CorpusResource
) -> dg.MaterializeResult:
    """Fetch today's YouTube feed + new transcripts into a Bronze partition.

    Non-partitioned: each run archives the current fetch-date partition
    (``bronze/transcripts/year=/month=/day=/``). Idempotent — a same-day re-run
    overwrites the identical set and skips already-transcribed videos via the
    corpus seen-ledger. The binary owns the fetch, the manifest, and the atomic
    ``_MANIFEST.json`` + ``_DONE`` seal; the asset only shells out and records the
    run. Needs ``SUPADATA_API_KEY`` in the process env.
    """
    status = corpus.run(
        context,
        "context",
        "fetch",
        "--dataset",
        DATASET,
        "--sink-path",
        corpus.sink_path,
    )
    metadata: dict[str, object] = {"dataset": DATASET, "tier": "bronze"}
    if status is not None:
        for key in ("partition", "objects", "new_documents"):
            if key in status:
                metadata[key] = status[key]
    return dg.MaterializeResult(metadata=metadata)


# --- Silver + Gold (corpus ADR-0055) ---------------------------------------
#
# Silver and Gold mirror Bronze's *fetch-date* partitioning: `corpus ingest
# --dataset transcripts --date D` parses the Bronze partition of fetch-date D, and
# each Gold derivative is a pure function of that same day's Silver alone (no
# look-back, no coverage gate). So the chain is non-partitioned in Dagster, exactly
# like `transcripts_bronze`: every run processes the fetch date the run itself just
# archived. `TranscriptsDateConfig.date` re-runs a past fetch date (e.g. a backfill
# partition) without a partition matrix — `transcripts.yaml` declares no
# `served_start`, so there is no config-owned start date to anchor one with.


class TranscriptsDateConfig(dg.Config):
    """Run-config for the fetch-date the Silver/Gold chain processes.

    Defaults to today (UTC) — the partition ``transcripts_bronze`` just archived.
    Set ``date`` (``YYYY-MM-DD``) to re-process an earlier fetch date, e.g. a
    historical backfill partition.
    """

    date: str | None = None


def _target_date(config: TranscriptsDateConfig) -> str:
    """Resolves the fetch date to process: the run-config date, else today (UTC)."""
    if config.date is not None:
        return config.date
    return dt.datetime.now(dt.UTC).date().isoformat()


@dg.asset(
    deps=[transcripts_bronze],
    group_name="transcripts",
    kinds={"corpus"},
)
def transcripts_silver(
    context: dg.AssetExecutionContext,
    corpus: CorpusResource,
    config: TranscriptsDateConfig,
) -> dg.MaterializeResult:
    """Parse one fetch-date Bronze partition into single-version Silver (ADR-0055).

    One row per video (natural key ``[video_id]``); a video discovered but without
    an archived transcript is **absent** (caption-less videos stay retryable,
    ADR-0048), and a transcript no metadata source names is a parse error, not a
    skip. The binary owns the parse, the schema and the ``_DONE`` seal; the asset
    shells out and verifies.
    """
    date = _target_date(config)
    corpus.run(
        context,
        "ingest",
        "--dataset",
        DATASET,
        "--date",
        date,
        "--sink-path",
        corpus.sink_path,
    )
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
    return dg.MaterializeResult(
        metadata={"dataset": DATASET, "tier": "silver", "partition": date}
    )


def _build_gold_asset(
    derivative: str, *, extra_deps: list[dg.AssetsDefinition] | None = None
) -> dg.AssetsDefinition:
    """Builds one non-partitioned Gold asset for a transcripts derivative (ADR-0025).

    Verify keys on the derivative name, not the dataset: ``corpus gold build`` writes
    ``gold/<derivative>/...`` and ``corpus verify --tier gold`` resolves
    ``gold/<--dataset>/...`` — so the Gold verify passes the *derivative* name as
    ``--dataset`` (Silver verify uses the dataset name).
    """

    @dg.asset(
        name=f"{derivative.replace('-', '_')}_gold",
        deps=[transcripts_silver, *(extra_deps or [])],
        group_name="transcripts",
        kinds={"corpus"},
    )
    def _gold(
        context: dg.AssetExecutionContext,
        corpus: CorpusResource,
        config: TranscriptsDateConfig,
    ) -> dg.MaterializeResult:
        date = _target_date(config)
        corpus.run(
            context,
            "gold",
            "build",
            "--dataset",
            DATASET,
            "--derivative",
            derivative,
            "--date",
            date,
            "--sink-path",
            corpus.sink_path,
        )
        corpus.run(
            context,
            "verify",
            "--dataset",
            derivative,
            "--date",
            date,
            "--tier",
            "gold",
            "--sink-path",
            corpus.sink_path,
        )
        return dg.MaterializeResult(
            metadata={
                "dataset": DATASET,
                "derivative": derivative,
                "tier": "gold",
                "partition": date,
            }
        )

    return _gold


transcripts_videos_gold = _build_gold_asset("transcripts-videos")
transcripts_sections_gold = _build_gold_asset("transcripts-sections")
# Cross-dataset Gold input (ADR-0055, mirroring news-entity-mentions/ADR-0052): the
# vocabulary is extracted from the `sde-*` Gold snapshot trees at the Gold root, and
# the SDE build it was read from is stamped into `_INDEX.json`'s
# `dependency_fingerprint`. The SDE snapshot Gold is therefore a real upstream.
transcripts_entity_mentions_gold = _build_gold_asset(
    "transcripts-entity-mentions", extra_deps=[sde.sde_snapshot_gold]
)

GOLD_ASSETS = (
    transcripts_videos_gold,
    transcripts_sections_gold,
    transcripts_entity_mentions_gold,
)

# --- transcripts-embeddings: enrich → Silver → Gold (corpus ADR-0053) -------
#
# Its own corpus dataset (`kind: enrich`), not a transcripts Gold derivative: the
# model run is non-deterministic, so its output is archived verbatim like a fetch
# and only the parse + join downstream of it are golden-gated. Same three-step shape
# as the rest of the chain, on the same fetch date:
#
#   corpus enrich embed --dataset transcripts-embeddings  → bronze (raw vectors)
#   corpus ingest       --dataset transcripts-embeddings  → silver
#   corpus gold build   --dataset transcripts-embeddings  → gold (`embeddings`)
#
# Upstream is `transcripts_sections_gold` — the `embed_text` column is the
# embedding-input contract — so the assets sit in the `transcripts` group and ride
# the existing `transcripts_daily_schedule` (group selection) with no new schedule.
#
# The embed step SHARES the `news_embed` pool (limit 1) with news-embeddings rather
# than taking its own: both run the same pinned local ONNX model in-process (measured
# ~4.4 GB RSS), so on the 12 GB LXC no two embeds of EITHER dataset may overlap. One
# shared limit-1 pool guarantees that across every launch path — schedule, UI,
# manual (deploy/dagster.yaml, redeploy.sh). An asset holds only one pool, so this
# does not exclude a concurrent `heavy` Gold build (~3 GB floor): worst case
# embed + heavy ≈ 7.4 GB, which fits.
EMBEDDINGS_DATASET = "transcripts-embeddings"
_EMBED_POOL = "news_embed"


class TranscriptsEmbedConfig(dg.Config):
    """Run-config for the embed step: fetch date plus an optional chunk cap.

    ``limit`` caps how many chunks one run embeds so an operator can chunk a large
    historical generation. The step is ledgered (``embedded_chunks``, keyed
    chunk × ``model_rev``), so a capped, partial or interrupted run resumes on the
    next run and a re-run with everything ledgered embeds nothing.
    """

    date: str | None = None
    limit: int | None = None


@dg.asset(
    deps=[transcripts_sections_gold],
    group_name="transcripts",
    kinds={"corpus"},
    pool=_EMBED_POOL,
)
def transcripts_embeddings_bronze(
    context: dg.AssetExecutionContext,
    corpus: CorpusResource,
    config: TranscriptsEmbedConfig,
) -> dg.MaterializeResult:
    """Embed the not-yet-ledgered section chunks; archive the raw vectors (ADR-0053).

    Runs the pinned local ONNX model in-process (offline, CPU) over
    ``gold/transcripts-sections``'s ``embed_text`` and writes the vectors verbatim to
    a keep-forever Bronze partition. The model artifact is located via
    ``CORPUS_EMBEDDING_MODEL_DIR`` (:class:`CorpusResource`); an absent or mismatched
    artifact fails the run loud, never falls back.
    """
    date = _target_date(TranscriptsDateConfig(date=config.date))
    args = [
        "enrich",
        "embed",
        "--dataset",
        EMBEDDINGS_DATASET,
        "--date",
        date,
        "--sink-path",
        corpus.sink_path,
    ]
    if config.limit is not None:
        args += ["--limit", str(config.limit)]
    corpus.run(context, *args)
    return dg.MaterializeResult(
        metadata={"dataset": EMBEDDINGS_DATASET, "tier": "bronze", "partition": date}
    )


@dg.asset(
    deps=[transcripts_embeddings_bronze],
    group_name="transcripts",
    kinds={"corpus"},
)
def transcripts_embeddings_silver(
    context: dg.AssetExecutionContext,
    corpus: CorpusResource,
    config: TranscriptsDateConfig,
) -> dg.MaterializeResult:
    """Parse the archived vector shards into Silver (one row per archived vector)."""
    date = _target_date(config)
    corpus.run(
        context,
        "ingest",
        "--dataset",
        EMBEDDINGS_DATASET,
        "--date",
        date,
        "--sink-path",
        corpus.sink_path,
    )
    corpus.run(
        context,
        "verify",
        "--dataset",
        EMBEDDINGS_DATASET,
        "--date",
        date,
        "--tier",
        "silver",
        "--sink-path",
        corpus.sink_path,
    )
    return dg.MaterializeResult(
        metadata={"dataset": EMBEDDINGS_DATASET, "tier": "silver", "partition": date}
    )


@dg.asset(
    # `transcripts_sections_gold` is a cross-dataset Gold input to this build (the
    # join side: `published_at` / `available_at` / `heading_path`), fingerprinted
    # into `_INDEX.json` — so it is a real upstream here too, not only via Bronze.
    deps=[transcripts_embeddings_silver, transcripts_sections_gold],
    group_name="transcripts",
    kinds={"corpus"},
)
def transcripts_embeddings_gold(
    context: dg.AssetExecutionContext,
    corpus: CorpusResource,
    config: TranscriptsDateConfig,
) -> dg.MaterializeResult:
    """Join the pinned generation's vectors to their section metadata (embeddings).

    Single-derivative dataset, so ``--derivative`` is left off and the Gold verify
    keys on the dataset name. The join is total: a vector whose ``chunk_hash`` is
    absent from the section tree fails the build.
    """
    date = _target_date(config)
    corpus.run(
        context,
        "gold",
        "build",
        "--dataset",
        EMBEDDINGS_DATASET,
        "--date",
        date,
        "--sink-path",
        corpus.sink_path,
    )
    corpus.run(
        context,
        "verify",
        "--dataset",
        EMBEDDINGS_DATASET,
        "--date",
        date,
        "--tier",
        "gold",
        "--sink-path",
        corpus.sink_path,
    )
    return dg.MaterializeResult(
        metadata={"dataset": EMBEDDINGS_DATASET, "tier": "gold", "partition": date}
    )


# --- transcripts-annotations: NEVER wired (manual generation, ADR-0055/t2) ---
#
# There is deliberately NO annotations asset, op, job, or schedule here — and there
# must never be one. The `transcripts-annotations` generation is a MANUAL operator
# run: the agent is the judge, via `corpus enrich pending` → judge → `corpus enrich
# import` (contract `t2`, speculative market outlook — NOT the news `t1`/buff-nerf
# contract). It is driven by the `annotate-transcripts` skill, off the API key, and
# is intentionally excluded from Dagster the same way news keeps `corpus enrich
# annotate` out of its chain. Do not "complete" the wiring by adding an asset — the
# absence is the design.


# --- asset check: listed vs archived (plan §3, mirroring news §1.4) ---------
#
# The corpus seen-ledger records only *archived* transcripts (a caption-less video
# stays retryable and is never ledgered, ADR-0048), and Silver holds exactly the
# videos with an archived transcript (no listing-only rows, unlike news). So the
# only two persistent, corpus-provided counts are: the videos Silver scanned
# (`corpus transcripts match-stats` → `report.videos`) and the ledger's archived
# transcripts (`state query` over `seen_documents`). Their delta is a ledger↔Silver
# reconciliation: 0 in the healthy case (every archived transcript parses into a
# Silver row), non-zero only on drift, which the check surfaces as metadata and
# never enforces. The discovered-but-caption-less cohort the plan calls out is
# observable only per-run in the fetch summary (`videos_in_scope` vs
# `new_documents`) — it is not queryable post-hoc, so it is not part of this
# persistent check.
KNOWN_SCANNED_NOT_ARCHIVED = 0


@dg.asset_check(asset=transcripts_silver, blocking=False)
def transcripts_listed_vs_archived(
    context: dg.AssetCheckExecutionContext, corpus: CorpusResource
) -> dg.AssetCheckResult:
    """Surfaces the Silver-scanned-vs-ledger-archived delta as expected metadata.

    Listed = the videos Silver scanned (``corpus transcripts match-stats`` →
    ``report.videos``). Archived = the transcripts in the corpus seen-ledger
    (``seen_documents``). The difference reconciles the parse against the ledger
    (plan §3); it always passes — the delta is reported, never enforced.
    """
    stats = corpus.transcripts_match_stats()
    listed = int(stats["report"]["videos"])
    rows = corpus.state_query(
        "SELECT count(*) AS archived FROM seen_documents WHERE dataset = 'transcripts'"
    )
    archived = int(rows[0]["archived"]) if rows else 0
    delta = listed - archived
    if delta != KNOWN_SCANNED_NOT_ARCHIVED:
        context.log.info(
            "transcripts scanned-vs-archived delta is %s (expected: %s)",
            delta,
            KNOWN_SCANNED_NOT_ARCHIVED,
        )
    return dg.AssetCheckResult(
        passed=True,
        severity=dg.AssetCheckSeverity.WARN,
        metadata={
            "listed": listed,
            "archived": archived,
            "scanned_not_archived": delta,
            "corpus_basis": str(stats["report"].get("corpus_basis", "")),
            "dependency_fingerprint": str(stats.get("dependency_fingerprint", "")),
        },
    )


class TranscriptsBackfillConfig(dg.Config):
    """Run-config for the historical transcript backfill.

    ``max_videos`` caps how many Supadata transcript calls one run makes — each
    costs a paid credit (~305 videos in scope, shared with the daily fetch's
    quota). Defaults to 90 so a bare run never sweeps uncapped and blows a monthly
    budget; set it higher to spend more per run, or ``null`` for a fully uncapped
    sweep. When the summary reports ``capped: true`` the operator re-runs (next
    billing month) until ``capped: false``.
    """

    max_videos: int | None = 90


@dg.op
def transcripts_backfill_op(
    context: dg.OpExecutionContext,
    corpus: CorpusResource,
    config: TranscriptsBackfillConfig,
) -> None:
    """Shell the resumable historical transcript backfill (``corpus context backfill``).

    Needs ``SUPADATA_API_KEY`` and ``YOUTUBE_API_KEY`` in the process env. Partial
    work is sealed via chunked commits and the seen-ledger, so a failed run
    (non-zero exit → ``dg.Failure``) is retryable: a plain re-run resumes.
    """
    args = [
        "context",
        "backfill",
        "--dataset",
        DATASET,
        "--sink-path",
        corpus.sink_path,
    ]
    if config.max_videos is not None:
        args += ["--max-videos", str(config.max_videos)]
    corpus.run(context, *args)


@dg.job
def transcripts_backfill_job() -> None:
    """Manually-triggered historical transcript backfill (operator-run, resumable)."""
    transcripts_backfill_op()
