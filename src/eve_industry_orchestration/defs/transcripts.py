"""transcripts: YouTube feed + Supadata transcripts archived to Bronze (ADR-0046/0048).

A Bronze-only archival dataset, the sibling of :mod:`news`. ``corpus context fetch
--dataset transcripts`` pulls the CCP YouTube channel Atom feed and each new
video's Supadata transcript JSON and archives the bytes verbatim to a
fetch-date-partitioned Bronze tree (``bronze/transcripts/year=/month=/day=/`` with
``_MANIFEST.json`` then ``_DONE`` last). There is no Silver, no Gold, no ``corpus
ingest`` and no coverage gate — the partition is keyed on the **fetch** date,
idempotent on re-run (videos transcribed earlier are skipped via the corpus
seen-ledger).

Orchestration mirrors :mod:`news`: a single non-partitioned asset on a daily
``dg.schedule`` (see :mod:`sensors`) plus a manually-triggered backfill job. No
upstream-listing sensor, no partition matrix.

The fetch hits the YouTube feed + Supadata (neither EVE Ref nor ESI), so the asset
joins no concurrency pool: request pacing (~1 req/s Supadata) lives in the binary.
Secrets come from the process env (ADR-0047, Doppler retired), passed through to
the subprocess by :class:`CorpusResource`:

- ``SUPADATA_API_KEY`` — the daily fetch **and** the backfill (Supadata transcript
  calls cost paid credits).
- ``YOUTUBE_API_KEY`` — the backfill only (enumerating the channel's full upload
  history).
"""

import dagster as dg

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
