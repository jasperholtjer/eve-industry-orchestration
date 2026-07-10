"""news: CCP news RSS + article HTML archived to Bronze (corpus ADR-0045/0048).

A Bronze-only archival dataset. ``corpus context fetch --dataset news`` pulls the
CCP news RSS feed and each linked article's raw HTML and archives the bytes
verbatim to a fetch-date-partitioned Bronze tree
(``bronze/news/year=/month=/day=/`` with ``_MANIFEST.json`` then ``_DONE`` last).
There is no Silver, no Gold, no ``corpus ingest`` and no coverage gate — the
partition is keyed on the **fetch** date (one dense partition per daily run),
idempotent on re-run (documents archived earlier are skipped via the corpus
seen-ledger).

Orchestration is therefore the simplest shape in the repo: a single
non-partitioned asset driven by a daily ``dg.schedule`` (see :mod:`sensors`), plus
a manually-triggered backfill job for the historical sweep. There is no
upstream-listing sensor and no partition matrix — the binary decides "today" and
dedups its own work, so nothing per-date exists for Dagster to diff.

The fetch hits CCP's news feed (neither EVE Ref nor ESI), so the asset joins no
concurrency pool: request pacing (~0.7 s between article fetches) lives in the
binary. The daily ``news`` fetch needs no secret; the historical backfill reads
``CONTENTFUL_DELIVERY_TOKEN`` from the process env (ADR-0047, Doppler retired),
passed through to the subprocess by :class:`CorpusResource`.
"""

import dagster as dg

from eve_industry_orchestration.defs.corpus_resource import CorpusResource

DATASET = "news"


@dg.asset(
    group_name="news",
    kinds={"corpus"},
)
def news_bronze(
    context: dg.AssetExecutionContext, corpus: CorpusResource
) -> dg.MaterializeResult:
    """Fetch today's CCP news RSS + article HTML into a Bronze partition.

    Non-partitioned: each run archives the current fetch-date partition
    (``bronze/news/year=/month=/day=/``). Idempotent — a same-day re-run overwrites
    the identical set and skips already-archived documents via the corpus
    seen-ledger. The binary owns the fetch, the manifest, and the atomic
    ``_MANIFEST.json`` + ``_DONE`` seal; the asset only shells out and records the
    run.
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


class NewsBackfillConfig(dg.Config):
    """Run-config for the historical news backfill.

    ``max_articles`` caps the ~2,700-article sweep (uncapped ≈ 45 min) so an
    operator can chunk it. Omitted ⇒ uncapped. When the summary reports
    ``capped: true`` the operator re-runs until ``capped: false``.
    """

    max_articles: int | None = None


@dg.op
def news_backfill_op(
    context: dg.OpExecutionContext, corpus: CorpusResource, config: NewsBackfillConfig
) -> None:
    """Shell the resumable historical news backfill (``corpus context backfill``).

    Partial work is sealed via chunked commits and the seen-ledger, so a failed
    run (non-zero exit → ``dg.Failure``) is retryable: a plain re-run resumes.
    """
    args = [
        "context",
        "backfill",
        "--dataset",
        DATASET,
        "--sink-path",
        corpus.sink_path,
    ]
    if config.max_articles is not None:
        args += ["--max-articles", str(config.max_articles)]
    corpus.run(context, *args)


@dg.job
def news_backfill_job() -> None:
    """Manually-triggered historical news backfill (operator-run, resumable)."""
    news_backfill_op()
