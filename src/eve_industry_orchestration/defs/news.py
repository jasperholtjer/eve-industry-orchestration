"""news: CCP news Bronze → Silver → four Gold trees (corpus ADR-0045/0050/0051/0052).

``corpus context fetch --dataset news`` pulls the CCP news RSS feed and each linked
article's raw HTML and archives the bytes verbatim to a fetch-date-partitioned
Bronze tree (``bronze/news/year=/month=/day=/`` with ``_MANIFEST.json`` then
``_DONE`` last), re-fetching articles inside a 90-day trailing window so
post-publication edits are captured (ADR-0051 — the window lives in the binary).
``corpus ingest`` parses that partition into bitemporal Silver (ADR-0050) and four
``corpus gold build`` derivatives fan out of it (ADR-0050/0052):

- ``news-articles`` (``articles``) — the canonical per-article table.
- ``news-sections`` (``text-sections``) — heading-bounded chunks + ``embed_text``.
- ``news-entity-mentions`` (``entity-mentions``) — SDE-name + war-keyword matches.
  Its vocabulary is a **cross-dataset Gold input**: the ``sde-*`` snapshot trees at
  the Gold root, fingerprinted into ``_INDEX.json``'s ``dependency_fingerprint`` —
  hence the extra dep on the SDE snapshot Gold.
- ``news-events`` (``event-calendar``) — the known-future covariate table.

Every tier is keyed on the **fetch** date (one dense partition per daily run) and
each Gold partition derives from that day's Silver alone, so the whole chain is
non-partitioned in Dagster and runs off one daily schedule (see :mod:`sensors`);
a past fetch date is re-processed via the ``NewsDateConfig`` run-config, not via a
partition matrix. A manually-triggered backfill job covers the historical sweep.
The corpus binary decides "today", dedups via its seen-ledger, and owns every
byte; the assets only shell out and record the run.

The fetch hits CCP's news feed (neither EVE Ref nor ESI), so the asset joins no
concurrency pool: request pacing (~0.7 s between article fetches) lives in the
binary. Neither the daily fetch nor the backfill needs a secret — the backfill
discovers the Contentful Content Delivery token from the public site bundle
itself (corpus ADR-0049), so no ``CONTENTFUL_DELIVERY_TOKEN`` env var is required.
"""

import datetime as dt

import dagster as dg

from eve_industry_orchestration.defs import sde
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


# --- Silver + Gold (corpus ADR-0050/0052) ----------------------------------
#
# Silver and Gold mirror Bronze's *fetch-date* partitioning: `corpus ingest
# --dataset news --date D` parses the Bronze partition of fetch-date D, and each
# Gold derivative is a pure function of that same day's Silver alone (no look-back,
# no coverage gate — a quiet news day is a legal 0-row partition, ADR-0050). So
# the chain is non-partitioned in Dagster, exactly like `news_bronze`: every run
# processes the fetch date the run itself just archived. `NewsDateConfig.date`
# re-runs a past fetch date (e.g. the 2026-07-10 backfill partition) without a
# partition matrix — `news.yaml` declares no `served_start`, so there is no
# config-owned start date to anchor one with, and hardcoding one is forbidden.


class NewsDateConfig(dg.Config):
    """Run-config for the fetch-date the Silver/Gold chain processes.

    Defaults to today (UTC) — the partition ``news_bronze`` just archived. Set
    ``date`` (``YYYY-MM-DD``) to re-process an earlier fetch date, e.g. the
    historical backfill partition.
    """

    date: str | None = None


def _target_date(config: NewsDateConfig) -> str:
    """Resolves the fetch date to process: the run-config date, else today (UTC)."""
    if config.date is not None:
        return config.date
    return dt.datetime.now(dt.UTC).date().isoformat()


@dg.asset(
    deps=[news_bronze],
    group_name="news",
    kinds={"corpus"},
)
def news_silver(
    context: dg.AssetExecutionContext, corpus: CorpusResource, config: NewsDateConfig
) -> dg.MaterializeResult:
    """Parse one fetch-date Bronze partition into bitemporal Silver (ADR-0050).

    One row per fetched *version* of an article (``[slug, content_hash]``);
    listed-but-never-served slugs keep a null-body row. The binary owns the parse,
    the schema and the ``_DONE`` seal; the asset shells out and verifies.
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
    """Builds one non-partitioned Gold asset for a news derivative (ADR-0025).

    Verify keys on the derivative name, not the dataset: ``corpus gold build``
    writes ``gold/<derivative>/...`` and ``corpus verify --tier gold`` resolves
    ``gold/<--dataset>/...``.
    """

    @dg.asset(
        name=f"{derivative.replace('-', '_')}_gold",
        deps=[news_silver, *(extra_deps or [])],
        group_name="news",
        kinds={"corpus"},
    )
    def _gold(
        context: dg.AssetExecutionContext,
        corpus: CorpusResource,
        config: NewsDateConfig,
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


news_articles_gold = _build_gold_asset("news-articles")
news_sections_gold = _build_gold_asset("news-sections")
# Cross-dataset Gold input (ADR-0052): the vocabulary is extracted from the `sde-*`
# Gold snapshot trees at the Gold root, and the SDE build it was read from is
# stamped into `_INDEX.json`'s `dependency_fingerprint`. The SDE snapshot Gold is
# therefore a real upstream, not just lineage decoration.
news_entity_mentions_gold = _build_gold_asset(
    "news-entity-mentions", extra_deps=[sde.sde_snapshot_gold]
)
news_events_gold = _build_gold_asset("news-events")

GOLD_ASSETS = (
    news_articles_gold,
    news_sections_gold,
    news_entity_mentions_gold,
    news_events_gold,
)

# Extension points (design §5/§7, corpus Phases 5-6): `news_embeddings_bronze/
# silver/gold` (`corpus enrich embed` → archived vectors → `embeddings_v1` Gold)
# and `news_annotations` (`corpus enrich annotate`). Neither subcommand exists in
# the binary yet, so no asset is defined for them — they land here, downstream of
# `news_sections_gold` (the embedding-input contract), once corpus ships them.


# --- asset check: listed vs archived (design §1.4) --------------------------

# Articles listed by CCP's Contentful collection but never served as HTML: seven
# slugs verified to return HTTP 500 at the source itself (2026-07-11), i.e. broken
# or unpublished at CCP, not a fetch failure. They are the *record*, not an error:
# Silver keeps a listing-only null-body row per slug. The check surfaces the delta
# as metadata and never fails the run — a growing delta is news, not a defect.
KNOWN_LISTED_NOT_ARCHIVED = 7


@dg.asset_check(asset=news_silver, blocking=False)
def news_listed_vs_archived(
    context: dg.AssetCheckExecutionContext, corpus: CorpusResource
) -> dg.AssetCheckResult:
    """Surfaces the listed-vs-archived article delta as expected metadata.

    Listed = the articles Silver holds (``corpus news match-stats``, latest version
    per slug, listing-only rows included). Archived = the documents in the corpus
    seen-ledger (``seen_documents``), i.e. article HTML that was actually served.
    The difference is the listed-but-never-archived cohort (§1.4). Always passes:
    the delta is reported, never enforced.
    """
    stats = corpus.news_match_stats()
    listed = int(stats["stats"]["articles"])
    rows = corpus.state_query(
        "SELECT count(*) AS archived FROM seen_documents WHERE dataset = 'news'"
    )
    archived = int(rows[0]["archived"]) if rows else 0
    delta = listed - archived
    if delta != KNOWN_LISTED_NOT_ARCHIVED:
        context.log.info(
            "news listed-vs-archived delta is %s (known cohort: %s)",
            delta,
            KNOWN_LISTED_NOT_ARCHIVED,
        )
    return dg.AssetCheckResult(
        passed=True,
        severity=dg.AssetCheckSeverity.WARN,
        metadata={
            "listed": listed,
            "archived": archived,
            "listed_not_archived": delta,
            "known_cohort": KNOWN_LISTED_NOT_ARCHIVED,
            "dependency_fingerprint": str(stats.get("dependency_fingerprint", "")),
        },
    )


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
