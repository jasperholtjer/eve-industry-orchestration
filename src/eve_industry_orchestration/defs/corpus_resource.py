"""Resource wrapping the static `corpus` binary on the Dagster LXC."""

from __future__ import annotations

import json
import os
import re
import subprocess
from collections import deque
from typing import Any

import dagster as dg

# Lines of the corpus subprocess's tail to attach to a Failure, so the real
# error (e.g. `Error: parse / Caused by: schema mismatch ...`) surfaces in the
# Dagster Failure instead of only the command line.
_FAILURE_TAIL_LINES = 20

# Dataset and Gold-derivative names as corpus spells them: lowercase, digits, and
# hyphens. The only shape allowed into a state-query WHERE clause.
_DATASET_NAME = re.compile(r"[a-z0-9][a-z0-9-]*")


def _parse_status_line(line: str) -> dict[str, Any] | None:
    """Parses a ``corpus ingest`` status object off one log line, else ``None``.

    Human progress goes to stderr and the status JSON to stdout, but ``run``
    merges both streams; scanning per line for a ``{"status": ...}`` object picks
    the status out of the interleaved output without coupling to line order.
    """
    line = line.strip()
    if not line.startswith("{") or '"status"' not in line:
        return None
    try:
        obj = json.loads(line)
    except json.JSONDecodeError:
        return None
    if isinstance(obj, dict) and "status" in obj:
        return obj
    return None


class CorpusResource(dg.ConfigurableResource):
    """Thin wrapper around the static ``corpus`` binary.

    The binary owns the ingest -> Silver -> Gold compute and writes the
    ``parquet + _INDEX.json + _DONE`` contract to the NFS sink. This resource
    only shells out to its subcommands and surfaces their output; it holds no
    compute or run-state of its own. Streaming calls (``run``) feed Silver/Gold
    materialisations; capturing calls (``everef_missing_partitions``,
    ``state_query``) feed sensors that read JSON off stdout.
    """

    binary_path: str = "/usr/local/bin/corpus"
    datasets_dir: str
    """Resolves dataset YAML configs; passed through as ``CORPUS_DATASETS_DIR``."""
    sink_path: str
    """NFS mount the contract is written to (e.g. ``/mnt/eve``)."""
    embedding_model_dir: str = ""
    """Local ONNX snapshot dir for ``corpus enrich embed`` (corpus ADR-0053).

    Passed through as ``CORPUS_EMBEDDING_MODEL_DIR``; deployment, never contract —
    the path is provisioned on the host (systemd unit), never hardcoded here. Empty
    ⇒ not exported, and ``corpus enrich embed`` fails loud on the absent artifact
    rather than falling back to an unlabeled generation.
    """

    def _env(self) -> dict[str, str]:
        env = {**os.environ, "CORPUS_DATASETS_DIR": self.datasets_dir}
        if self.embedding_model_dir:
            env["CORPUS_EMBEDDING_MODEL_DIR"] = self.embedding_model_dir
        return env

    def run(
        self,
        context: dg.AssetExecutionContext | dg.OpExecutionContext,
        *args: str,
    ) -> dict[str, Any] | None:
        """Runs one ``corpus`` subcommand, streaming output to the run log.

        Raises ``dg.Failure`` when the subcommand exits non-zero so the
        partition is marked failed rather than silently materialised.

        Returns the machine-readable status object ``corpus ingest`` prints on
        stdout (``{"status": "written"|"skipped", ...}``, ADR-0028) when present,
        else ``None`` (subcommands without a status line). A ``skipped`` status
        is a genuinely-absent upstream day — exit 0, no partition written — which
        the caller turns into a left-Missing partition, not a failure.
        """
        cmd = [self.binary_path, *args]
        context.log.info("corpus: %s", " ".join(cmd))

        process = subprocess.Popen(  # noqa: S603 — fixed binary, no shell
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            env=self._env(),
        )
        stream = process.stdout
        if stream is None:  # pragma: no cover - PIPE always yields a stream
            raise dg.Failure(description="corpus produced no stdout stream")
        tail: deque[str] = deque(maxlen=_FAILURE_TAIL_LINES)
        status: dict[str, Any] | None = None
        try:
            for line in stream:
                stripped = line.rstrip()
                context.log.info(stripped)
                tail.append(stripped)
                parsed = _parse_status_line(stripped)
                if parsed is not None:
                    status = parsed
            returncode = process.wait()
        finally:
            # On a Dagster interrupt (run cancelled / daemon restart) the loop
            # raises mid-stream; without this the corpus subprocess is orphaned,
            # holding the run-state SQLite lock and possibly writing a partial
            # partition. Killing it is safe under the contract: `_DONE` is
            # written last, so a half-written partition has no `_DONE` and reads
            # as absent (ADR-0009). A no-op once corpus has already exited.
            if process.poll() is None:
                process.kill()
                process.wait()

        if returncode != 0:
            description = f"corpus exited {returncode}: {' '.join(cmd)}"
            detail = "\n".join(tail).strip()
            if detail:
                description = f"{description}\n{detail}"
            raise dg.Failure(description=description)
        return status

    def _capture(self, *args: str) -> str:
        """Runs a ``corpus`` subcommand and returns its stdout.

        Raises ``dg.Failure`` on non-zero exit, attaching captured stderr so
        the sensor tick fails loudly instead of parsing empty output.
        """
        cmd = [self.binary_path, *args]
        result = subprocess.run(  # noqa: S603 — fixed binary, no shell
            cmd,
            capture_output=True,
            text=True,
            env=self._env(),
            check=False,
        )
        if result.returncode != 0:
            raise dg.Failure(
                description=f"corpus exited {result.returncode}: {' '.join(cmd)}\n"
                f"{result.stderr.strip()}",
            )
        return result.stdout

    def _capture_json(self, *args: str) -> Any:
        out = self._capture(*args)
        try:
            return json.loads(out)
        except json.JSONDecodeError as exc:
            joined = " ".join(args)
            raise dg.Failure(
                description=f"corpus emitted non-JSON output for {joined}: {exc}",
            ) from exc

    def everef_missing_partitions(
        self, dataset: str, *, window_days: int | None = None
    ) -> dict[str, Any]:
        """Returns the EVE Ref availability diff for a dataset as a dict.

        Wraps ``corpus everef missing-partitions``, which diffs upstream
        availability against the local ``partitions`` table (SQLite run-state).
        The ``missing`` key holds the dates available upstream but not yet
        ingested locally.
        """
        args = [
            "everef",
            "missing-partitions",
            "--dataset",
            dataset,
            "--sink-path",
            self.sink_path,
            "--format",
            "json",
        ]
        if window_days is not None:
            args += ["--window-days", str(window_days)]
        return self._capture_json(*args)

    def gold_ready_dates(
        self, dataset: str, *, derivative: str | None = None
    ) -> dict[str, Any]:
        """Returns the dates whose Gold partition is ready to build as a dict.

        Wraps ``corpus gold ready-dates``, which reads the run-state
        ``partitions`` table and reports dates whose target-day Silver is
        present, whose look-back window meets ``coverage_min_ratio`` (windowed
        shapes only), and whose Gold partition is not yet built. The ``ready``
        key holds that date list (alongside ``derivative`` / ``served_start``).

        Pass ``derivative`` for a multi-derivative dataset (ADR-0025); a
        single-derivative dataset (market-history) resolves it automatically and
        leaves the flag off.
        """
        args = [
            "gold",
            "ready-dates",
            "--dataset",
            dataset,
            "--sink-path",
            self.sink_path,
            "--format",
            "json",
        ]
        if derivative is not None:
            args += ["--derivative", derivative]
        return self._capture_json(*args)

    def killmails_freshness(self, dataset: str = "killmails") -> list[dict[str, Any]]:
        """Returns the served Silver days whose upstream kill count has changed.

        Wraps ``corpus killmails freshness`` (corpus ADR-0060). Killmail days are
        the corpus's only **mutable** partitions — zKillboard keeps discovering
        old kills and EVE Ref re-archives the day with more members — so ``_DONE``
        plus a source sha256 is not a complete freshness contract there. The
        binary fetches upstream's root ``totals.json`` and diffs it against the
        count each partition recorded at ingest; it is read-only and does **not**
        re-ingest. Each row holds ``date``, ``ingested_count`` (``null`` for a
        partition ingested before the token existed) and ``upstream_count``.

        Returns a bare JSON array, not an envelope — that is the sensor contract.
        """
        return self._capture_json(
            "killmails",
            "freshness",
            "--dataset",
            dataset,
            "--sink-path",
            self.sink_path,
            "--format",
            "json",
        )

    def stale_gold_dates(self, dataset: str, derivative: str) -> list[str]:
        """Returns dates whose Gold was built before its Silver was last written.

        ``corpus gold ready-dates`` reports only dates whose Gold does **not yet**
        exist, so a day whose Silver is re-ingested after its Gold was built is
        never re-proposed — the Gold silently keeps serving the superseded Silver.
        That is a real gap for ``killmails``, whose partitions mutate by design
        (corpus ADR-0060), and it is invisible to every other signal.

        The run-state ``partitions`` table already answers it: ``last_seen_at`` is
        stamped on every upsert, so ``silver.last_seen_at > gold.last_seen_at``
        means "Silver was rewritten after this Gold was built". On the normal path
        Gold always follows its Silver, so nothing is reported; after a repair
        ingest exactly the repaired days are. Self-healing and stateless — it also
        catches a manual re-ingest or a parser-fix backfill, not just drift.

        Read-only, through the sanctioned ``state query`` seam: the decision this
        drives is *which run to request*, which is orchestration's job.
        """
        # `state query` takes a SQL string over a CLI boundary — there is no
        # parameter binding to bind to — so the two interpolated names are
        # validated as plain dataset/derivative identifiers first. Both are
        # module constants today; the guard keeps that true if a caller ever
        # threads a value through.
        for name in (dataset, derivative):
            if not _DATASET_NAME.fullmatch(name):
                raise dg.Failure(
                    description=f"refusing to build a state query for {name!r}: "
                    "dataset and derivative names are [a-z0-9-] identifiers",
                )
        sql = (
            # `substr(…, 6)` strips the `date=` partition-key prefix.
            "SELECT substr(s.partition_key, 6) AS date "  # noqa: S608 — names validated above
            "FROM partitions s JOIN partitions g "
            "ON g.partition_key = s.partition_key "
            f"WHERE s.dataset = '{dataset}' AND s.tier = 'silver' "
            f"AND g.dataset = '{derivative}' AND g.tier = 'gold' "
            "AND s.last_seen_at > g.last_seen_at "
            "ORDER BY s.partition_key"
        )
        return [row["date"] for row in self.state_query(sql) if row.get("date")]

    def stale_changelog_builds(self) -> list[int]:
        """Returns SDE builds whose changelog Gold was diffed across a hole.

        ``corpus sde gold`` diffs a build against "the largest **committed**
        Silver build below it". The Gold sensor asks for a build as soon as *any*
        smaller build has committed Silver, so while the sequence has a hole —
        300 commits before 200, both ingesting under ``everef_download`` — 300 is
        diffed against 100 and the 200→300 link never exists. Committed Gold is
        subtracted from the outstanding set, so nothing re-proposes 300.

        The run-state ``partitions`` table answers it without the recorded
        predecessor: ``last_seen_at`` is stamped on every upsert, so a changelog
        Gold whose ``last_seen_at`` predates that of the **nearest** lower
        committed ``sde`` Silver was built against a different predecessor than
        the binary would pick now. Only the nearest one matters, which makes the
        set exact against false positives on commit ordering — no cascade — but
        not exact against content: a plain re-ingest of an unchanged lower
        Silver re-stamps ``last_seen_at`` and flags the changelog above it too,
        even though nothing it diffs against actually changed; the resulting
        rebuild is idempotent and self-clearing. It also isn't exact against
        false negatives from run-timing: the comparison is against Gold's own
        *write* time, while the binary picks its predecessor at Gold *run
        start*, so a lower Silver that commits mid-run leaves Gold's
        ``last_seen_at`` newer and the hole undetected here — the deferral rule
        that gates when a build is proposed is what closes that window, not
        this query. A baseline build has no lower Silver, so the subquery
        yields NULL and the row drops out on its own.

        Repair is a plain rematerialise: Gold overwrites in place and the binary
        recomputes the predecessor from currently committed Silver.

        Read-only, through the sanctioned ``state query`` seam — no path is
        constructed and no parquet is opened. The build number is the key
        throughout; ``release_date`` is a label only (three of them exist per
        build and they disagree), so ordering never turns on it.
        """
        sql = (
            # `substr(…, 7)` strips the `build=` partition-key prefix.
            "SELECT DISTINCT CAST(substr(g.partition_key, 7) AS INTEGER) AS build "
            "FROM partitions g "
            "WHERE g.dataset = 'sde-changelog' AND g.tier = 'gold' "
            "AND g.last_seen_at < ("
            "SELECT s.last_seen_at FROM partitions s "
            "WHERE s.dataset = 'sde' AND s.tier = 'silver' "
            "AND CAST(substr(s.partition_key, 7) AS INTEGER) "
            "< CAST(substr(g.partition_key, 7) AS INTEGER) "
            "ORDER BY CAST(substr(s.partition_key, 7) AS INTEGER) DESC "
            "LIMIT 1)"
        )
        return sorted(
            int(row["build"])
            for row in self.state_query(sql)
            if row.get("build") is not None
        )

    def everef_list_builds(self, dataset: str) -> list[dict[str, Any]]:
        """Returns discovered upstream builds for a build-versioned dataset.

        Wraps ``corpus everef list`` (ADR-0031), which lists upstream archives
        rather than days for the ``build-versioned`` layout (SDE). Each row holds
        ``build`` (the partition identity), ``release_date`` (the Hive path), and
        ``url`` / ``size``. Drives the SDE build-discovery sensor.
        """
        return self._capture_json(
            "everef",
            "list",
            "--dataset",
            dataset,
            "--sink-path",
            self.sink_path,
            "--format",
            "json",
        )

    def everef_list_reports(self, dataset: str) -> list[dict[str, Any]]:
        """Returns discovered upstream report-months for a monthly-archive dataset.

        Wraps ``corpus everef list`` (corpus ADR-0058), which lists report-months
        (not days) for the ``monthly-archive`` layout (MER). Each row holds
        ``report_month`` (``YYYY-MM-01``, the partition identity), ``url``,
        ``filename``, ``size``, and ``last_modified``. Drives the MER
        report-discovery sensor. ``--sink-path`` is accepted but unused by the
        monthly-archive discovery (network-only), matching ``everef_list_builds``.
        """
        return self._capture_json(
            "everef",
            "list",
            "--dataset",
            dataset,
            "--sink-path",
            self.sink_path,
            "--format",
            "json",
        )

    def news_match_stats(self) -> dict[str, Any]:
        """Returns the news entity-mention tuning report as a dict.

        Wraps ``corpus news match-stats`` (corpus ADR-0052), which scans every
        ``_DONE``-sealed ``silver/news`` partition against the SDE snapshot
        vocabulary at the Gold root and prints its JSON report. ``stats.articles``
        is the number of articles Silver holds (latest version per slug) — the
        *listed* side of the listed-vs-archived delta; the ledger (``state
        query`` over ``seen_documents``) is the *archived* side.
        """
        return self._capture_json("news", "match-stats", "--sink-path", self.sink_path)

    def transcripts_match_stats(self) -> dict[str, Any]:
        """Returns the transcripts case-rule match report as a dict.

        Wraps ``corpus transcripts match-stats`` (corpus ADR-0055 §4c), which scans
        every ``_DONE``-sealed ``silver/transcripts`` partition against the SDE
        snapshot vocabulary at the Gold root and prints its JSON report.
        ``report.videos`` is the number of videos Silver scanned (each has an
        archived transcript) — the *listed* side of the listed-vs-archived delta;
        the ledger (``state query`` over ``seen_documents``) is the *archived* side.
        """
        return self._capture_json(
            "transcripts", "match-stats", "--sink-path", self.sink_path
        )

    def state_query(self, sql: str) -> list[dict[str, Any]]:
        """Runs a read-only ``corpus state query`` and returns the JSON rows."""
        return self._capture_json(
            "state",
            "query",
            "--sql",
            sql,
            "--sink-path",
            self.sink_path,
            "--format",
            "json",
        )
