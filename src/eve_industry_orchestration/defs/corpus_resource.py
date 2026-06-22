"""Resource wrapping the static `corpus` binary on the Dagster LXC."""

from __future__ import annotations

import json
import os
import subprocess
from typing import Any

import dagster as dg


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

    def _env(self) -> dict[str, str]:
        return {**os.environ, "CORPUS_DATASETS_DIR": self.datasets_dir}

    def run(self, context: dg.AssetExecutionContext, *args: str) -> None:
        """Runs one ``corpus`` subcommand, streaming output to the run log.

        Raises ``dg.Failure`` when the subcommand exits non-zero so the
        partition is marked failed rather than silently materialised.
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
        for line in stream:
            context.log.info(line.rstrip())
        returncode = process.wait()

        if returncode != 0:
            raise dg.Failure(description=f"corpus exited {returncode}: {' '.join(cmd)}")

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
