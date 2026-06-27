"""Resource shelling out to the ``eve-serving`` loader on the DB-VM over SSH.

The serving tier (Postgres ``eve`` + Neo4j on the DB-VM) owns *how* a load works;
this orchestrator owns *when*. It triggers loads by running the idempotent
``eve-serving load`` CLI over SSH, the same thin-shim pattern as
:class:`~eve_industry_orchestration.defs.corpus_resource.CorpusResource` — shell
out, stream the output into the run log, fail the asset on a non-zero exit, and
surface the loader's ``loaded``/``skipped`` summary as metadata.

The DB-VM has a PATH wrapper ``eve-serving`` that sources its own environment
(Gold root, DB DSNs), so a bare ``eve-serving load ...`` over SSH is fully
configured — orchestration never reaches into the databases directly and bakes no
credentials in code; it relies on the corpus account's existing authorized SSH
key for ``serving@<host>``.
"""

from __future__ import annotations

import re
import subprocess
from collections import deque
from typing import Any

import dagster as dg

# Lines of the loader's tail attached to a Failure, so the real error surfaces in
# the Dagster Failure instead of only the SSH command line.
_FAILURE_TAIL_LINES = 20

# The loader's summary line ends in `... loaded: <n> rows` or `... skipped: 0 rows`
# (idempotent on the partition's parquet_sha256). Scan per line for the last such
# match without coupling to exact line position.
_SUMMARY_RE = re.compile(r"\b(loaded|skipped)\b[^\d]*?(\d+)\s*rows", re.IGNORECASE)


def _parse_summary(line: str) -> dict[str, Any] | None:
    """Parses an ``eve-serving`` ``loaded``/``skipped`` summary off one line."""
    match = _SUMMARY_RE.search(line)
    if match is None:
        return None
    return {"action": match.group(1).lower(), "rows": int(match.group(2))}


class ServingResource(dg.ConfigurableResource):
    """Thin wrapper around the remote ``eve-serving load`` CLI over SSH.

    The loader is idempotent on each Gold partition's ``parquet_sha256``: an
    unchanged partition re-loads as a no-op (``skipped: 0 rows``). This resource
    only triggers it and surfaces its output; it holds no DB connection or
    load-state of its own.
    """

    host: str = "192.168.2.212"
    """DB-VM the serving tier runs on."""
    user: str = "serving"
    """SSH account with the loader on PATH (corpus holds its authorized key)."""
    ssh_binary: str = "ssh"
    """SSH client to exec; overridden in tests with a fake launcher."""
    remote_command: str = "eve-serving"
    """PATH wrapper on the DB-VM that sources the loader's own env."""

    def load(
        self, context: dg.AssetExecutionContext, dataset: str, *flags: str
    ) -> dict[str, Any]:
        """Triggers one ``eve-serving load --dataset <dataset> [flags]`` over SSH.

        Streams the loader's stdout/stderr into the run log. Raises ``dg.Failure``
        on a non-zero exit so the asset is marked failed rather than silently
        materialised. Returns the parsed ``{"action", "rows"}`` summary (``action``
        is ``loaded`` or ``skipped``); ``rows`` is ``None`` when the loader emitted
        no recognisable summary line.
        """
        cmd = [
            self.ssh_binary,
            f"{self.user}@{self.host}",
            self.remote_command,
            "load",
            "--dataset",
            dataset,
            *flags,
        ]
        context.log.info("eve-serving: %s", " ".join(cmd))

        process = subprocess.Popen(  # noqa: S603 — fixed binary, no shell
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        stream = process.stdout
        if stream is None:  # pragma: no cover - PIPE always yields a stream
            raise dg.Failure(description="eve-serving produced no stdout stream")
        tail: deque[str] = deque(maxlen=_FAILURE_TAIL_LINES)
        summary: dict[str, Any] | None = None
        try:
            for line in stream:
                stripped = line.rstrip()
                context.log.info(stripped)
                tail.append(stripped)
                parsed = _parse_summary(stripped)
                if parsed is not None:
                    summary = parsed
            returncode = process.wait()
        finally:
            # On a Dagster interrupt the loop raises mid-stream; without this the
            # SSH subprocess is orphaned. The remote load is idempotent, so a
            # killed-then-retried load re-converges. A no-op once SSH has exited.
            if process.poll() is None:
                process.kill()
                process.wait()

        if returncode != 0:
            description = f"eve-serving exited {returncode}: {' '.join(cmd)}"
            detail = "\n".join(tail).strip()
            if detail:
                description = f"{description}\n{detail}"
            raise dg.Failure(description=description)
        return summary or {"action": None, "rows": None}
