"""Delete Dagster runs older than the retention window, and their compute logs.

The half of storage retention Dagster cannot do itself. `retention:` in
`deploy/dagster.yaml` purges schedule and sensor *ticks*; there is no equivalent
setting for *runs* in Dagster OSS, so a run's event log (`history/runs/<id>.db`,
one SQLite file each) and its compute logs (`storage/<id>/`) live forever unless
something deletes them. On 2026-09-04 that filled the LXC's 20 GiB root: 65k runs,
12 GiB of per-run event logs, and every UI query failing with
`sqlite3.OperationalError: disk I/O error` because SQLite could not write its WAL.

Run ids come from a read-only query against `runs.db` rather than from
`DagsterInstance.get_runs(filters=RunsFilter(created_before=...))`: on the
deployed Dagster that filter raises `TypeError` inside the record type-checker
before it ever reaches storage. The cutoff is SQLite's own
`datetime('now', '-N days')`, which is UTC and matches how SQLAlchemy stores
`create_timestamp` — comparing against a Python `isoformat()` string would differ
at the date/time separator and silently select the wrong side.

`delete_run` is not enough on its own, and that is the whole point of this
script. It deletes the run row and DELETEs the rows inside the run's event-log
shard, but it never unlinks the shard file — only `wipe()`, which is
all-or-nothing, does that, and a SQLite file does not shrink when its rows go.
An emptied 190 KiB shard still costs 190 KiB, times 65k runs. So the two trees a
run owns outside `runs.db` are removed here by run id: its event-log shard
(`history/runs/<id>.db`, plus any `-wal`/`-shm`) and its compute logs
(`storage/<id>/`). Keyed on the run id, so nothing is deleted that the run did
not own.

No VACUUM: the space actually reclaimed is whole files, and `runs.db`/`index.db`
reuse their freed pages rather than growing without bound.

Invoked weekly by `dagster-purge.timer`; safe to run by hand:

    DAGSTER_HOME=/var/lib/dagster PURGE_AFTER_DAYS=30 \
      /opt/eve-industry-orchestration/.venv/bin/python deploy/purge_runs.py
"""

from __future__ import annotations

import os
import shutil
import sqlite3
from pathlib import Path

DEFAULT_PURGE_AFTER_DAYS = 30


def stale_run_ids(runs_db: Path, days: int) -> list[str]:
    """Run ids created more than `days` ago, oldest first.

    Opens read-only: a full disk is the failure this script exists to prevent,
    and a read-only connection needs no journal to answer the query.
    """
    uri = f"file:{runs_db.as_posix()}?mode=ro"
    with sqlite3.connect(uri, uri=True) as con:
        rows = con.execute(
            "select run_id from runs"
            " where create_timestamp < datetime('now', ?)"
            " order by create_timestamp",
            (f"-{days} days",),
        ).fetchall()
    return [row[0] for row in rows]


def purge_run_files(home: Path, run_id: str) -> None:
    """Unlink what `delete_run` empties but leaves on disk.

    The event-log shard and its WAL/SHM sidecars, and the compute-log tree. All
    named after the run id, so this cannot reach a run that is being kept.
    """
    shard = home / "history" / "runs" / f"{run_id}.db"
    for path in (shard, shard.with_suffix(".db-wal"), shard.with_suffix(".db-shm")):
        path.unlink(missing_ok=True)
    shutil.rmtree(home / "storage" / run_id, ignore_errors=True)


def main() -> None:
    days = int(os.environ.get("PURGE_AFTER_DAYS", DEFAULT_PURGE_AFTER_DAYS))
    home = Path(os.environ["DAGSTER_HOME"])

    ids = stale_run_ids(home / "history" / "runs.db", days)
    print(f"purging {len(ids)} runs older than {days} days", flush=True)

    # Imported here so `stale_run_ids` stays importable by the test without
    # pulling in the whole Dagster instance machinery.
    from dagster import DagsterInstance

    with DagsterInstance.get() as instance:
        for n, run_id in enumerate(ids, 1):
            instance.delete_run(run_id)
            purge_run_files(home, run_id)
            if n % 500 == 0:
                print(f"  {n}/{len(ids)}", flush=True)

    print(f"purged {len(ids)} runs", flush=True)


if __name__ == "__main__":
    main()
