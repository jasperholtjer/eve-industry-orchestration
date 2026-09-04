"""The cutoff in `deploy/purge_runs.py` selects the right side of the window.

Only `stale_run_ids` is worth a test: the rest of the script is `delete_run` in a
loop. This one query is where the script can be silently, destructively wrong —
a cutoff compared in the wrong format either deletes nothing (the disk fills
again) or deletes everything (the run history is gone). It is pinned against
timestamps written the way SQLAlchemy writes a `DateTime` column on SQLite,
which is the format Dagster's `runs` table actually holds.
"""

from __future__ import annotations

import importlib.util
import sqlite3
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

_SPEC = importlib.util.spec_from_file_location(
    "purge_runs", Path(__file__).parents[1] / "deploy" / "purge_runs.py"
)
assert _SPEC is not None and _SPEC.loader is not None
purge_runs = importlib.util.module_from_spec(_SPEC)
sys.modules["purge_runs"] = purge_runs
_SPEC.loader.exec_module(purge_runs)


def sqlalchemy_timestamp(moment: datetime) -> str:
    """How SQLAlchemy renders a `DateTime` into SQLite: naive UTC, space separator."""
    return moment.replace(tzinfo=None).isoformat(sep=" ")


@pytest.fixture
def runs_db(tmp_path: Path) -> Path:
    """A `runs` table holding one run per age, in days, from 0 to 60."""
    path = tmp_path / "runs.db"
    now = datetime.now(UTC)
    with sqlite3.connect(path) as con:
        con.execute("create table runs (run_id text, create_timestamp text)")
        con.executemany(
            "insert into runs values (?, ?)",
            [
                (f"age-{age}", sqlalchemy_timestamp(now - timedelta(days=age)))
                for age in (0, 1, 29, 31, 60)
            ],
        )
    return path


def test_selects_only_runs_past_the_window(runs_db: Path) -> None:
    """Anything inside the window survives; the boundary is not off by a day."""
    assert purge_runs.stale_run_ids(runs_db, days=30) == ["age-60", "age-31"]


def test_returns_oldest_first(runs_db: Path) -> None:
    """A purge interrupted by a full disk should have freed the most space it could."""
    assert purge_runs.stale_run_ids(runs_db, days=1) == ["age-60", "age-31", "age-29"]


def test_empty_when_nothing_is_old_enough(runs_db: Path) -> None:
    """A window wider than the history deletes nothing rather than everything."""
    assert purge_runs.stale_run_ids(runs_db, days=365) == []


def test_purge_run_files_removes_the_shard_and_its_compute_logs(tmp_path: Path) -> None:
    """The reason this script exists: `delete_run` empties the shard, never unlinks it.

    Verified against a real instance --- `DagsterInstance.delete_run` leaves
    `history/runs/<id>.db` on disk at full size. 65k of those filled the LXC.
    """
    kept, purged = "keep-me", "purge-me"
    shards = tmp_path / "history" / "runs"
    shards.mkdir(parents=True)
    for run_id in (kept, purged):
        (shards / f"{run_id}.db").write_text("events")
        (shards / f"{run_id}.db-wal").write_text("wal")
        logs = tmp_path / "storage" / run_id / "compute_logs"
        logs.mkdir(parents=True)
        (logs / "step.out").write_text("stdout")

    purge_runs.purge_run_files(tmp_path, purged)

    assert not (shards / f"{purged}.db").exists()
    assert not (shards / f"{purged}.db-wal").exists()
    assert not (tmp_path / "storage" / purged).exists()
    assert (shards / f"{kept}.db").exists()
    assert (tmp_path / "storage" / kept).exists()


def test_purge_run_files_is_idempotent(tmp_path: Path) -> None:
    """A run with no shard or logs yet is not an error --- the timer must not fail."""
    (tmp_path / "history" / "runs").mkdir(parents=True)
    purge_runs.purge_run_files(tmp_path, "never-existed")
