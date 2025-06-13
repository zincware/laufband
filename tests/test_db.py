import sqlite3
import time
from pathlib import Path

import pytest

from laufband.db import LaufbandDB  # Assuming your class is in laufband/db.py


# Use a function to create a fresh ProgressTracker instance for each test
@pytest.fixture
def tracker(tmp_path: Path) -> LaufbandDB:
    return LaufbandDB(tmp_path / "test_progress.db")


def test_create_table(tracker: LaufbandDB):
    tracker.create(10)
    assert tracker.db_path.exists()
    assert tracker.list_state("pending") == list(range(10))

    with pytest.raises(sqlite3.OperationalError):
        # Attempt to create the table again, which should fail
        tracker.create(10)


def test_len(tracker: LaufbandDB):
    tracker.create(10)
    assert len(tracker) == 10


def test_next(tracker: LaufbandDB):
    tracker.create(10)

    data = list(tracker)
    assert tracker.list_state("running") == data
    assert data == list(range(10))


def test_set_completed(tracker: LaufbandDB):
    tracker.create(10)

    for job in tracker:
        tracker.finalize(job, "completed")

    assert tracker.list_state("completed") == list(range(10))
    assert tracker.list_state("running") == []
    assert tracker.list_state("failed") == []
    assert tracker.list_state("pending") == []


def test_set_failed(tracker: LaufbandDB):
    tracker.create(10)

    for job in tracker:
        tracker.finalize(job, "failed")

    assert tracker.list_state("failed") == list(range(10))
    assert tracker.list_state("running") == []
    assert tracker.list_state("completed") == []
    assert tracker.list_state("pending") == []


def test_get_worker(tracker: LaufbandDB):
    tracker.create(10)
    tracker.worker = "worker_1"

    for job in tracker:
        tracker.finalize(job, "completed")
        if job == 5:
            break

    tracker.worker = "worker_2"

    for job in tracker:
        tracker.finalize(job, "completed")

    for idx in range(10):
        assert tracker.get_worker(idx) == "worker_2" if idx > 5 else "worker_1"


def test_dublicate_worker_identifier(tmp_path: Path):
    a = LaufbandDB(tmp_path / "test.db", worker="worker")
    b = LaufbandDB(tmp_path / "test.db", worker="worker")

    a.create(5)

    assert list(a) == list(range(5))
    assert list(a) == []  # test creating the "iter" object again
    with pytest.raises(
        ValueError, match="Worker with identifier 'worker' already exists."
    ):
        list(b)


@pytest.mark.parametrize("max_died_retries", [0, 1, 2])
def test_retry_killed(tmp_path: Path, max_died_retries: int):
    """Test if laufband can handle killed jobs."""
    com = tmp_path / "laufband.sqlite"
    db = LaufbandDB(com, max_died_retries=max_died_retries)
    db.create(5)

    assert list(db) == list(range(5))

    for _ in range(max_died_retries):
        db.finalize(0, "died")
        assert list(db) == [0]

    assert list(db) == []


def test_heartbeat_timeout(tmp_path: Path):
    """Test if laufband can handle killed jobs."""
    com = tmp_path / "laufband.sqlite"
    db_1 = LaufbandDB(com, heartbeat_timeout=0.1, worker="1")
    db_1.create(2)

    assert list(db_1) == [0, 1]
    assert db_1.list_state("running") == [0, 1]

    time.sleep(2)

    db_2 = LaufbandDB(com, worker="2", heartbeat_timeout=1)
    assert list(db_2) == []  # update the worker state
    assert db_2.list_state("running") == []
    assert db_2.list_state("died") == [0, 1]

    db_3 = LaufbandDB(com, worker="3", heartbeat_timeout=1, max_died_retries=1)

    assert list(db_3) == [0, 1]  # update the worker state from 'died' to 'running'
    assert db_3.list_state("running") == [0, 1]
    assert db_3.list_state("died") == []


def test_db_identifier_none(tmp_path: Path):
    with pytest.raises(ValueError):
        LaufbandDB(tmp_path / "test.db", worker=None)
