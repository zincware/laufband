import sqlite3
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
