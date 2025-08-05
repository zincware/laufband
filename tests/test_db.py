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


def test_get_job_stats(tracker: LaufbandDB):
    """Test get_job_stats returns correct counts for each state."""
    tracker.create(10)

    # Initially all jobs should be pending
    stats = tracker.get_job_stats()
    assert stats["pending"] == 10
    assert stats["running"] == 0
    assert stats["completed"] == 0
    assert stats["failed"] == 0
    assert stats["died"] == 0

    # Process some jobs
    jobs = list(tracker)  # This marks all as running
    assert len(jobs) == 10

    # Complete some jobs
    tracker.finalize(0, "completed")
    tracker.finalize(1, "completed")
    tracker.finalize(2, "failed")

    stats = tracker.get_job_stats()
    assert stats["pending"] == 0
    assert stats["running"] == 7  # 10 - 3 processed
    assert stats["completed"] == 2
    assert stats["failed"] == 1
    assert stats["died"] == 0


def test_get_worker_info_single_worker(tracker: LaufbandDB):
    """Test get_worker_info with a single worker."""
    tracker.create(5)
    tracker.worker = "test_worker"

    # Initially no workers should be in the table
    worker_info = tracker.get_worker_info()
    assert len(worker_info) == 0

    # Process some jobs (this creates worker entry)
    jobs = list(tracker)
    assert len(jobs) == 5

    worker_info = tracker.get_worker_info()
    assert len(worker_info) == 1

    worker = worker_info[0]
    assert worker["worker"] == "test_worker"
    assert worker["active_jobs"] == 5
    assert worker["completed_jobs"] == 0
    assert worker["failed_jobs"] == 0
    assert worker["processed_jobs"] == 0
    assert (
        worker["max_retries"] == 1
    )  # count starts at 0, incremented to 1 when running
    assert worker["last_seen"] is not None

    # Complete some jobs
    tracker.finalize(0, "completed")
    tracker.finalize(1, "completed")
    tracker.finalize(2, "failed")

    worker_info = tracker.get_worker_info()
    worker = worker_info[0]
    assert worker["active_jobs"] == 2  # 5 - 3 processed
    assert worker["completed_jobs"] == 2
    assert worker["failed_jobs"] == 1
    assert worker["processed_jobs"] == 3  # completed + failed


def test_get_worker_info_multiple_workers(tmp_path: Path):
    """Test get_worker_info with multiple workers."""
    db_path = tmp_path / "test_multi_worker.db"

    # Worker 1 processes first batch
    worker1 = LaufbandDB(db_path, worker="worker_1")
    worker1.create(10)

    # Process only 3 jobs with worker1
    jobs1 = []
    worker1_iter = iter(worker1)
    for _ in range(3):
        job = next(worker1_iter)
        jobs1.append(job)
        worker1.finalize(job, "completed")

    # Worker 2 processes next batch
    worker2 = LaufbandDB(db_path, worker="worker_2")

    # Process 2 jobs with worker2
    jobs2 = []
    worker2_iter = iter(worker2)
    for _ in range(2):
        job = next(worker2_iter)
        jobs2.append(job)

    worker2.finalize(jobs2[0], "completed")
    worker2.finalize(jobs2[1], "failed")

    # Check worker info
    worker_info = worker1.get_worker_info()
    assert len(worker_info) == 2

    # Workers should be ordered by last_seen DESC
    workers_by_name = {w["worker"]: w for w in worker_info}

    assert "worker_1" in workers_by_name
    assert "worker_2" in workers_by_name

    w1 = workers_by_name["worker_1"]
    assert w1["completed_jobs"] == 3
    assert w1["failed_jobs"] == 0
    assert w1["processed_jobs"] == 3
    assert w1["active_jobs"] == 0

    w2 = workers_by_name["worker_2"]
    assert w2["completed_jobs"] == 1
    assert w2["failed_jobs"] == 1
    assert w2["processed_jobs"] == 2
    assert w2["active_jobs"] == 0


def test_get_worker_info_with_retries(tmp_path: Path):
    """Test get_worker_info tracks max retries correctly."""
    db_path = tmp_path / "test_retries.db"
    worker = LaufbandDB(db_path, worker="retry_worker", max_died_retries=2)
    worker.create(3)

    _ = list(worker)

    # Simulate some retries by marking jobs as died and reprocessing
    worker.finalize(0, "died")
    worker.finalize(1, "died")

    # Process died jobs again (this increments count)
    _ = list(worker)  # Should get the died jobs back

    worker_info = worker.get_worker_info()
    worker_data = worker_info[0]

    # max_retries should reflect the highest count value
    assert worker_data["max_retries"] >= 1  # Jobs have been retried at least once
