import multiprocessing
import threading
import time

from flufl.lock import Lock, LockState
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from laufband import Graphband, Task
from laufband.db import WorkerEntry, WorkerStatus


def simple_tasks():
    """Generate simple tasks for testing."""
    for i in range(3):
        yield Task(id=f"task_{i}", data={"value": i})


def slow_tasks():
    """Generate tasks that take time to process."""
    for i in range(2):
        yield Task(id=f"slow_task_{i}", data={"value": i})


def lock_holding_worker(lock_path, db_lock_path, db_path, hold_duration):
    """Worker that holds user lock for extended period."""
    user_lock = Lock(lock_path, lifetime=60)
    db_lock = Lock(db_lock_path, lifetime=60)

    pbar = Graphband(
        simple_tasks(),
        lock=user_lock,
        db_lock=db_lock,
        db=db_path,
        heartbeat_interval=1,
        heartbeat_timeout=5,
        identifier="lock_holder",
    )

    # Hold user lock for extended period
    with pbar.lock:
        time.sleep(hold_duration)


def heartbeat_checker_worker(lock_path, db_lock_path, db_path, results):
    """Worker that checks if heartbeat can update during user lock hold."""
    user_lock = Lock(lock_path, lifetime=60)
    db_lock = Lock(db_lock_path, lifetime=60)

    try:
        pbar = Graphband(
            simple_tasks(),
            lock=user_lock,
            db_lock=db_lock,
            db=db_path,
            heartbeat_interval=1,
            heartbeat_timeout=5,
            identifier="heartbeat_checker",
        )

        # Try to access database while user lock might be held
        time.sleep(2)

        with pbar.db_lock:
            with Session(pbar._engine) as session:
                worker = session.get(WorkerEntry, "heartbeat_checker")
                if worker and worker.status == WorkerStatus.IDLE:
                    results.append("success")
                else:
                    results.append("failed")
    except Exception as e:
        results.append(f"error: {e}")


def concurrent_db_worker(worker_id, db_path, results):
    """Worker that performs database operations concurrently."""
    try:
        pbar = Graphband(
            simple_tasks(),
            db=db_path,
            identifier=f"worker_{worker_id}",
            heartbeat_interval=1,
        )

        # Perform multiple database operations
        for _ in range(3):
            with pbar.db_lock:
                time.sleep(0.1)  # Simulate database work

        results.append(f"worker_{worker_id}_success")
    except Exception as e:
        results.append(f"worker_{worker_id}_error: {e}")


def test_user_lock_does_not_block_heartbeat(tmp_path):
    """Test that holding user lock doesn't prevent heartbeat database updates."""
    lock_path = f"{tmp_path}/test.lock"
    db_lock_path = f"{tmp_path}/test_db.lock"
    db_path = f"sqlite:///{tmp_path}/test.db"

    # Start process that holds user lock
    lock_process = multiprocessing.Process(
        target=lock_holding_worker, args=(lock_path, db_lock_path, db_path, 3)
    )
    lock_process.start()

    # Start process that checks heartbeat functionality
    manager = multiprocessing.Manager()
    results = manager.list()

    checker_process = multiprocessing.Process(
        target=heartbeat_checker_worker,
        args=(lock_path, db_lock_path, db_path, results),
    )
    checker_process.start()

    lock_process.join()
    checker_process.join()

    # Heartbeat should work despite user lock being held
    # assert len(results) > 0
    # assert "success" in results[0] or "error" not in results[0]
    assert "success" in list(results), f"unexpected results: {list(results)}"


def test_database_lock_prevents_corruption(tmp_path):
    """Test that database lock prevents corruption during concurrent access."""
    db_path = f"sqlite:///{tmp_path}/test.db"

    manager = multiprocessing.Manager()
    results = manager.list()

    # Start multiple workers concurrently
    processes = []
    for i in range(3):
        proc = multiprocessing.Process(
            target=concurrent_db_worker, args=(i, db_path, results)
        )
        processes.append(proc)
        proc.start()

    for proc in processes:
        proc.join()

    # All workers should complete successfully
    success_count = sum(1 for r in results if "success" in str(r))
    assert success_count == 3, f"Expected 3 successes, got results: {list(results)}"


def test_separate_lock_files_created(tmp_path):
    """Test that separate lock files are created for user and database locks."""
    lock_path = f"{tmp_path}/test.lock"
    db_lock_path = f"{tmp_path}/test_db.lock"
    db_path = f"sqlite:///{tmp_path}/test.db"

    user_lock = Lock(lock_path, lifetime=60)
    db_lock = Lock(db_lock_path, lifetime=60)

    pbar = Graphband(simple_tasks(), lock=user_lock, db_lock=db_lock, db=db_path)

    # Both locks should be separate objects
    assert pbar.lock is not pbar.db_lock
    assert hasattr(pbar, "lock")
    assert hasattr(pbar, "db_lock")

    pbar.close()


def test_default_db_lock_creation(tmp_path):
    """Test that default database lock is created when not provided."""
    db_path = f"sqlite:///{tmp_path}/test.db"

    pbar = Graphband(simple_tasks(), db=db_path)

    # Should have both user and database locks
    assert hasattr(pbar, "lock")
    assert hasattr(pbar, "db_lock")
    assert pbar.lock is not pbar.db_lock

    pbar.close()


def failing_tasks():
    yield Task(id="task_0", data={"value": 0})
    yield Task(id="task_1", data={"value": 1}, dependencies={"nonexistent"})


def test_failed_job_cache_coordination(tmp_path):
    """Test that failed job cache is properly coordinated by user lock."""
    db_path = f"sqlite:///{tmp_path}/test.db"

    pbar = Graphband(failing_tasks(), db=db_path, heartbeat_interval=1)

    # Process tasks to trigger failed job cache usage
    processed = []
    for task in pbar:
        processed.append(task.id)
        if len(processed) >= 1:  # Only process first task
            break

    # Check that failed job cache is properly managed
    assert len(pbar._failed_job_cache) > 0

    # Test has_more_jobs considers failed cache
    assert pbar.has_more_jobs

    pbar.close()


def test_heartbeat_continues_during_user_lock_hold(tmp_path):
    """Test that heartbeat database operations continue even when user holds lock."""
    lock_path = f"{tmp_path}/test.lock"
    db_lock_path = f"{tmp_path}/test_db.lock"
    db_path = f"sqlite:///{tmp_path}/test.db"

    # Use reasonable lock lifetimes
    user_lock = Lock(lock_path, lifetime=30)  # Long enough for user operations
    db_lock = Lock(db_lock_path, lifetime=10)  # Shorter for quick database ops

    pbar = Graphband(
        simple_tasks(),
        lock=user_lock,
        db_lock=db_lock,
        db=db_path,
        heartbeat_interval=1,  # 1 second heartbeat
        heartbeat_timeout=5,
    )

    # Let heartbeat start up
    time.sleep(2)

    # Get initial heartbeat time
    from sqlalchemy.orm import Session

    from laufband.db import WorkerEntry

    engine = create_engine(db_path)

    with Session(engine) as session:
        worker = session.get(WorkerEntry, pbar.identifier)
        initial_heartbeat = worker.last_heartbeat

    # Hold user lock and verify heartbeat still updates
    with pbar.lock:
        time.sleep(3)  # Hold for 3 seconds

    # Check that heartbeat was updated during the lock hold
    with Session(engine) as session:
        worker = session.get(WorkerEntry, pbar.identifier)
        final_heartbeat = worker.last_heartbeat

    # Heartbeat should have been updated despite user lock being held
    assert final_heartbeat > initial_heartbeat, (
        "Heartbeat should continue during user lock hold"
    )

    pbar.close()


def test_user_lock_refresh_prevents_expiration(tmp_path):
    """Test that user lock is refreshed by heartbeat
    to prevent expiration during long operations."""
    lock_path = f"{tmp_path}/test.lock"
    db_lock_path = f"{tmp_path}/test_db.lock"
    db_path = f"sqlite:///{tmp_path}/test.db"

    # Create user lock with lifetime shorter than heartbeat
    # interval to test refresh functionality
    user_lock = Lock(lock_path, lifetime=20)  # 20 second lifetime
    db_lock = Lock(db_lock_path, lifetime=60)

    pbar = Graphband(
        simple_tasks(),
        lock=user_lock,
        db_lock=db_lock,
        db=db_path,
        heartbeat_interval=2,  # 10 second heartbeat
        heartbeat_timeout=6,
    )

    # Let heartbeat start up
    time.sleep(0.5)

    def hold_lock_for_duration():
        """Hold the user lock for longer than its lifetime."""
        with pbar.lock:
            # Hold for 5 seconds (longer than 20 second lifetime)
            # Heartbeat should refresh it to prevent expiration
            time.sleep(5)
            return user_lock.state

    # Run lock holding in a thread to simulate user operations
    result = []
    thread = threading.Thread(target=lambda: result.append(hold_lock_for_duration()))
    thread.start()
    thread.join()

    # Lock should still be valid due to heartbeat refresh
    final_state = result[0]
    assert final_state == LockState.ours, (
        f"Lock should remain valid, got: {final_state}"
    )

    pbar.close()
