"""Shared test fixtures and utilities."""

import time
from datetime import datetime

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from laufband.db import (
    Base,
    TaskEntry,
    TaskStatusEntry,
    TaskStatusEnum,
    WorkerEntry,
    WorkerStatus,
    WorkflowEntry,
)
from laufband.time_provider import MockTimeProvider


@pytest.fixture
def mock_time():
    """Provide a controllable mock time provider."""
    return MockTimeProvider(start_time=datetime(2024, 1, 1, 12, 0, 0))


@pytest.fixture
def db_engine(tmp_path):
    """Create an in-memory SQLite database for fast testing."""
    # Use tmp_path for file-based DB to avoid conflicts
    db_path = tmp_path / "test.db"
    engine = create_engine(f"sqlite:///{db_path}", echo=False)
    Base.metadata.create_all(engine)
    return engine


@pytest.fixture
def db_session(db_engine):
    """Provide a database session with automatic rollback."""
    with Session(db_engine) as session:
        yield session
        # Rollback any uncommitted changes
        session.rollback()


@pytest.fixture
def workflow_factory(db_session):
    """Factory for creating workflow entries."""

    def _create_workflow(workflow_id="main", total_tasks=None):
        workflow = WorkflowEntry(id=workflow_id, total_tasks=total_tasks)
        db_session.add(workflow)
        db_session.commit()
        return workflow

    return _create_workflow


@pytest.fixture
def worker_factory(db_session, workflow_factory, mock_time):
    """Factory for creating worker entries with various states."""
    counter = {"count": 0}

    def _create_worker(
        identifier=None,
        status=WorkerStatus.IDLE,
        workflow=None,
        heartbeat_interval=30,
        heartbeat_timeout=60,
        labels=None,
        hostname="test-host",
        pid=12345,
    ):
        if workflow is None:
            workflow = workflow_factory()

        if identifier is None:
            identifier = f"test-worker-{counter['count']}"
            counter["count"] += 1

        worker = WorkerEntry(
            id=identifier,
            status=status,
            workflow_id=workflow.id,
            heartbeat_interval=heartbeat_interval,
            heartbeat_timeout=heartbeat_timeout,
            labels=labels or [],
            hostname=hostname,
            pid=pid,
            last_heartbeat=mock_time.now(),
            started_at=mock_time.now(),
        )
        db_session.add(worker)
        db_session.commit()
        db_session.refresh(worker)
        return worker

    return _create_worker


@pytest.fixture
def task_factory(db_session, worker_factory, workflow_factory, mock_time):
    """Factory for creating task entries with various states."""
    counter = {"count": 0}

    def _create_task(
        task_id=None,
        status=TaskStatusEnum.RUNNING,
        worker=None,
        workflow=None,
        dependencies=None,
        requirements=None,
        max_parallel_workers=1,
    ):
        if workflow is None:
            workflow = workflow_factory()

        if task_id is None:
            task_id = f"task_{counter['count']}"
            counter["count"] += 1

        if worker is None and status in [
            TaskStatusEnum.RUNNING,
            TaskStatusEnum.COMPLETED,
        ]:
            worker = worker_factory(workflow=workflow)

        task = TaskEntry(
            id=task_id,
            workflow_id=workflow.id,
            requirements=requirements or [],
            max_parallel_workers=max_parallel_workers,
        )
        db_session.add(task)
        db_session.flush()  # Get task ID assigned

        status_entry = TaskStatusEntry(
            task=task,
            status=status,
            worker=worker,
            dependencies=dependencies or [],
            timestamp=mock_time.now(),
        )
        db_session.add(status_entry)
        db_session.commit()
        db_session.refresh(task)
        return task

    return _create_task


@pytest.fixture
def wait_for_condition():
    """Utility for polling until a condition is met."""

    def _wait(check_fn, timeout=5, poll_interval=0.05, error_message="Condition not met"):
        """Poll for condition instead of fixed sleep.

        Args:
            check_fn: Function that returns True when condition is met
            timeout: Maximum seconds to wait
            poll_interval: Seconds between checks
            error_message: Message to show on timeout

        Raises:
            TimeoutError: If condition not met within timeout
        """
        start = time.time()
        while time.time() - start < timeout:
            if check_fn():
                return True
            time.sleep(poll_interval)
        raise TimeoutError(f"{error_message} (waited {timeout}s)")

    return _wait


@pytest.fixture
def db_wait_helpers(db_engine, wait_for_condition):
    """Helpers for waiting on database state changes."""

    class DbWaitHelpers:
        def __init__(self, engine, wait_fn):
            self.engine = engine
            self.wait = wait_fn

        def wait_for_task_count(self, expected, timeout=5):
            """Wait until task count reaches expected value."""

            def check():
                with Session(self.engine) as session:
                    return session.query(TaskEntry).count() == expected

            self.wait(check, timeout, error_message=f"Task count != {expected}")

        def wait_for_worker_status(self, worker_id, expected_status, timeout=5):
            """Wait until worker reaches expected status."""

            def check():
                with Session(self.engine) as session:
                    worker = session.get(WorkerEntry, worker_id)
                    return worker and worker.status == expected_status

            self.wait(
                check,
                timeout,
                error_message=f"Worker {worker_id} status != {expected_status}",
            )

        def wait_for_task_status(self, task_id, expected_status, timeout=5):
            """Wait until task reaches expected status."""

            def check():
                with Session(self.engine) as session:
                    task = session.get(TaskEntry, task_id)
                    return task and task.current_status.status == expected_status

            self.wait(
                check,
                timeout,
                error_message=f"Task {task_id} status != {expected_status}",
            )

        def wait_for_worker_killed(self, worker_id, timeout=5):
            """Wait until worker is marked as killed."""
            self.wait_for_worker_status(worker_id, WorkerStatus.KILLED, timeout)

    return DbWaitHelpers(db_engine, wait_for_condition)
