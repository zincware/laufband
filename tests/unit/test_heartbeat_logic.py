"""Unit tests for heartbeat expiration logic."""

import pytest

from laufband.db import WorkerStatus
from laufband.worker_logic import check_and_mark_expired_workers


@pytest.mark.unit
def test_heartbeat_not_expired_initially(worker_factory, mock_time):
    """Worker heartbeat should not be expired immediately after creation."""
    worker = worker_factory(heartbeat_timeout=5)

    assert not worker.is_heartbeat_expired(mock_time)
    assert worker.status == WorkerStatus.IDLE


@pytest.mark.unit
def test_heartbeat_expires_after_timeout(worker_factory, mock_time):
    """Worker heartbeat should expire after timeout period."""
    worker = worker_factory(heartbeat_timeout=5)

    # Advance time beyond timeout
    mock_time.advance(6)

    assert worker.is_heartbeat_expired(mock_time)


@pytest.mark.unit
def test_heartbeat_not_expired_before_timeout(worker_factory, mock_time):
    """Worker heartbeat should not expire before timeout."""
    worker = worker_factory(heartbeat_timeout=10)

    # Advance time but stay under timeout
    mock_time.advance(5)

    assert not worker.is_heartbeat_expired(mock_time)


@pytest.mark.unit
def test_check_and_mark_expired_workers_marks_killed(
    db_session, worker_factory, task_factory, workflow_factory, mock_time
):
    """Expired workers should be marked as KILLED with their tasks."""
    workflow = workflow_factory()

    # Create worker with running task
    worker = worker_factory(
        workflow=workflow, heartbeat_timeout=5, status=WorkerStatus.BUSY
    )
    task = task_factory(worker=worker, workflow=workflow)

    # Initially not expired
    assert not worker.is_heartbeat_expired(mock_time)

    # Advance time to expire heartbeat
    mock_time.advance(6)

    # Check and mark expired workers
    killed = check_and_mark_expired_workers(db_session, workflow.id, mock_time)
    db_session.commit()  # Commit the changes

    # Verify worker was marked as killed
    assert len(killed) == 1
    assert killed[0].id == worker.id

    db_session.refresh(worker)
    db_session.refresh(task)

    assert worker.status == WorkerStatus.KILLED
    # Task should have KILLED status added
    assert task.current_status.status.name == "KILLED"


@pytest.mark.unit
def test_check_expired_workers_ignores_offline_workers(
    db_session, worker_factory, workflow_factory, mock_time
):
    """Offline workers should not be checked for heartbeat expiration."""
    workflow = workflow_factory()

    worker = worker_factory(
        workflow=workflow, heartbeat_timeout=5, status=WorkerStatus.OFFLINE
    )

    # Advance time
    mock_time.advance(10)

    # Should not mark offline workers as killed
    killed = check_and_mark_expired_workers(db_session, workflow.id, mock_time)

    assert len(killed) == 0
    db_session.refresh(worker)
    assert worker.status == WorkerStatus.OFFLINE


@pytest.mark.unit
def test_check_expired_workers_multiple_workers(
    db_session, worker_factory, workflow_factory, mock_time
):
    """Multiple expired workers should all be marked as killed."""
    workflow = workflow_factory()

    workers = [
        worker_factory(
            workflow=workflow,
            identifier=f"worker-{i}",
            heartbeat_timeout=5,
            status=WorkerStatus.IDLE,
        )
        for i in range(3)
    ]

    # Expire all heartbeats
    mock_time.advance(6)

    killed = check_and_mark_expired_workers(db_session, workflow.id, mock_time)
    db_session.commit()

    assert len(killed) == 3
    killed_ids = {w.id for w in killed}
    assert killed_ids == {w.id for w in workers}


@pytest.mark.unit
def test_check_expired_workers_mixed_states(
    db_session, worker_factory, workflow_factory, mock_time
):
    """Only IDLE and BUSY workers should be checked for expiration."""
    workflow = workflow_factory()

    expired_worker = worker_factory(
        workflow=workflow,
        identifier="expired",
        heartbeat_timeout=5,
        status=WorkerStatus.IDLE,
    )
    valid_worker = worker_factory(
        workflow=workflow,
        identifier="valid",
        heartbeat_timeout=100,
        status=WorkerStatus.BUSY,
    )
    offline_worker = worker_factory(
        workflow=workflow,
        identifier="offline",
        heartbeat_timeout=5,
        status=WorkerStatus.OFFLINE,
    )

    # Expire some heartbeats
    mock_time.advance(6)

    killed = check_and_mark_expired_workers(db_session, workflow.id, mock_time)
    db_session.commit()

    # Only the expired IDLE worker should be killed
    assert len(killed) == 1
    assert killed[0].id == expired_worker.id

    db_session.refresh(expired_worker)
    db_session.refresh(valid_worker)
    db_session.refresh(offline_worker)

    assert expired_worker.status == WorkerStatus.KILLED
    assert valid_worker.status == WorkerStatus.BUSY
    assert offline_worker.status == WorkerStatus.OFFLINE
