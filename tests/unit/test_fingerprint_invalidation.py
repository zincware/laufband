"""Unit tests for Phase 2: Fingerprint-based invalidation."""

import pytest

from laufband.db import TaskEntry, TaskStatusEntry, TaskStatusEnum
from laufband.task import Task


@pytest.mark.unit
def test_task_has_fingerprint_field():
    """Task dataclass should have fingerprint field."""
    task = Task(id="test", fingerprint="abc123")
    assert task.fingerprint == "abc123"


@pytest.mark.unit
def test_task_fingerprint_optional():
    """Task fingerprint should be optional."""
    task = Task(id="test")
    assert task.fingerprint is None


@pytest.mark.unit
def test_task_entry_has_last_fingerprint(db_session, task_factory):
    """TaskEntry should have last_fingerprint field."""
    task = task_factory(task_id="test")

    # Initially None
    assert task.last_fingerprint is None

    # Can be set
    task.last_fingerprint = "abc123"
    db_session.commit()
    db_session.refresh(task)

    assert task.last_fingerprint == "abc123"


@pytest.mark.unit
def test_task_status_entry_has_fingerprint(db_session, task_factory):
    """TaskStatusEntry should have fingerprint field for audit trail."""
    task = task_factory(task_id="test", status=TaskStatusEnum.RUNNING)

    # Add status with fingerprint
    db_session.add(
        TaskStatusEntry(
            task=task,
            status=TaskStatusEnum.COMPLETED,
            worker=task.current_status.worker,
            fingerprint="xyz789",
        )
    )
    db_session.commit()
    db_session.refresh(task)

    # Check fingerprint was stored
    completed_status = next(
        s for s in task.statuses if s.status == TaskStatusEnum.COMPLETED
    )
    assert completed_status.fingerprint == "xyz789"


@pytest.mark.unit
def test_invalidated_status_exists():
    """TaskStatusEnum should have INVALIDATED status."""
    assert hasattr(TaskStatusEnum, "INVALIDATED")
    assert TaskStatusEnum.INVALIDATED == "invalidated"


@pytest.mark.unit
def test_invalidated_task_not_completed(db_session, task_factory):
    """Tasks with INVALIDATED status should not be considered completed."""
    task = task_factory(task_id="test", status=TaskStatusEnum.COMPLETED)

    # Task is completed
    assert task.completed is True

    # Invalidate it
    db_session.add(
        TaskStatusEntry(
            task=task,
            status=TaskStatusEnum.INVALIDATED,
            worker=task.current_status.worker,
        )
    )
    db_session.commit()
    db_session.refresh(task)

    # Should no longer be considered completed
    assert task.completed is False
    assert task.current_status.status == TaskStatusEnum.INVALIDATED


@pytest.mark.unit
def test_completed_task_with_active_workers_not_completed(
    db_session, task_factory, worker_factory
):
    """Completed task with active workers should not be considered completed."""
    workflow = task_factory(status=TaskStatusEnum.RUNNING).workflow
    worker = worker_factory(workflow=workflow)

    task = task_factory(
        task_id="multi-worker",
        status=TaskStatusEnum.COMPLETED,
        workflow=workflow,
        max_parallel_workers=2,
    )

    # Add another running worker
    db_session.add(
        TaskStatusEntry(task=task, status=TaskStatusEnum.RUNNING, worker=worker)
    )
    db_session.commit()
    db_session.refresh(task)

    # Should not be completed (has active workers)
    assert task.completed is False


@pytest.mark.unit
def test_fingerprint_stored_on_completion(db_session, task_factory):
    """Fingerprint should be stored in TaskStatusEntry on completion."""
    task = task_factory(task_id="test", status=TaskStatusEnum.RUNNING)

    # Complete with fingerprint
    db_session.add(
        TaskStatusEntry(
            task=task,
            status=TaskStatusEnum.COMPLETED,
            worker=task.current_status.worker,
            fingerprint="fingerprint123",
        )
    )
    db_session.commit()
    db_session.refresh(task)

    # Verify fingerprint in status entry
    completed_status = task.current_status
    assert completed_status.status == TaskStatusEnum.COMPLETED
    assert completed_status.fingerprint == "fingerprint123"


@pytest.mark.unit
def test_last_fingerprint_updated_on_completion(db_session, task_factory):
    """TaskEntry.last_fingerprint should be updated when task completes."""
    task = task_factory(task_id="test", status=TaskStatusEnum.RUNNING)

    # Initially None
    assert task.last_fingerprint is None

    # Update last_fingerprint (simulating what graphband does)
    task.last_fingerprint = "new_fingerprint"
    db_session.commit()
    db_session.refresh(task)

    assert task.last_fingerprint == "new_fingerprint"


@pytest.mark.unit
def test_multiple_fingerprints_audit_trail(db_session, task_factory):
    """Should maintain audit trail of all fingerprints."""
    task = task_factory(task_id="test", status=TaskStatusEnum.RUNNING)

    # First completion
    db_session.add(
        TaskStatusEntry(
            task=task,
            status=TaskStatusEnum.COMPLETED,
            worker=task.current_status.worker,
            fingerprint="v1",
        )
    )
    db_session.commit()
    db_session.refresh(task)

    # Invalidate
    db_session.add(
        TaskStatusEntry(
            task=task,
            status=TaskStatusEnum.INVALIDATED,
            worker=task.current_status.worker,
            fingerprint="v2",
        )
    )
    db_session.commit()
    db_session.refresh(task)

    # Second completion with new fingerprint
    db_session.add(
        TaskStatusEntry(
            task=task,
            status=TaskStatusEnum.COMPLETED,
            worker=task.current_status.worker,
            fingerprint="v2",
        )
    )
    db_session.commit()
    db_session.refresh(task)

    # Check audit trail
    fingerprints = [s.fingerprint for s in task.statuses if s.fingerprint]
    assert "v1" in fingerprints
    assert "v2" in fingerprints
    assert len(fingerprints) == 3  # v1 (completed), v2 (invalidated), v2 (completed)


@pytest.mark.unit
def test_task_without_fingerprint_still_works(db_session, task_factory):
    """Tasks without fingerprints should work as before (backward compatibility)."""
    task = task_factory(task_id="test", status=TaskStatusEnum.RUNNING)

    # Complete without fingerprint
    db_session.add(
        TaskStatusEntry(
            task=task,
            status=TaskStatusEnum.COMPLETED,
            worker=task.current_status.worker,
            # No fingerprint provided
        )
    )
    db_session.commit()
    db_session.refresh(task)

    # Should be completed normally
    assert task.completed is True
    assert task.current_status.status == TaskStatusEnum.COMPLETED
    assert task.current_status.fingerprint is None


@pytest.mark.unit
def test_fingerprint_field_nullable(db_session, task_factory):
    """Fingerprint fields should be nullable for backward compatibility."""
    task = task_factory(task_id="test")

    # TaskEntry.last_fingerprint should be nullable
    assert task.last_fingerprint is None

    # TaskStatusEntry.fingerprint should be nullable
    for status in task.statuses:
        assert status.fingerprint is None or isinstance(status.fingerprint, str)
