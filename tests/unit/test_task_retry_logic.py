"""Unit tests for task retry policy logic."""

import pytest

from laufband.db import TaskStatusEntry, TaskStatusEnum
from laufband.worker_logic import should_retry_task


@pytest.mark.unit
def test_should_not_retry_completed_task(task_factory):
    """Completed tasks should not be retried."""
    task = task_factory(status=TaskStatusEnum.COMPLETED)

    should_retry, reason = should_retry_task(task, max_failed_retries=2, max_killed_retries=2)

    assert not should_retry
    assert reason == "already_completed"


@pytest.mark.unit
def test_should_retry_failed_task_within_limit(db_session, task_factory):
    """Failed tasks within retry limit should be retried."""
    task = task_factory(status=TaskStatusEnum.FAILED)

    should_retry, reason = should_retry_task(task, max_failed_retries=2, max_killed_retries=2)

    assert should_retry
    assert reason is None


@pytest.mark.unit
def test_should_not_retry_failed_task_exceeding_limit(db_session, task_factory):
    """Failed tasks exceeding retry limit should not be retried."""
    task = task_factory(status=TaskStatusEnum.FAILED)

    # Add more failed attempts
    for _ in range(2):
        status = TaskStatusEntry(
            task=task,
            status=TaskStatusEnum.FAILED,
            worker=task.current_status.worker,
        )
        db_session.add(status)
    db_session.commit()
    db_session.refresh(task)

    # Now task has failed 3 times total
    assert task.failed_retries == 3

    should_retry, reason = should_retry_task(task, max_failed_retries=2, max_killed_retries=2)

    assert not should_retry
    assert reason == "max_failed_retries_exceeded"


@pytest.mark.unit
def test_should_retry_killed_task_within_limit(task_factory):
    """Killed tasks within retry limit should be retried."""
    task = task_factory(status=TaskStatusEnum.KILLED)

    should_retry, reason = should_retry_task(task, max_failed_retries=2, max_killed_retries=2)

    assert should_retry
    assert reason is None


@pytest.mark.unit
def test_should_not_retry_killed_task_exceeding_limit(db_session, task_factory):
    """Killed tasks exceeding retry limit should not be retried."""
    task = task_factory(status=TaskStatusEnum.KILLED)

    # Add more killed attempts
    for _ in range(2):
        status = TaskStatusEntry(
            task=task,
            status=TaskStatusEnum.KILLED,
            worker=task.current_status.worker,
        )
        db_session.add(status)
    db_session.commit()
    db_session.refresh(task)

    # Now task has been killed 3 times total
    assert task.killed_retries == 3

    should_retry, reason = should_retry_task(task, max_failed_retries=2, max_killed_retries=2)

    assert not should_retry
    assert reason == "max_killed_retries_exceeded"


@pytest.mark.unit
@pytest.mark.parametrize(
    "num_retries,max_allowed,expected_retry",
    [
        (0, 0, False),  # No retries allowed, task already failed once
        (0, 1, True),  # First failure, can retry once
        (1, 1, False),  # Two failures total (initial + 1), max 1 allowed - exceeded
        (0, 2, True),  # First failure, within limit
        (4, 3, False),  # Five failures total, well over limit
    ],
)
def test_failed_retry_limits(db_session, task_factory, num_retries, max_allowed, expected_retry):
    """Test various failed retry limit scenarios.

    Note: task starts with one FAILED status, then we add num_retries more.
    So total failures = 1 + num_retries.
    """
    task = task_factory(status=TaskStatusEnum.FAILED)

    # Add additional failures
    for _ in range(num_retries):
        status = TaskStatusEntry(
            task=task,
            status=TaskStatusEnum.FAILED,
            worker=task.current_status.worker,
        )
        db_session.add(status)
    db_session.commit()
    db_session.refresh(task)

    should_retry, reason = should_retry_task(
        task, max_failed_retries=max_allowed, max_killed_retries=0
    )

    assert should_retry == expected_retry


@pytest.mark.unit
def test_should_retry_running_task_with_worker_availability(task_factory):
    """Running tasks with available worker slots should be retried."""
    task = task_factory(status=TaskStatusEnum.RUNNING, max_parallel_workers=2)

    # Only 1 worker currently running (the one that created the status)
    should_retry, reason = should_retry_task(task, max_failed_retries=2, max_killed_retries=2)

    assert should_retry
    assert reason is None


@pytest.mark.unit
def test_should_not_retry_running_task_at_max_workers(db_session, task_factory, worker_factory):
    """Running tasks at max workers should not allow more workers."""
    workflow = task_factory(status=TaskStatusEnum.RUNNING, max_parallel_workers=1).workflow
    worker = worker_factory(workflow=workflow)
    task = task_factory(
        task_id="multi-worker-task",
        status=TaskStatusEnum.RUNNING,
        max_parallel_workers=1,
        workflow=workflow,
        worker=worker,
    )

    # Task already has 1 worker and max is 1
    should_retry, reason = should_retry_task(task, max_failed_retries=2, max_killed_retries=2)

    assert not should_retry
    assert reason == "max_workers_reached"


@pytest.mark.unit
@pytest.mark.parametrize(
    "failure_type",
    [TaskStatusEnum.FAILED, TaskStatusEnum.KILLED],
)
def test_retry_policy_for_different_failure_types(task_factory, failure_type):
    """Both failed and killed tasks should follow their respective retry policies."""
    task = task_factory(status=failure_type)

    should_retry, reason = should_retry_task(task, max_failed_retries=1, max_killed_retries=1)

    assert should_retry
    assert reason is None
