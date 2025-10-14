"""Unit tests for database model properties and methods."""

import pytest

from laufband.db import TaskStatusEntry, TaskStatusEnum, WorkerStatus


@pytest.mark.unit
def test_worker_runtime_calculation_idle(worker_factory, mock_time):
    """Worker runtime should be calculated correctly for idle workers."""
    worker = worker_factory(status=WorkerStatus.IDLE)

    # Advance time
    mock_time.advance(30)

    runtime = worker.calculate_runtime(mock_time)
    assert runtime.total_seconds() == 30


@pytest.mark.unit
def test_worker_runtime_calculation_offline(worker_factory, mock_time):
    """Worker runtime for offline workers should use last_heartbeat."""
    worker = worker_factory(status=WorkerStatus.IDLE)

    # Advance time and mark offline
    mock_time.advance(20)
    worker.last_heartbeat = mock_time.now()
    worker.status = WorkerStatus.OFFLINE

    # Advance more time (should not affect offline worker runtime)
    mock_time.advance(10)

    runtime = worker.calculate_runtime(mock_time)
    # Runtime should be 20 seconds (started to last heartbeat)
    assert runtime.total_seconds() == 20


@pytest.mark.unit
def test_task_failed_retries_count(db_session, task_factory):
    """Task should correctly count failed retries."""
    task = task_factory(status=TaskStatusEnum.RUNNING)

    # Initially no failed retries
    assert task.failed_retries == -1

    # Add failed status
    db_session.add(
        TaskStatusEntry(
            task=task, status=TaskStatusEnum.FAILED, worker=task.current_status.worker
        )
    )
    db_session.commit()
    db_session.refresh(task)

    assert task.failed_retries == 1

    # Add more failures
    db_session.add(
        TaskStatusEntry(
            task=task, status=TaskStatusEnum.FAILED, worker=task.current_status.worker
        )
    )
    db_session.commit()
    db_session.refresh(task)

    assert task.failed_retries == 2


@pytest.mark.unit
def test_task_killed_retries_count(db_session, task_factory):
    """Task should correctly count killed retries."""
    task = task_factory(status=TaskStatusEnum.RUNNING)

    # Initially no killed retries
    assert task.killed_retries == -1

    # Add killed status
    db_session.add(
        TaskStatusEntry(
            task=task, status=TaskStatusEnum.KILLED, worker=task.current_status.worker
        )
    )
    db_session.commit()
    db_session.refresh(task)

    assert task.killed_retries == 1


@pytest.mark.unit
def test_task_runtime_completed(db_session, task_factory, mock_time):
    """Completed task should have positive runtime."""
    task = task_factory(status=TaskStatusEnum.RUNNING)

    # Advance time and complete
    mock_time.advance(15)

    db_session.add(
        TaskStatusEntry(
            task=task,
            status=TaskStatusEnum.COMPLETED,
            worker=task.current_status.worker,
            timestamp=mock_time.now(),
        )
    )
    db_session.commit()
    db_session.refresh(task)

    runtime = task.runtime
    assert runtime == 15


@pytest.mark.unit
def test_task_runtime_not_completed(task_factory):
    """Non-completed task should have runtime -1."""
    task = task_factory(status=TaskStatusEnum.RUNNING)

    assert task.runtime == -1


@pytest.mark.unit
def test_task_active_workers_count(db_session, task_factory, worker_factory):
    """Task should correctly count active workers."""
    workflow = task_factory(status=TaskStatusEnum.RUNNING).workflow
    worker1 = worker_factory(identifier="w1", workflow=workflow)
    worker2 = worker_factory(identifier="w2", workflow=workflow)

    task = task_factory(
        task_id="multi-worker",
        status=TaskStatusEnum.RUNNING,
        workflow=workflow,
        worker=worker1,
        max_parallel_workers=2,
    )

    # Initially 1 worker
    assert task.active_workers == 1

    # Add second worker
    db_session.add(
        TaskStatusEntry(task=task, status=TaskStatusEnum.RUNNING, worker=worker2)
    )
    db_session.commit()
    db_session.refresh(task)

    assert task.active_workers == 2

    # One worker completes
    db_session.add(
        TaskStatusEntry(task=task, status=TaskStatusEnum.COMPLETED, worker=worker1)
    )
    db_session.commit()
    db_session.refresh(task)

    # Should still have 1 active (worker2) until it also completes
    assert task.active_workers == 1


@pytest.mark.unit
def test_task_worker_availability_single_worker(task_factory):
    """Single worker task with running worker should not be available."""
    task = task_factory(status=TaskStatusEnum.RUNNING, max_parallel_workers=1)

    assert not task.worker_availability


@pytest.mark.unit
def test_task_worker_availability_multi_worker(
    db_session, task_factory, worker_factory
):
    """Multi-worker task should show availability until max reached."""
    workflow = task_factory(status=TaskStatusEnum.RUNNING).workflow
    worker1 = worker_factory(identifier="w1", workflow=workflow)

    task = task_factory(
        task_id="multi",
        status=TaskStatusEnum.RUNNING,
        workflow=workflow,
        worker=worker1,
        max_parallel_workers=2,
    )

    # 1 worker running, max 2 - should be available
    assert task.worker_availability

    # Add second worker
    worker2 = worker_factory(identifier="w2", workflow=workflow)
    db_session.add(
        TaskStatusEntry(task=task, status=TaskStatusEnum.RUNNING, worker=worker2)
    )
    db_session.commit()
    db_session.refresh(task)

    # 2 workers running, max 2 - should not be available
    assert not task.worker_availability


@pytest.mark.unit
def test_task_completed_property(db_session, task_factory):
    """Task completed property should check status and active workers."""
    task = task_factory(status=TaskStatusEnum.RUNNING)

    assert not task.completed

    # Complete the task
    db_session.add(
        TaskStatusEntry(
            task=task,
            status=TaskStatusEnum.COMPLETED,
            worker=task.current_status.worker,
        )
    )
    db_session.commit()
    db_session.refresh(task)

    assert task.completed


@pytest.mark.unit
def test_worker_running_tasks_property(db_session, worker_factory, task_factory):
    """Worker should correctly track its running tasks."""
    workflow = task_factory(status=TaskStatusEnum.RUNNING).workflow
    worker = worker_factory(workflow=workflow, identifier="test-worker")

    # Create multiple tasks
    task_factory(
        task_id="t1", status=TaskStatusEnum.RUNNING, worker=worker, workflow=workflow
    )
    task_factory(
        task_id="t2", status=TaskStatusEnum.RUNNING, worker=worker, workflow=workflow
    )
    task_factory(
        task_id="t3", status=TaskStatusEnum.COMPLETED, worker=worker, workflow=workflow
    )

    db_session.refresh(worker)

    running = worker.running_tasks
    running_ids = {t.id for t in running}

    # Should only have t1 and t2, not t3 (completed)
    assert running_ids == {"t1", "t2"}
