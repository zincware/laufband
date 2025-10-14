"""Business logic for worker and task management.

Extracted for testability - these functions can be tested without database or multiprocessing.
"""

from sqlalchemy.orm import Session, selectinload

from laufband.db import (
    TaskEntry,
    TaskStatusEntry,
    TaskStatusEnum,
    WorkerEntry,
    WorkerStatus,
)
from laufband.time_provider import RealTimeProvider, TimeProvider


def check_and_mark_expired_workers(
    session: Session,
    workflow_id: str,
    time_provider: TimeProvider | None = None,
) -> list[WorkerEntry]:
    """Check for expired worker heartbeats and mark them as killed.

    Args:
        session: Database session
        workflow_id: Workflow to check workers for
        time_provider: Time provider for testing

    Returns:
        List of workers that were marked as killed
    """
    if time_provider is None:
        time_provider = RealTimeProvider()

    killed_workers = []

    for worker in (
        session.query(WorkerEntry)
        .options(selectinload(WorkerEntry.task_statuses))
        .filter(
            WorkerEntry.workflow_id == workflow_id,
            WorkerEntry.status.in_([WorkerStatus.BUSY, WorkerStatus.IDLE]),
        )
        .all()
    ):
        if worker.is_heartbeat_expired(time_provider):
            worker.status = WorkerStatus.KILLED
            for task in worker.running_tasks:
                task_status = TaskStatusEntry(
                    status=TaskStatusEnum.KILLED, worker=worker, task=task
                )
                session.add(task_status)
            session.add(worker)
            killed_workers.append(worker)

    return killed_workers


def should_retry_task(
    task: TaskEntry,
    max_failed_retries: int,
    max_killed_retries: int,
) -> tuple[bool, str | None]:
    """Determine if a task should be retried based on its current status.

    Args:
        task: Task to check
        max_failed_retries: Maximum number of failed retries allowed
        max_killed_retries: Maximum number of killed retries allowed

    Returns:
        Tuple of (should_retry, reason_if_not)
    """
    current_status = task.current_status.status

    if current_status == TaskStatusEnum.COMPLETED:
        return False, "already_completed"

    if current_status == TaskStatusEnum.RUNNING:
        # Check if worker availability allows another worker
        if not task.worker_availability:
            return False, "max_workers_reached"
        return True, None

    if current_status == TaskStatusEnum.FAILED:
        if task.failed_retries > max_failed_retries:
            return False, "max_failed_retries_exceeded"
        return True, None

    if current_status == TaskStatusEnum.KILLED:
        if task.killed_retries > max_killed_retries:
            return False, "max_killed_retries_exceeded"
        return True, None

    return True, None


def update_worker_heartbeat(
    session: Session,
    worker_id: str,
    time_provider: TimeProvider | None = None,
) -> WorkerEntry:
    """Update worker's last heartbeat timestamp.

    Args:
        session: Database session
        worker_id: Worker identifier
        time_provider: Time provider for testing

    Returns:
        Updated worker entry

    Raises:
        ValueError: If worker not found
    """
    if time_provider is None:
        time_provider = RealTimeProvider()

    worker = session.get(WorkerEntry, worker_id)
    if worker is None:
        raise ValueError(f"Worker with identifier {worker_id} not found.")

    worker.last_heartbeat = time_provider.now()
    session.add(worker)
    return worker


def create_worker_entry(
    session: Session,
    identifier: str,
    workflow_id: str,
    labels: list[str] | None = None,
    heartbeat_interval: int = 30,
    heartbeat_timeout: int = 60,
    hostname: str | None = None,
    pid: int | None = None,
    time_provider: TimeProvider | None = None,
) -> WorkerEntry:
    """Create a new worker entry in the database.

    Args:
        session: Database session
        identifier: Unique worker identifier
        workflow_id: Workflow this worker belongs to
        labels: Worker capability labels
        heartbeat_interval: Seconds between heartbeats
        heartbeat_timeout: Seconds before heartbeat expires
        hostname: Worker hostname
        pid: Worker process ID
        time_provider: Time provider for testing

    Returns:
        Created worker entry
    """
    if time_provider is None:
        time_provider = RealTimeProvider()

    now = time_provider.now()

    worker = WorkerEntry(
        id=identifier,
        status=WorkerStatus.IDLE,
        workflow_id=workflow_id,
        labels=labels or [],
        heartbeat_interval=heartbeat_interval,
        heartbeat_timeout=heartbeat_timeout,
        hostname=hostname,
        pid=pid,
        last_heartbeat=now,
        started_at=now,
    )
    session.add(worker)
    return worker
