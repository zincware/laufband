from datetime import datetime
from enum import StrEnum
from typing import List

from sqlalchemy import (
    JSON,
    DateTime,
    Enum,
    ForeignKey,
    Integer,
    String,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

# from sqlalchemy.orm import MappedAsDataclass


# --- Base class ---
class Base(DeclarativeBase):
    pass


# --- Enums ---
class WorkerStatus(StrEnum):
    IDLE = "idle"
    BUSY = "busy"
    OFFLINE = "offline"
    KILLED = "killed"


class TaskStatusEnum(StrEnum):
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    ABANDONED = "abandoned"
    CANCELLED = "cancelled"
    BLOCKED = "blocked"
    KILLED = "killed"


class WorkflowEntry(Base):
    __tablename__ = "workflows"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    total_tasks: Mapped[int] = mapped_column(Integer, nullable=True)

    workers: Mapped[List["WorkerEntry"]] = relationship(back_populates="workflow")
    tasks: Mapped[List["TaskEntry"]] = relationship(back_populates="workflow")


# --- Worker ---
class WorkerEntry(Base):
    __tablename__ = "workers"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    status: Mapped[WorkerStatus] = mapped_column(Enum(WorkerStatus))

    last_heartbeat: Mapped[datetime] = mapped_column(DateTime, default=datetime.now)
    heartbeat_interval: Mapped[int] = mapped_column(Integer, default=30)
    heartbeat_timeout: Mapped[int] = mapped_column(Integer, default=60)

    labels: Mapped[List[str]] = mapped_column(JSON, default=list)

    hostname: Mapped[str | None] = mapped_column(String, nullable=True)
    pid: Mapped[int | None] = mapped_column(Integer, nullable=True)
    started_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now)

    workflow_id: Mapped[str] = mapped_column(ForeignKey("workflows.id"))

    workflow: Mapped["WorkflowEntry"] = relationship(back_populates="workers")

    # One worker can appear in many TaskStatusEntries
    task_statuses: Mapped[List["TaskStatusEntry"]] = relationship(
        back_populates="worker",
    )

    @property
    def heartbeat_expired(self) -> bool:
        """Check if the worker's heartbeat is expired."""
        return (
            datetime.now() - self.last_heartbeat
        ).total_seconds() > self.heartbeat_timeout

    @property
    def running_tasks(self) -> set["TaskEntry"]:
        """Get all running tasks for this worker."""
        running_tasks = set()
        for status in self.task_statuses:
            if status.status == TaskStatusEnum.RUNNING:
                running_tasks.add(status.task)
            else:
                # remove if running has been outdated.
                running_tasks.discard(status.task)
        return running_tasks


# --- TaskStatusEntry ---
class TaskStatusEntry(Base):
    __tablename__ = "task_statuses"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    task_id: Mapped[str] = mapped_column(ForeignKey("tasks.id"))

    status: Mapped[TaskStatusEnum] = mapped_column(Enum(TaskStatusEnum))
    timestamp: Mapped[datetime] = mapped_column(DateTime, default=datetime.now)

    # new: one worker per status
    worker_id: Mapped[str | None] = mapped_column(
        ForeignKey("workers.id"), nullable=True
    )
    worker: Mapped[WorkerEntry] = relationship(back_populates="task_statuses")

    task: Mapped["TaskEntry"] = relationship(back_populates="statuses")


# --- TaskEntry ---
class TaskEntry(Base):
    __tablename__ = "tasks"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    requirements: Mapped[List[str]] = mapped_column(JSON, default=list)

    workflow_id: Mapped[str] = mapped_column(ForeignKey("workflows.id"))

    workflow: Mapped["WorkflowEntry"] = relationship(back_populates="tasks")

    statuses: Mapped[List[TaskStatusEntry]] = relationship(
        back_populates="task",
        cascade="all, delete-orphan",
        order_by="TaskStatusEntry.timestamp",
    )

    max_parallel_workers: Mapped[int] = mapped_column(Integer, default=1)

    @property
    def current_status(self) -> TaskStatusEntry:
        return self.statuses[-1]

    @property
    def failed_retries(self) -> int:
        result = sum(1 for x in self.statuses if x.status == TaskStatusEnum.FAILED)
        if result == 0:
            # there is no "0" failed retries, just no failed retries and we
            # indicate that with -1
            return -1
        return result

    @property
    def killed_retries(self) -> int:
        result = sum(1 for x in self.statuses if x.status == TaskStatusEnum.KILLED)
        if result == 0:
            # there is no "0" killed retries, just no killed retries and we
            # indicate that with -1
            return -1
        return result

    @property
    def runtime(self) -> float:
        start_time = self.statuses[0].timestamp
        end_time = self.statuses[-1].timestamp
        if self.statuses[-1].status != TaskStatusEnum.COMPLETED:
            return -1
        return (end_time - start_time).total_seconds()

    @property
    def active_workers(self) -> int:
        running_workers = set()
        for status in self.statuses:
            if status.status == TaskStatusEnum.RUNNING:
                running_workers.add(status.worker_id)
            else:
                # discard workers that, e.g. died while running
                running_workers.discard(status.worker_id)
        return len(running_workers)

    @property
    def worker_availability(self) -> bool:
        running_workers = set()
        for status in self.statuses:
            if status.worker_id is None:
                continue
            if status.status == TaskStatusEnum.RUNNING:
                running_workers.add(status.worker_id)
            elif status.status == TaskStatusEnum.COMPLETED:
                if status.worker_id in running_workers:
                    # a worker has successfully completed and
                    #  no new workers should be picked up
                    return False
            else:
                # discard workers that, e.g. died while running
                running_workers.discard(status.worker_id)
        return len(running_workers) < self.max_parallel_workers

    @property
    def completed(self) -> bool:
        if self.active_workers > 0:
            return False
        return self.current_status.status == TaskStatusEnum.COMPLETED
