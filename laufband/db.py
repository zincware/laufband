from datetime import datetime
from enum import StrEnum
from typing import List

from sqlalchemy import (
    JSON,
    Column,
    DateTime,
    Enum,
    ForeignKey,
    Integer,
    String,
    Table,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
# from sqlalchemy.orm import MappedAsDataclass


# --- Base class ---
class Base(DeclarativeBase):
    pass


task_workers = Table(
    "task_workers",
    Base.metadata,
    Column("task_id", ForeignKey("tasks.id"), primary_key=True),
    Column("worker_id", ForeignKey("workers.id"), primary_key=True),
)


# --- Enums ---
class WorkerStatus(StrEnum):
    IDLE = "idle"
    BUSY = "busy"
    OFFLINE = "offline"


class TaskStatusEnum(StrEnum):
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    ABANDONED = "abandoned"
    CANCELLED = "cancelled"
    BLOCKED = "blocked"
    KILLED = "killed"


# --- Worker ---
class WorkerEntry(Base):
    __tablename__ = "workers"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    status: Mapped[WorkerStatus] = mapped_column(Enum(WorkerStatus))

    last_heartbeat: Mapped[datetime] = mapped_column(DateTime, default=datetime.now)
    heartbeat_interval: Mapped[int] = mapped_column(Integer, default=30)  # seconds

    labels: Mapped[List[str]] = mapped_column(JSON, default=list)

    hostname: Mapped[str | None] = mapped_column(String, nullable=True)
    pid: Mapped[int | None] = mapped_column(Integer, nullable=True)
    started_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now)

    tasks: Mapped[List["TaskEntry"]] = relationship(
        secondary=task_workers,
        back_populates="workers",
    )


# --- TaskStatusEntry (history) ---
class TaskStatusEntry(Base):
    __tablename__ = "task_statuses"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    task_id: Mapped[str] = mapped_column(ForeignKey("tasks.id"))

    status: Mapped[TaskStatusEnum] = mapped_column(Enum(TaskStatusEnum))
    timestamp: Mapped[datetime] = mapped_column(DateTime, default=datetime.now)

    task: Mapped["TaskEntry"] = relationship(back_populates="statuses")


# --- TaskEntry ---
class TaskEntry(Base):
    __tablename__ = "tasks"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    requirements: Mapped[List[str]] = mapped_column(JSON, default=list)

    # one-to-many to TaskStatusEntry
    statuses: Mapped[List[TaskStatusEntry]] = relationship(
        back_populates="task",
        cascade="all, delete-orphan",
        order_by="TaskStatusEntry.timestamp",
    )

    max_parallel_workers: Mapped[int] = mapped_column(Integer, default=1)
    workers: Mapped[List[WorkerEntry]] = relationship(
        secondary=task_workers,
    )

    @property
    def current_status(self) -> TaskStatusEntry | None:
        return self.statuses[-1] if self.statuses else None
