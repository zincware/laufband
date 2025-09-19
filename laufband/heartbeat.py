import threading
from datetime import datetime

from flufl.lock import Lock, LockState
from sqlalchemy import create_engine
from sqlalchemy.orm import selectinload, sessionmaker

from laufband.db import (
    TaskStatusEntry,
    TaskStatusEnum,
    WorkerEntry,
    WorkerStatus,
)


def heartbeat(
    db_lock: Lock,
    user_file_lock: Lock,
    db: str,
    identifier: str,
    stop_event: threading.Event,
):
    engine = create_engine(db, echo=False)
    Session = sessionmaker(bind=engine)  # noqa: N806

    with db_lock:
        with Session() as session:
            worker = session.get(WorkerEntry, identifier)
            if worker is None:
                raise ValueError(f"Worker with identifier {identifier} not found.")
            worker.last_heartbeat = datetime.now()
            heartbeat_interval = worker.heartbeat_interval
            session.add(worker)
            session.commit()

    while not stop_event.wait(heartbeat_interval):
        # Refresh user lock if we own it to prevent expiration during
        # long user operations but still handle cases, where this process
        # is killed and the lock should expire
        if user_file_lock.state == LockState.ours:
            user_file_lock.refresh(int(heartbeat_interval * 1.5))
        with db_lock:
            with Session() as session:
                worker = session.get(WorkerEntry, identifier)
                if worker is None:
                    raise ValueError(f"Worker with identifier {identifier} not found.")
                worker.last_heartbeat = datetime.now()
                session.add(worker)
                # check expired heartbeats
                workflow_id = worker.workflow_id
                for w in (
                    session.query(WorkerEntry)
                    .options(selectinload(WorkerEntry.task_statuses))
                    .filter(
                        WorkerEntry.workflow_id == workflow_id,
                        WorkerEntry.status.in_([WorkerStatus.BUSY, WorkerStatus.IDLE]),
                    )
                    .all()
                ):
                    if w.heartbeat_expired:
                        w.status = WorkerStatus.KILLED
                        for task in w.running_tasks:
                            task_status = TaskStatusEntry(
                                status=TaskStatusEnum.KILLED, worker=w, task=task
                            )
                            session.add(task_status)
                        session.add(w)
                session.commit()

    with db_lock:
        with Session() as session:
            worker = session.get(WorkerEntry, identifier)
            if worker is not None:
                worker.status = WorkerStatus.OFFLINE
                session.add(worker)
                session.commit()
