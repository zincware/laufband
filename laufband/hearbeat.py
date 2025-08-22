import threading
from datetime import datetime

from flufl.lock import Lock
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from laufband.db import WorkerEntry, WorkerStatus, TaskEntry, TaskStatusEntry, TaskStatusEnum


def heartbeat(lock: Lock, db: str, identifier: str, stop_event: threading.Event):
    engine = create_engine(db, echo=False)
    session = Session(engine)

    with lock:
        with session:
            worker = session.get(WorkerEntry, identifier)
            if worker is None:
                raise ValueError(f"Worker with identifier {identifier} not found.")
            worker.last_heartbeat = datetime.now()
            heartbeat_interval = worker.heartbeat_interval
            session.commit()
            # check expired hearbeats
            for worker in session.query(WorkerEntry).all():
                if worker.heartbeat_expired:
                    worker.status = WorkerStatus.KILLED
                    for task in worker.running_tasks:
                        task_status = TaskStatusEntry(status=TaskStatusEnum.KILLED, worker=worker)
                        task.statuses.append(task_status)
                        session.add(task_status)
                    session.add(worker)
            session.commit()

    while not stop_event.wait(heartbeat_interval):
        with lock:
            with session:
                worker = session.get(WorkerEntry, identifier)
                if worker is None:
                    raise ValueError(f"Worker with identifier {identifier} not found.")
                worker.last_heartbeat = datetime.now()
                session.commit()
    with lock:
        with session:
            worker = session.get(WorkerEntry, identifier)
            if worker is not None:
                worker.status = WorkerStatus.OFFLINE
                session.commit()
