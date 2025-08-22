import threading
from datetime import datetime

from flufl.lock import Lock
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from laufband.db import WorkerEntry, WorkerStatus


def heartbeat(lock: Lock, db: str, identifier: str, stop_event: threading.Event):
    engine = create_engine(db, echo=False)
    session = Session(engine)

    heartbeat_interval = 30  # seconds

    with lock:
        with session.begin():
            worker = session.get(WorkerEntry, identifier)
            if worker is None:
                raise ValueError(f"Worker with identifier {identifier} not found.")
            worker.last_heartbeat = datetime.now()
            heartbeat_interval = worker.heartbeat_interval
            session.commit()

    while not stop_event.wait(heartbeat_interval):
        with lock:
            with session.begin():
                worker = session.get(WorkerEntry, identifier)
                if worker is None:
                    raise ValueError(f"Worker with identifier {identifier} not found.")
                worker.last_heartbeat = datetime.now()
                session.commit()
    with lock:
        with session.begin():
            worker = session.get(WorkerEntry, identifier)
            if worker is not None:
                worker.status = WorkerStatus.OFFLINE
                session.commit()
