import threading

from flufl.lock import Lock, LockState
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from laufband.time_provider import RealTimeProvider
from laufband.worker_logic import (
    check_and_mark_expired_workers,
    update_worker_heartbeat,
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
    time_provider = RealTimeProvider()

    with db_lock:
        with Session() as session:
            worker = update_worker_heartbeat(session, identifier, time_provider)
            heartbeat_interval = worker.heartbeat_interval
            session.commit()

    while not stop_event.wait(heartbeat_interval):
        # Refresh user lock if we own it to prevent expiration during
        # long user operations but still handle cases, where this process
        # is killed and the lock should expire
        if user_file_lock.state == LockState.ours:
            user_file_lock.refresh(int(heartbeat_interval * 1.5))
        with db_lock:
            with Session() as session:
                worker = update_worker_heartbeat(session, identifier, time_provider)
                workflow_id = worker.workflow_id
                # Check and mark expired workers
                check_and_mark_expired_workers(session, workflow_id, time_provider)
                session.commit()

    with db_lock:
        with Session() as session:
            from laufband.db import WorkerEntry, WorkerStatus

            worker = session.get(WorkerEntry, identifier)
            if worker is not None:
                worker.status = WorkerStatus.OFFLINE
                session.add(worker)
                session.commit()
