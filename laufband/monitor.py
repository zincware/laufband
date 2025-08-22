import dataclasses

from flufl.lock import Lock
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from laufband.db import TaskEntry, TaskStatusEnum, WorkerEntry, WorkflowEntry


@dataclasses.dataclass
class Monitor:
    lock: Lock = Lock("graphband.lock")
    db: str = "sqlite:///graphband.sqlite"

    def __post_init__(self):
        self.engine = create_engine(self.db)

    def get_workers(self) -> list[WorkerEntry]:
        with Session(self.engine) as session:
            workers = session.query(WorkerEntry).all()
            # Detach all objects from session so they can be used outside the context
            session.expunge_all()
            return workers

    def get_tasks(
        self, state: TaskStatusEnum | None = None, worker: WorkerEntry | None = None
    ) -> list[TaskEntry]:
        with Session(self.engine) as session:
            tasks = session.query(TaskEntry).all()
            
            # Load all relationships while still in session
            for task in tasks:
                _ = task.statuses  # Force loading of statuses
                for status_entry in task.statuses:
                    _ = status_entry.worker  # Force loading of worker
            
            if state is not None:
                tasks = [task for task in tasks if task.statuses and task.current_status.status == state]
            if worker is not None:
                # check if worker in any status for all tasks is the current worker
                task_dict = {}
                for task in tasks:
                    for status_entry in task.statuses:
                        if status_entry.worker_id == worker.id:
                            task_dict[task.id] = task
                            break
                tasks = list(task_dict.values())          
            session.expunge_all()
            
            return tasks

    def get_workflow(self) -> WorkflowEntry:
        with Session(self.engine) as session:
            workflow = (
                session.query(WorkflowEntry).filter(WorkflowEntry.id == "main").first()
            )
            if workflow is None:
                raise ValueError("Workflow 'main' not found.")
            session.expunge_all()
            return workflow
