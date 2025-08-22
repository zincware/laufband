import dataclasses
from flufl.lock import Lock

from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from laufband.db import WorkerEntry, TaskEntry, TaskStatusEnum, WorkflowEntry

@dataclasses.dataclass
class Monitor:
    lock: Lock = Lock("graphband.lock")
    db: str = "sqlite:///graphband.sqlite"

    def __post_init__(self):
        self.engine = create_engine(self.db)
        self.session = Session(self.engine)

    def get_workers(self) -> list[WorkerEntry]:
        return self.session.query(WorkerEntry).all()

    def get_tasks(self, state: TaskStatusEnum | None = None) -> list[TaskEntry]:
        tasks = self.session.query(TaskEntry).all()
        if state is not None:
            tasks = [task for task in tasks if task.current_status.status == state]
        return tasks

    def get_workflow(self) -> WorkflowEntry:
        workflow = self.session.query(WorkflowEntry).filter(WorkflowEntry.id == "main").first()
        if workflow is None:
            raise ValueError("Workflow 'main' not found.")
        return workflow
