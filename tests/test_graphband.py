from laufband import Graphband, Task
from flufl.lock import Lock
from sqlalchemy.orm import Session
from laufband.db import TaskEntry, TaskStatusEntry, TaskStatusEnum, WorkerEntry, WorkerStatus

def sequential_task():
    for i in range(10):
        yield Task(id=f"task_{i}", data={"value": i})

def test_graphband_sequential_success(tmp_path):
    pbar = Graphband(sequential_task(), db=f"sqlite:///{tmp_path}/graphband.sqlite", lock=Lock(f"{tmp_path}/graphband.lock"))
    items = list(pbar)

    assert len(items) == 10
    with Session(pbar._engine) as session:
        workers = session.query(WorkerEntry).all()
        assert len(workers) == 1
        assert workers[0].status == WorkerStatus.OFFLINE
        tasks = session.query(TaskEntry).all()
        assert len(tasks) == 10
        for task in tasks:
            assert task.current_status.status == TaskStatusEnum.COMPLETED
            assert task.workers == workers

    # if we no iterate again, we yield nothing
    assert list(pbar) == []