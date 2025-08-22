from laufband import Graphband, Task
from flufl.lock import Lock
from sqlalchemy.orm import Session
from laufband.db import TaskEntry, TaskStatusEnum, WorkerEntry, WorkerStatus
import pytest

def sequential_task():
    for i in range(10):
        yield Task(id=f"task_{i}", data={"value": i})

@pytest.mark.human_reviewed
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
        for id, task in enumerate(tasks):
            assert task.current_status.status == TaskStatusEnum.COMPLETED
            assert task.workers == workers
            assert task.id == f"task_{id}"

    # if we no iterate again, we yield nothing
    assert list(pbar) == []

@pytest.mark.human_reviewed
def test_graphband_sequential_close_and_resume(tmp_path):
    pbar = Graphband(sequential_task(), db=f"sqlite:///{tmp_path}/graphband.sqlite", lock=Lock(f"{tmp_path}/graphband.lock"))
    
    items = []
    for item in pbar:
        items.append(item)
        if item.id == "task_5":
            pbar.close() # this counts as this task succeeded
        
    assert len(items) == 6  # 0 to 5 inclusive

    with Session(pbar._engine) as session:
        tasks = session.query(TaskEntry).all()
        assert len(tasks) == 6
        for id, task in enumerate(tasks):
            assert task.current_status.status == TaskStatusEnum.COMPLETED
            assert task.id == f"task_{id}"

    for item in pbar:
        items.append(item)
    
    assert len(items) == 10  # 6 to 9 inclusive
    assert len(set(x.id for x in items)) == 10

    with Session(pbar._engine) as session:
        tasks = session.query(TaskEntry).all()
        assert len(tasks) == 10
        assert all(task.current_status.status == TaskStatusEnum.COMPLETED for task in tasks)

@pytest.mark.human_reviewed
def test_graphband_sequential_break_and_resume(tmp_path):
    pbar = Graphband(sequential_task(), db=f"sqlite:///{tmp_path}/graphband.sqlite", lock=Lock(f"{tmp_path}/graphband.lock"))
    
    items = []
    for item in pbar:
        items.append(item)
        if item.id == "task_5":
            break # this counts as this task failed

    assert len(items) == 6  # 0 to 5 inclusive

    with Session(pbar._engine) as session:
        tasks = session.query(TaskEntry).all()
        assert len(tasks) == 6
        for idx, task in enumerate(tasks):
            if idx == 5:
                assert task.current_status.status == TaskStatusEnum.FAILED
            else:
                assert task.current_status.status == TaskStatusEnum.COMPLETED

    for item in pbar:
        items.append(item)
    
    assert len(items) == 10  # includes the failed task data!

    with Session(pbar._engine) as session:
        tasks = session.query(TaskEntry).all()
        assert len(tasks) == 10
        for idx, task in enumerate(tasks):
            if idx == 5:
                assert task.current_status.status == TaskStatusEnum.FAILED
            else:
                assert task.current_status.status == TaskStatusEnum.COMPLETED
