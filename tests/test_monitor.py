import os
import typing as t

import pytest
from flufl.lock import Lock

from laufband import Graphband, Monitor, Task
from laufband.db import TaskStatusEnum


@pytest.fixture
def graphband_state(tmp_path) -> t.Tuple[Lock, str]:
    def sequential_task():
        for i in range(10):
            yield Task(id=f"task_{i}", data={"value": i})

    pbar = Graphband(
        sequential_task(),
        db=f"sqlite:///{tmp_path}/graphband.sqlite",
        lock=Lock(f"{tmp_path}/graphband.lock"),
    )
    for item in pbar:
        if item.id == "task_5":
            break

    return pbar.lock, pbar.db


def test_monitor(graphband_state):
    lock, db = graphband_state
    m = Monitor(lock=lock, db=db)
    worker = m.get_workers()
    tasks = m.get_tasks()
    assert len(worker) == 1
    assert worker[0].id == str(os.getpid())
    assert len(tasks) == 6
    assert tasks[0].id == "task_0"
    assert tasks[0].current_status.status == TaskStatusEnum.COMPLETED
    assert tasks[0].current_status.worker.id == str(os.getpid())
    assert tasks[-1].current_status.status == TaskStatusEnum.FAILED

    assert tasks[0].active_workers == 0
    assert tasks[0].completed is True
    assert tasks[-1].completed is False
    assert tasks[0].runtime > 0
    assert tasks[-1].runtime == -1  # because it was not completed

    completed_tasks = m.get_tasks(TaskStatusEnum.COMPLETED)
    assert len(completed_tasks) == 5
    failed_tasks = m.get_tasks(TaskStatusEnum.FAILED)
    assert len(failed_tasks) == 1
    running_tasks = m.get_tasks(TaskStatusEnum.RUNNING)
    assert len(running_tasks) == 0
    workflow = m.get_workflow()
    assert workflow.id == "main"
    assert workflow.total_tasks is None
