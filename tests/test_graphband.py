import multiprocessing
import os
import time
import typing as t

import networkx as nx
import pytest
from flufl.lock import Lock
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from laufband import Graphband, GraphTraversalProtocol, Task
from laufband.db import TaskEntry, TaskStatusEnum, WorkerEntry, WorkerStatus


def sequential_task():
    for i in range(10):
        yield Task(id=f"task_{i}", data={"value": i})


def sequential_multi_worker_task():
    for i in range(10):
        yield Task(id=f"task_{i}", data={"value": i}, max_parallel_workers=2)


def sequential_task_with_labels():
    for i in range(5):
        yield Task(id=f"task_{i}", data={"value": i}, requirements={"cpu"})
    for i in range(5, 10):
        yield Task(id=f"task_{i}", data={"value": i}, requirements={"gpu"})


def graph_task():
    digraph = nx.DiGraph()
    edges = [
        ("a", "b"),
        ("a", "c"),
        ("b", "d"),
        ("b", "e"),
        ("c", "f"),
        ("c", "g"),
    ]
    digraph.add_edges_from(edges)
    digraph.nodes["b"]["requirements"] = {"b-branch"}
    digraph.nodes["d"]["requirements"] = {"b-branch"}
    digraph.nodes["e"]["requirements"] = {"b-branch"}
    for node in nx.topological_sort(digraph):
        yield Task(
            id=node,
            data=node,
            dependencies=set(digraph.predecessors(node)),
            requirements=digraph.nodes[node].get("requirements", {"main"}),
        )


def multi_dependency_graph_task():
    """Create a graph where some tasks depend on multiple previous tasks.

    Graph structure:
    a --> c
    b --> c  (c depends on both a and b)
    c --> e
    d --> e  (e depends on both c and d)
    """
    digraph = nx.DiGraph()
    edges = [
        ("a", "c"),
        ("b", "c"),
        ("c", "e"),
        ("d", "e"),
    ]
    digraph.add_edges_from(edges)
    for node in nx.topological_sort(digraph):
        yield Task(
            id=node,
            data=node,
            dependencies=set(digraph.predecessors(node)),
        )


@pytest.mark.human_reviewed
def test_graphband_sequential_success(tmp_path):
    pbar = Graphband(
        sequential_task(),
        db=f"sqlite:///{tmp_path}/graphband.sqlite",
        lock=Lock(f"{tmp_path}/graphband.lock"),
    )
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
            assert task.current_status.worker == workers[0]
            assert task.id == f"task_{id}"

    # if we no iterate again, we yield nothing
    assert list(pbar) == []

    pbar = Graphband(
        sequential_task(),
        db=f"sqlite:///{tmp_path}/graphband.sqlite",
        lock=Lock(f"{tmp_path}/graphband.lock"),
        identifier="2nd-worker",
    )
    assert list(pbar) == []  # another iterator won't do anything now


@pytest.mark.human_reviewed
def test_graphband_sequential_close_and_resume(tmp_path):
    pbar = Graphband(
        sequential_task(),
        db=f"sqlite:///{tmp_path}/graphband.sqlite",
        lock=Lock(f"{tmp_path}/graphband.lock"),
    )

    items = []
    for item in pbar:
        items.append(item)
        if item.id == "task_5":
            pbar.close()  # this counts as this task succeeded

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
    assert len({x.id for x in items}) == 10

    with Session(pbar._engine) as session:
        tasks = session.query(TaskEntry).all()
        assert len(tasks) == 10
        assert all(
            task.current_status.status == TaskStatusEnum.COMPLETED for task in tasks
        )


@pytest.mark.human_reviewed
def test_graphband_sequential_break_and_resume(tmp_path):
    pbar = Graphband(
        sequential_task(),
        db=f"sqlite:///{tmp_path}/graphband.sqlite",
        lock=Lock(f"{tmp_path}/graphband.lock"),
    )

    items = []
    for item in pbar:
        if item.id == "task_5":
            break  # this counts as this task failed
        items.append(item)

    assert len(items) == 5

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

    assert len(items) == 9

    with Session(pbar._engine) as session:
        tasks = session.query(TaskEntry).all()
        assert len(tasks) == 10
        for idx, task in enumerate(tasks):
            if idx == 5:
                assert task.current_status.status == TaskStatusEnum.FAILED
            else:
                assert task.current_status.status == TaskStatusEnum.COMPLETED


@pytest.mark.human_reviewed
def test_graphband_sequential_break_and_retry(tmp_path):
    pbar = Graphband(
        sequential_task(),
        db=f"sqlite:///{tmp_path}/graphband.sqlite",
        lock=Lock(f"{tmp_path}/graphband.lock"),
        max_failed_retries=2,
    )

    items = []
    for item in pbar:
        if item.id == "task_5":
            break
        items.append(item)

    assert len(items) == 5
    assert "task_5" in pbar._failed_job_cache
    items.extend(list(pbar))
    assert len(pbar._failed_job_cache) == 0
    # failed job has been cleaned up after iteration
    assert len([x.id for x in items]) == 10
    # the failed task has been retried and added.

    with Session(pbar._engine) as session:
        task_5 = session.query(TaskEntry).filter(TaskEntry.id == "task_5").first()
        assert task_5 is not None
        assert task_5.statuses[0].status == TaskStatusEnum.RUNNING
        assert task_5.statuses[1].status == TaskStatusEnum.FAILED
        assert task_5.statuses[2].status == TaskStatusEnum.RUNNING
        assert task_5.statuses[3].status == TaskStatusEnum.COMPLETED


def test_duplicate_worker(tmp_path):
    _ = Graphband(
        sequential_task(),
        db=f"sqlite:///{tmp_path}/graphband.sqlite",
        lock=Lock(f"{tmp_path}/graphband.lock"),
    )
    with pytest.raises(ValueError, match="Worker with this identifier already exists"):
        _ = Graphband(
            sequential_task(),
            db=f"sqlite:///{tmp_path}/graphband.sqlite",
            lock=Lock(f"{tmp_path}/graphband.lock"),
        )


def task_worker(
    iterator: t.Type[GraphTraversalProtocol],
    lock_path: str,
    db: str,
    file: str,
    timeout: float,
    **kwargs: dict,
):
    lock = Lock(lock_path)
    pbar = Graphband(iterator(), lock=lock, db=db, **kwargs)
    for task in pbar:
        with pbar.lock:
            with open(file, "a") as f:
                f.write(f"{task.id} - {pbar.identifier} \n")
        time.sleep(timeout)


@pytest.mark.human_reviewed
@pytest.mark.parametrize("num_processes", [1, 2, 4])
def test_multiprocessing_sequential_task(tmp_path, num_processes):
    """Test laufband using a multiprocessing pool."""
    lock_path = f"{tmp_path}/graphband.lock"
    db = f"sqlite:///{tmp_path}/graphband.sqlite"
    file = f"{tmp_path}/output.txt"

    with multiprocessing.Pool(processes=num_processes) as pool:
        # increase the timeout for more processes
        pool.starmap(
            task_worker,
            [(sequential_task, lock_path, db, file, num_processes * 0.2)]
            * num_processes,
        )

    worker = []
    tasks = []
    with open(file, "r") as f:
        for line in f:
            task_id, worker_id = line.strip().split(" - ")
            worker.append(worker_id)
            tasks.append(task_id)

    assert len(set(worker)) == num_processes
    assert len(set(tasks)) == 10


def test_sequential_multi_worker_task(tmp_path):
    lock_path = f"{tmp_path}/graphband.lock"
    db = f"sqlite:///{tmp_path}/graphband.sqlite"
    file = f"{tmp_path}/output.txt"
    num_processes = 4

    with multiprocessing.Pool(processes=num_processes) as pool:
        # increase the timeout for more processes
        pool.starmap(
            task_worker,
            [(sequential_multi_worker_task, lock_path, db, file, num_processes * 0.2)]
            * num_processes,
        )

    worker = []
    tasks = []
    with open(file, "r") as f:
        for line in f:
            task_id, worker_id = line.strip().split(" - ")
            worker.append(worker_id)
            tasks.append(task_id)
    assert len(set(worker)) == num_processes
    assert len(set(tasks)) == 10
    assert 18 <= len(tasks) <= 20, f"Expected ~20 task executions, got {len(tasks)}"
    # with the given timeout we expect each job to be processed by 2 workers.


def test_kill_sequential_task_worker(tmp_path):
    lock_path = f"{tmp_path}/graphband.lock"
    db = f"sqlite:///{tmp_path}/graphband.sqlite"
    file = f"{tmp_path}/output.txt"

    proc = multiprocessing.Process(
        target=task_worker,
        args=(sequential_task, lock_path, db, file, 2),
        kwargs={
            "heartbeat_timeout": "2",
            "heartbeat_interval": "1",
        },
    )
    proc.start()
    time.sleep(1)  # let the worker start and process about 4 tasks
    # kill the worker immediately with no time to properly exit
    proc.kill()
    proc.join()
    # assert the worker is still registered as "online"
    engine = create_engine(db)
    with Session(engine) as session:
        tasks = session.query(TaskEntry).all()
        assert len(tasks) == 1
        assert tasks[0].current_status.status == TaskStatusEnum.RUNNING
        assert tasks[0].current_status.worker.status == WorkerStatus.BUSY
        assert tasks[0].current_status.worker.heartbeat_expired is False
        time.sleep(2)  # wait for the heartbeat to expire
        assert tasks[0].current_status.worker.heartbeat_expired is True

    task_worker(
        sequential_task,
        lock_path,
        db,
        file,
        0.1,
        heartbeat_timeout=2,
        heartbeat_interval=1,
    )

    with Session(engine) as session:
        w1 = session.get(WorkerEntry, proc.pid)
        assert w1 is not None
        assert w1.status == WorkerStatus.KILLED
        assert len(w1.running_tasks) == 0
        w2 = session.get(WorkerEntry, os.getpid())
        assert w2 is not None
        assert w2.status == WorkerStatus.OFFLINE
        assert len(w2.running_tasks) == 0

        tasks = session.query(TaskEntry).all()
        assert len(tasks) == 10
        assert tasks[0].current_status.status == TaskStatusEnum.KILLED
        for task in tasks[1:]:
            assert task.current_status.status == TaskStatusEnum.COMPLETED

    task_worker(
        sequential_task,
        lock_path,
        db,
        file,
        0.1,
        heartbeat_timeout=2,
        heartbeat_interval=1,
        max_killed_retries=2,
        identifier="killed-retry-worker",
    )

    with Session(engine) as session:
        w3 = session.get(WorkerEntry, "killed-retry-worker")
        assert w3 is not None
        assert w3.status == WorkerStatus.OFFLINE
        tasks = session.query(TaskEntry).all()
        assert tasks[0].current_status.status == TaskStatusEnum.COMPLETED
        assert tasks[0].current_status.worker == w3


def test_sequential_task_with_labels(tmp_path):
    cpu_worker = Graphband(
        sequential_task_with_labels(),
        db=f"sqlite:///{tmp_path}/graphband.sqlite",
        lock=Lock(f"{tmp_path}/graphband.lock"),
        labels={"cpu"},
        identifier="cpu_worker",
    )
    gpu_worker = Graphband(
        sequential_task_with_labels(),
        db=f"sqlite:///{tmp_path}/graphband.sqlite",
        lock=Lock(f"{tmp_path}/graphband.lock"),
        labels={"gpu"},
        identifier="gpu_worker",
    )
    assert len(list(cpu_worker)) == 5
    assert len(list(gpu_worker)) == 5

    with Session(cpu_worker._engine) as session:
        cpu_tasks = (
            session.query(TaskEntry)
            .filter(TaskEntry.requirements.contains("cpu"))
            .all()
        )
        gpu_tasks = (
            session.query(TaskEntry)
            .filter(TaskEntry.requirements.contains("gpu"))
            .all()
        )
        assert len(cpu_tasks) == 5
        assert len(gpu_tasks) == 5


def test_sequential_task_with_labels_multi_label_worker(tmp_path):
    worker = Graphband(
        sequential_task_with_labels(),
        db=f"sqlite:///{tmp_path}/graphband.sqlite",
        lock=Lock(f"{tmp_path}/graphband.lock"),
        labels={"cpu", "gpu"},
    )
    assert len(list(worker)) == 10


def test_failure_policy_stop(tmp_path):
    """Test if failure policy stop works."""
    worker = Graphband(
        sequential_task(),
        db=f"sqlite:///{tmp_path}/graphband.sqlite",
        lock=Lock(f"{tmp_path}/graphband.lock"),
        failure_policy="stop",
    )
    for item in worker:
        if item.id == "task_5":
            break  # counts as a failed job

    # reiterating will raise an error, as there is one job
    # in the database that has failed
    with pytest.raises(RuntimeError):
        for idx in worker:
            pass


def test_graph_task(tmp_path):
    w1 = Graphband(
        graph_task(),
        db=f"sqlite:///{tmp_path}/graphband.sqlite",
        lock=Lock(f"{tmp_path}/graphband.lock"),
        labels={"b-branch"},
        identifier="b-branch-worker",
    )
    items = [x.id for x in w1]
    assert items == []

    w2 = Graphband(
        graph_task(),
        db=f"sqlite:///{tmp_path}/graphband.sqlite",
        lock=Lock(f"{tmp_path}/graphband.lock"),
        identifier="main-worker",
        labels={"main"},
    )
    items = [x.id for x in w2]
    assert items == [
        "a",
        "c",
        "f",
        "g",
    ]  # the "b", "d", "e" are in the b-branch which should not be run
    items = [x.id for x in w1]
    assert items == ["b", "d", "e"]

    # assert dependencies are stored correctly
    expected_dependencies = {
        "a": [],
        "b": ["a"],
        "c": ["a"],
        "d": ["b"],
        "e": ["b"],
        "f": ["c"],
        "g": ["c"],
    }
    with Session(w1._engine) as session:
        entries = {}
        for task_id in expected_dependencies:
            entry = session.query(TaskEntry).filter(TaskEntry.id == task_id).first()
            assert entry is not None
            assert entry.current_status.status == TaskStatusEnum.COMPLETED
            entries[task_id] = entry

        for task_id, deps in expected_dependencies.items():
            assert entries[task_id].current_status.dependencies == [
                entries[d] for d in deps
            ]


def test_multi_dependency_graph_task(tmp_path):
    """Test task execution with multiple dependencies per task."""
    worker = Graphband(
        multi_dependency_graph_task(),
        db=f"sqlite:///{tmp_path}/graphband.sqlite",
        lock=Lock(f"{tmp_path}/graphband.lock"),
    )
    items = [x.id for x in worker]
    assert set(items) == {"a", "b", "c", "d", "e"}

    # Verify dependencies are stored correctly
    expected_dependencies = {
        "a": [],
        "b": [],
        "c": ["a", "b"],  # c depends on both a and b
        "d": [],
        "e": ["c", "d"],  # e depends on both c and d
    }

    with Session(worker._engine) as session:
        entries = {}
        for task_id in expected_dependencies:
            entry = session.query(TaskEntry).filter(TaskEntry.id == task_id).first()
            assert entry is not None
            assert entry.current_status.status == TaskStatusEnum.COMPLETED
            entries[task_id] = entry

        for task_id, deps in expected_dependencies.items():
            assert entries[task_id].current_status.dependencies == [
                entries[d] for d in deps
            ]
