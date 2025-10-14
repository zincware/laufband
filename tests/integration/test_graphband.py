import multiprocessing
import os
import socket
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
    with pbar:
        items = list(pbar)
        assert pbar._context_managed
        assert len(items) == 10
        with Session(pbar._engine) as session:
            workers = session.query(WorkerEntry).all()
            assert len(workers) == 1
            assert workers[0].status == WorkerStatus.IDLE
            tasks = session.query(TaskEntry).all()
            assert len(tasks) == 10
            for id, task in enumerate(tasks):
                assert task.current_status.status == TaskStatusEnum.COMPLETED
                assert task.current_status.worker == workers[0]
                assert task.id == f"task_{id}"

        # if we no iterate again, we yield nothing
        assert list(pbar) == []

    # Test that worker goes offline when leaving context
    engine = pbar._engine

    with Session(engine) as session:
        workers = session.query(WorkerEntry).all()
        assert len(workers) == 1
        assert workers[0].status == WorkerStatus.OFFLINE

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
    with pytest.raises(
        ValueError, match="Worker with .* already exists with status 'idle'."
    ):
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
            [(sequential_task, lock_path, db, file, num_processes * 0.3)]
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
            "heartbeat_timeout": 2,
            "heartbeat_interval": 1,
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
        w1 = session.get(WorkerEntry, f"{socket.gethostname()}:{proc.pid}")
        assert w1 is not None
        assert w1.status == WorkerStatus.KILLED
        assert len(w1.running_tasks) == 0
        w2 = session.get(WorkerEntry, f"{socket.gethostname()}:{os.getpid()}")
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
            assert sorted(
                [e.id for e in entries[task_id].current_status.dependencies]
            ) == sorted(deps)


def test_multi_dependency_graph_task(tmp_path):
    """Test task execution with multiple dependencies per task."""
    worker = Graphband(
        multi_dependency_graph_task(),
        db=f"sqlite:///{tmp_path}/graphband.sqlite",
        lock=Lock(f"{tmp_path}/graphband.lock"),
    )
    items = [x.id for x in worker]
    assert set(items) == {"a", "b", "c", "d", "e"}

    expected_dependencies = {
        "a": [],
        "b": [],
        "c": ["a", "b"],
        "d": [],
        "e": ["c", "d"],
    }

    with Session(worker._engine) as session:
        entries = {}
        for task_id in expected_dependencies:
            entry = session.query(TaskEntry).filter(TaskEntry.id == task_id).first()
            assert entry is not None
            assert entry.current_status.status == TaskStatusEnum.COMPLETED
            entries[task_id] = entry

        for task_id, deps in expected_dependencies.items():
            assert sorted(
                [e.id for e in entries[task_id].current_status.dependencies]
            ) == sorted(deps)


def test_has_more_jobs(tmp_path):
    worker = Graphband(
        sequential_task(),
        db=f"sqlite:///{tmp_path}/graphband.sqlite",
        lock=Lock(f"{tmp_path}/graphband.lock"),
    )
    assert worker.has_more_jobs is True
    for item in worker:
        if item.id == "task_5":
            break
    # there are still 4 remaining tasks
    assert worker.has_more_jobs is True

    assert len(list(worker)) == 4
    # won't pick up the failed job
    assert worker.has_more_jobs is False

    w2 = Graphband(
        sequential_task(),
        db=f"sqlite:///{tmp_path}/graphband.sqlite",
        lock=Lock(f"{tmp_path}/graphband.lock"),
        max_failed_retries=2,
        identifier="retry-worker",
    )
    # will pick up the failed job
    assert w2.has_more_jobs is True
    assert len(list(w2)) == 1
    assert w2.has_more_jobs is False

    w3 = Graphband(
        sequential_task(),
        db=f"sqlite:///{tmp_path}/graphband.sqlite",
        lock=Lock(f"{tmp_path}/graphband.lock"),
        max_failed_retries=2,
        identifier="retry-worker-2",
    )
    assert w3.has_more_jobs is False  # all jobs are no completed successfully


def blocked_dependency_graph_task():
    """Create a graph where task b blocks task c due to label requirements.

    Graph structure:
    a --> b --> c
    Where b requires 'special-worker' label, but a and c require 'main' label.
    """
    digraph = nx.DiGraph()
    edges = [
        ("a", "b"),
        ("b", "c"),
    ]
    digraph.add_edges_from(edges)
    digraph.nodes["b"]["requirements"] = {"special-worker"}
    for node in nx.topological_sort(digraph):
        yield Task(
            id=node,
            data=node,
            dependencies=set(digraph.predecessors(node)),
            requirements=digraph.nodes[node].get("requirements", {"main"}),
        )


def test_has_more_jobs_with_blocked_dependencies(tmp_path):
    """Test has_more_jobs when dependencies are blocked by label mismatches.

    This test verifies the scenario where:
    - a --> b --> c dependency chain
    - b needs a different label than a, c
    - has_more_jobs for the a/c worker should be true until c is completed
    - but the worker won't be able to pick up c because b isn't completed
    """
    # Worker that can process 'main' tasks (a and c) but not 'special-worker' tasks (b)
    main_worker = Graphband(
        blocked_dependency_graph_task(),
        db=f"sqlite:///{tmp_path}/graphband.sqlite",
        lock=Lock(f"{tmp_path}/graphband.lock"),
        labels={"main"},
        identifier="main-worker",
    )

    # Initially should have jobs available
    assert main_worker.has_more_jobs is True

    # Process available tasks - should only get task 'a'
    items = list(main_worker)
    assert len(items) == 1
    assert items[0].id == "a"

    # After processing 'a', should still have more jobs (task 'c' exists but is blocked)
    # This is the key behavior: has_more_jobs returns True even though this worker
    # cannot make progress because 'c' depends on 'b' which requires different labels
    assert main_worker.has_more_jobs is True

    # Trying to iterate again should yield nothing since 'c' is blocked by 'b'
    items_second = list(main_worker)
    assert len(items_second) == 0

    # Should still report more jobs available (task 'c' is incomplete)
    assert main_worker.has_more_jobs is True

    # Now create a worker that can process the 'special-worker' task 'b'
    special_worker = Graphband(
        blocked_dependency_graph_task(),
        db=f"sqlite:///{tmp_path}/graphband.sqlite",
        lock=Lock(f"{tmp_path}/graphband.lock"),
        labels={"special-worker"},
        identifier="special-worker",
    )

    # Special worker should have jobs (task 'b')
    assert special_worker.has_more_jobs is True

    # Process task 'b'
    items_special = list(special_worker)
    assert len(items_special) == 1
    assert items_special[0].id == "b"

    # After 'b' is complete, special worker should have no more jobs
    assert special_worker.has_more_jobs is False

    # Now main worker should be able to process task 'c'
    assert main_worker.has_more_jobs is True

    items_final = list(main_worker)
    assert len(items_final) == 1
    assert items_final[0].id == "c"

    # Finally, no more jobs for either worker
    assert main_worker.has_more_jobs is False
    assert special_worker.has_more_jobs is False


def test_has_more_jobs_with_killed_workers(tmp_path):
    """Test has_more_jobs behavior when workers are killed
    and tasks exceed retry limits."""
    # Test case where killed tasks cannot be retried (max_killed_retries=0)
    lock_path = f"{tmp_path}/graphband.lock"
    db = f"sqlite:///{tmp_path}/graphband.sqlite"
    file = f"{tmp_path}/output.txt"

    # Start a worker that will be killed with no retry allowance
    proc = multiprocessing.Process(
        target=task_worker,
        args=(sequential_task, lock_path, db, file, 3),
        kwargs={
            "heartbeat_timeout": 2,
            "heartbeat_interval": 1,
            "max_killed_retries": 0,  # No retries allowed for killed tasks
            "identifier": "killed-worker",
        },
    )
    proc.start()
    time.sleep(2)  # Let the worker start one task
    proc.kill()
    proc.join()

    time.sleep(2)
    # need to start another worker to mark the job as killed in the db
    _ = Graphband(
        sequential_task(),
        db=db,
        lock=Lock(lock_path),
        heartbeat_timeout=2,
        heartbeat_interval=1,
        identifier="update-worker",
    )
    time.sleep(2)

    # Verify the killed task is permanently blocked
    engine = create_engine(db)
    with Session(engine) as session:
        tasks = session.query(TaskEntry).all()
        # Verify we have exactly one killed task and 9 completed tasks
        killed_tasks = [
            t for t in tasks if t.current_status.status == TaskStatusEnum.KILLED
        ]
        assert len(killed_tasks) == 1

    no_retries_worker = Graphband(
        sequential_task(),
        db=db,
        lock=Lock(lock_path),
        heartbeat_timeout=2,
        heartbeat_interval=1,
        max_killed_retries=0,  # No retries allowed
        identifier="no-retries-worker",
    )

    retries_worker = Graphband(
        sequential_task(),
        db=db,
        lock=Lock(lock_path),
        heartbeat_timeout=2,
        heartbeat_interval=1,
        max_killed_retries=2,  # Allow retries
        identifier="retries-worker",
    )

    assert no_retries_worker.has_more_jobs is True
    assert retries_worker.has_more_jobs is True
    assert len(list(no_retries_worker)) == 9
    assert no_retries_worker.has_more_jobs is False
    assert retries_worker.has_more_jobs is True
    assert len(list(retries_worker)) == 1
    assert retries_worker.has_more_jobs is False


@pytest.mark.human_reviewed
def test_resume_worker(tmp_path):
    lock_path = f"{tmp_path}/graphband.lock"
    db_path = f"sqlite:///{tmp_path}/graphband.sqlite"

    worker = Graphband(
        sequential_task(),
        db=db_path,
        lock=Lock(lock_path),
        heartbeat_timeout=2,
        heartbeat_interval=1,
        identifier="worker",
    )
    engine = create_engine(worker.db)
    length = 0

    with Session(engine) as session:
        worker_entry = session.get(WorkerEntry, "worker")
        assert worker_entry is not None
        assert worker_entry.status == WorkerStatus.IDLE

    with worker:
        for item in worker:
            with Session(engine) as session:
                worker_entry = session.get(WorkerEntry, "worker")
                assert worker_entry is not None
                assert worker_entry.status == WorkerStatus.BUSY
            length += 1
            if item.id == "task_5":
                break
        assert length == 6

        with Session(engine) as session:
            worker_entry = session.get(WorkerEntry, "worker")
            assert worker_entry is not None
            assert worker_entry.status == WorkerStatus.IDLE

        for item in worker:
            with Session(engine) as session:
                worker_entry = session.get(WorkerEntry, "worker")
                assert worker_entry is not None
                assert worker_entry.status == WorkerStatus.BUSY
            length += 1

        assert length == 10

    # leaving the context should set the worker to offline

    with Session(engine) as session:
        worker_entry = session.get(WorkerEntry, "worker")
        assert worker_entry is not None
        assert worker_entry.status == WorkerStatus.OFFLINE

    del worker

    with Session(engine) as session:
        worker_entry = session.get(WorkerEntry, "worker")
        assert worker_entry is not None
        assert worker_entry.status == WorkerStatus.OFFLINE

    proc = multiprocessing.Process(
        target=task_worker,
        args=(sequential_task, lock_path, db_path, tmp_path / "test.txt", 0.1),
        kwargs={
            "heartbeat_timeout": 2,
            "heartbeat_interval": 1,
            "max_killed_retries": 0,  # No retries allowed for killed tasks
            "identifier": "killed-worker",
        },
    )
    proc.start()
    proc.join(timeout=5)

    with Session(engine) as session:
        worker_entry = session.get(WorkerEntry, "killed-worker")
        assert worker_entry is not None
        assert worker_entry.status == WorkerStatus.OFFLINE


def test_context_manager_protocol(tmp_path):
    """Test context manager protocol sets worker to offline on exit."""
    lock_path = tmp_path / "test.lock"
    db_path = tmp_path / "test.sqlite"
    lock = Lock(str(lock_path))
    engine = create_engine(f"sqlite:///{db_path}", echo=False)

    # Test context manager usage
    with Graphband(
        sequential_task(),
        lock=lock,
        db=f"sqlite:///{db_path}",
        identifier="context-worker",
    ) as worker:
        # Worker should be idle initially
        with Session(engine) as session:
            worker_entry = session.get(WorkerEntry, "context-worker")
            assert worker_entry is not None
            assert worker_entry.status == WorkerStatus.IDLE

        # Process one task - worker should be busy during processing
        tasks_processed = 0
        for task in worker:
            with Session(engine) as session:
                worker_entry = session.get(WorkerEntry, "context-worker")
                assert worker_entry is not None
                assert worker_entry.status == WorkerStatus.BUSY
            tasks_processed += 1
            if tasks_processed >= 3:
                break

        # After processing, worker should be idle (inside context)
        with Session(engine) as session:
            worker_entry = session.get(WorkerEntry, "context-worker")
            assert worker_entry is not None
            assert worker_entry.status == WorkerStatus.IDLE

    # After exiting context, worker should be offline
    with Session(engine) as session:
        worker_entry = session.get(WorkerEntry, "context-worker")
        assert worker_entry is not None
        assert worker_entry.status == WorkerStatus.OFFLINE


def test_non_context_manager_sets_offline(tmp_path):
    """Test non-context manager usage sets worker to offline after iteration."""
    lock_path = tmp_path / "test.lock"
    db_path = tmp_path / "test.sqlite"
    lock = Lock(str(lock_path))
    engine = create_engine(f"sqlite:///{db_path}", echo=False)

    worker = Graphband(
        sequential_task(),
        lock=lock,
        db=f"sqlite:///{db_path}",
        identifier="non-context-worker",
    )

    # Process all tasks
    tasks_processed = list(worker)
    assert len(tasks_processed) == 10

    # Worker should be offline after completing all tasks (non-context usage)
    with Session(engine) as session:
        worker_entry = session.get(WorkerEntry, "non-context-worker")
        assert worker_entry is not None
        assert worker_entry.status == WorkerStatus.OFFLINE


def test_worker_reuse_from_offline(tmp_path):
    """Test reusing a worker that was previously offline."""
    lock_path = tmp_path / "test.lock"
    db_path = tmp_path / "test.sqlite"
    lock = Lock(str(lock_path))
    engine = create_engine(f"sqlite:///{db_path}", echo=False)

    # First worker - use context manager to set offline
    with Graphband(
        sequential_task(),
        lock=lock,
        db=f"sqlite:///{db_path}",
        identifier="reuse-worker",
    ) as worker1:
        tasks_processed = list(worker1)
        assert len(tasks_processed) == 10

    # Verify worker is offline
    with Session(engine) as session:
        worker_entry = session.get(WorkerEntry, "reuse-worker")
        assert worker_entry is not None
        assert worker_entry.status == WorkerStatus.OFFLINE

    # Second worker - should reuse the offline worker
    with Graphband(
        sequential_task(),
        lock=lock,
        db=f"sqlite:///{db_path}",
        identifier="reuse-worker",  # Same identifier
    ):
        # Worker should be reset to idle when reused
        with Session(engine) as session:
            worker_entry = session.get(WorkerEntry, "reuse-worker")
            assert worker_entry is not None
            assert worker_entry.status == WorkerStatus.IDLE

    # After second context, worker should be offline again
    with Session(engine) as session:
        worker_entry = session.get(WorkerEntry, "reuse-worker")
        assert worker_entry is not None
        assert worker_entry.status == WorkerStatus.OFFLINE


def test_worker_reuse_from_non_offline_fails(tmp_path):
    """Test that attempting to reuse a non-offline worker raises ValueError."""
    lock_path = tmp_path / "test.lock"
    db_path = tmp_path / "test.sqlite"
    lock = Lock(str(lock_path))

    # First worker - don't use context manager so it stays idle
    worker1 = Graphband(
        sequential_task(),
        lock=lock,
        db=f"sqlite:///{db_path}",
        identifier="busy-worker",
    )

    with worker1:
        # Process one task but don't complete iteration
        for _ in worker1:
            break  # This leaves worker in idle state

        # Attempting to create another worker with same identifier should fail
        with pytest.raises(ValueError, match="already exists with status"):
            Graphband(
                sequential_task(),
                lock=lock,
                db=f"sqlite:///{db_path}",
                identifier="busy-worker",  # Same identifier
            )
