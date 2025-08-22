import hashlib
import os
import threading
import typing as t
from collections.abc import Callable, Generator, Iterator
from contextlib import nullcontext
from pathlib import Path
import socket

from flufl.lock import Lock
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from tqdm import tqdm

from laufband.db import (
    Base,
    WorkerEntry,
    WorkerStatus,
    TaskEntry,
    TaskStatusEnum,
    TaskStatusEntry,
)
from laufband.hearbeat import heartbeat
from laufband.task import Task


import threading
from contextlib import ExitStack

_T = t.TypeVar("_T", covariant=True)

# Issue, if we have a generator like ase.io.iread we don't want to fully iterate it at each __next__
# If we have a dynamik graph, we need to iterate it at each __next__
# if we have different labels, we need to exhaust either the full graph or up to the next N entries to check for jobs.
# if we want to be able to pass a generator to laufband, we don't have a generator factory.
# possibly the best solution would be, iterate the generator and define how many iterations in the future should be checked for labels.
#   If set to "all" then it will iterate the entire graph and check / cache the jobs.
# Given we define a worker timeout, until timeout it should recheck all items previously unavailable, otherwise just end.
# Need to define a recheck-interval.
# dynamik graph building is realized by restarting the process.
# for dynamik graph building, if an entry allready exists but has received a new dependency, for now raise an error.



class MultiLock:
    """Context manager that acquires multiple locks at once."""

    def __init__(self, *locks: t.Union[Lock, threading.Lock, nullcontext]):
        self._locks = locks
        self._stack = ExitStack()

    def __enter__(self):
        for lock in self._locks:
            self._stack.enter_context(lock)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # ExitStack handles releasing in reverse order
        return self._stack.__exit__(exc_type, exc_val, exc_tb)


class GraphTraversalProtocol(t.Protocol):
    """Protocol for iterating over a graph's nodes and their predecessors.

    Yields
    -------
    Iterator[tuple[str, set[str]]]
        An iterator over the graph's nodes and their predecessors.

    Example
    -------
    >>> g = nx.DiGraph()
    >>> g.add_edges_from([("A", "B"), ("A", "C"), ("B", "D"), ("C", "D")])
    >>> for node in nx.topological_sort(g):
    ...     yield Task(id=node, dependencies=set(g.predecessors(node)))
    """

    def __iter__(self) -> Iterator[Task]: ...


class SizedGraphTraversalProtocol(GraphTraversalProtocol):
    """
    Protocol for graph traversal that also supports __len__.

    Extends
    -------
    GraphTraversalProtocol

    Methods
    -------
    __len__() -> int
        Return the number of nodes in the graph.

    Example
    -------
    >>> class MyGraph(SizedGraphTraversalProtocol):
    ...     def __iter__(self):
    ...         yield from [
    ...             Task(id="A", dependencies=set()),
    ...             Task(id="B", dependencies={"A"})
    ...         ]
    ...     def __len__(self):
    ...         return 2
    """

    def __len__(self) -> int: ...


def _check_disabled(func: t.Callable) -> t.Callable:
    """Decorator to raise an error if Graphband is disabled."""

    def wrapper(self, *args, **kwargs):
        if self.disabled:
            raise RuntimeError("Graphband is disabled. Cannot call this method.")
        return func(self, *args, **kwargs)

    return wrapper


# def _default_hash_fn(item: t.Any) -> str:
#     """Default hash function for tasks (deterministic across processes)."""
#     return hashlib.sha256(str(item).encode()).hexdigest()


class Graphband(t.Generic[_T]):
    def __init__(
        self,
        graph_fn: GraphTraversalProtocol | SizedGraphTraversalProtocol,
        *,
        lock: Lock = Lock(Path("graphband.lock").as_posix()),
        db: str = "sqlite:///graphband.sqlite",
        identifier: str | t.Callable = os.getpid,
        failure_policy: t.Literal["continue", "stop"] = os.getenv(
            "LAUFBAND_FAILURE_POLICY", "continue"
        ),
        heartbeat_timeout: int = int(os.getenv("LAUFBAND_HEARTBEAT_TIMEOUT", 60 * 60)),
        max_died_retries: int = int(os.getenv("LAUFBAND_MAX_DIED_RETRIES", 0)),
        disabled: bool = bool(int(os.getenv("LAUFBAND_DISABLED", "0"))),
        tqdm_kwargs: dict[str, t.Any] | None = None,
    ):
        """Graphband generator for parallel processing of DAGs using file-based locking.

        Arguments
        ---------
        graph_fn : GraphTraversalProtocol
        lock : Lock
            A lock object to ensure thread safety.
        db : str
            The database connection string. Defaults to "sqlite:///graphband.sqlite".
        identifier : str | callable, optional
            A unique identifier for the worker. If not set, the process ID will be used.
            If a callable is provided, it will be called to generate the identifier.
            Must be unique across all workers. Can be set via the environment variable
            ``LAUFBAND_IDENTIFIER``.
        failure_policy : str
            If an error occurs, the generator will always yield that error.
            With the "continue" policy, other processes will continue,
            while with the "stop" policy, other processes will stop
            and raise an exception that one process failed.
        heartbeat_timeout : int
            The timeout in seconds to consider a worker as dead if it has not been seen
            in the last `heartbeat_timeout` seconds. This is used to mark jobs
            as "died" if the worker process is killed unexpectedly.
            Defaults to 1 hour or the value of the environment variable
            ``LAUFBAND_HEARTBEAT_TIMEOUT`` if set.
        max_died_retries : int
            The number of times to retry processing items that have been marked as died.
            If set to 0, no retries will be attempted.
            Defaults to 0 or the value of the environment variable
            ``LAUFBAND_MAX_DIED_RETRIES`` if set.
        disabled : bool
            If True, disable Graphband features and return a simple iterator.
            Can also be set via the environment variable
            ``LAUFBAND_DISABLE``.
        tqdm_kwargs : dict
            Additional arguments to pass to tqdm.
        """
        self.disabled = disabled
        self.graph_fn: GraphTraversalProtocol = graph_fn
        self._lock = lock if not disabled else nullcontext()
        self._close_trigger = False
        self.failure_policy = failure_policy
        self.tqdm_kwargs = tqdm_kwargs or {}
        self._identifier = identifier() if callable(identifier) else identifier
        self._db = db

        if not self.disabled:
            # we need to lock between threads and workers,
            # because the worker and heartbeat will share the same pid.
            # thread-lock MUST be the first lock to be passed!
            self._thread_lock = threading.Lock()
            self.lock = MultiLock(self._thread_lock, self._lock)
            self._engine = create_engine(self._db, echo=False)
            self._register_worker()
            self._thread_event = threading.Event()
            self._heartbeat_thread = threading.Thread(
                target=heartbeat,
                args=(self.lock, self._db, self._identifier, self._thread_event),
                daemon=True,
            )
            self._heartbeat_thread.start()
        else:
            self.lock = MultiLock(self._lock)

    def _register_worker(self):
        """Register the worker with the database."""
        with self.lock:
            Base.metadata.create_all(self._engine)
            with Session(self._engine) as session:
                worker_entry = WorkerEntry(
                    id=self._identifier, status=WorkerStatus.IDLE, hostname=socket.gethostname(), pid=os.getpid()
                )
                session.add(worker_entry)
                session.commit()

    def __del__(self):
        self._thread_event.set()

    def close(self):
        """Exit out of the graphband generator.

        If you use ``break`` inside a graphband loop,
        it will be registered as a failed job.
        Instead, you can use this function to exit
        the graphband generator marking the job as completed.
        """
        self._close_trigger = True

    def __len__(self) -> int:
        """Return the number of tasks in the graph."""
        with self.lock:
            return len(self.graph_fn)

    @property
    def identifier(self) -> str:
        """Unique identifier of this worker"""
        return self._identifier

    @property
    @_check_disabled
    def completed(self) -> list[str]:
        """Return the task IDs that have been completed."""
        with self.lock:
            return self.db.list_state("completed")

    @property
    @_check_disabled
    def failed(self) -> list[str]:
        """Return the task IDs that have failed processing."""
        with self.lock:
            return self.db.list_state("failed")

    @property
    @_check_disabled
    def running(self) -> list[str]:
        """Return the task IDs that are currently being processed."""
        with self.lock:
            return self.db.list_state("running")

    @property
    @_check_disabled
    def died(self) -> list[str]:
        """Return the task IDs that have been marked as 'died'."""
        with self.lock:
            return self.db.list_state("died")

    def __iter__(self) -> Generator[Task, None, None]:
        """The generator that handles the iteration logic."""
        iterator = tqdm((node for node in self.graph_fn), **self.tqdm_kwargs)
        if self.disabled:
            # If disabled, just iterate over the graph protocol
            yield from iterator
            return

        for task in iterator:
            with self.lock:
                with Session(self._engine) as session:
                    existing_task = session.get(TaskEntry, task.id)
                    if existing_task:
                        # TODO: handle stuff like retires here later.
                        continue
                    # create the task
                    worker = session.get(WorkerEntry, self._identifier)
                    if worker is None:
                        raise ValueError(
                            f"Worker with identifier {self._identifier} not found."
                        )
                    task_entry = TaskEntry(
                        id=task.id,
                        # dependencies=task.dependencies,
                        requirements=list(task.requirements),
                        max_parallel_workers=task.max_parallel_workers,
                    )
                    task_entry.workers.append(worker)
                    task_entry.statuses.append(TaskStatusEntry(
                        # task_id=task_entry.id,
                        status=TaskStatusEnum.RUNNING
                    ))
                    session.add(task_entry)
                    session.commit()
            yield task

            with Session(self._engine) as session:
                task_entry = session.get(TaskEntry, task.id)
                if task_entry:
                    task_entry.statuses.append(TaskStatusEntry(
                        # task_id=task_entry.id,
                        status=TaskStatusEnum.COMPLETED
                    ))
                    session.commit()
        self._thread_event.set()
        self._heartbeat_thread.join()
