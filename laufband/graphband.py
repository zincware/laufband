import hashlib
import os
import typing as t
from collections.abc import Callable, Generator, Iterator
from contextlib import nullcontext
from pathlib import Path

from flufl.lock import Lock
from tqdm import tqdm

from laufband.db import GraphbandDB

_T = t.TypeVar("_T", covariant=True)


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
    ...     yield (node, set(g.predecessors(node)))
    """

    def __iter__(self) -> Iterator[tuple[str, set[str]]]: ...


def _check_disabled(func: t.Callable) -> t.Callable:
    """Decorator to raise an error if Graphband is disabled."""

    def wrapper(self, *args, **kwargs):
        if self.disabled:
            raise RuntimeError("Graphband is disabled. Cannot call this method.")
        return func(self, *args, **kwargs)

    return wrapper


def _default_hash_fn(item: t.Any) -> str:
    """Default hash function for tasks (deterministic across processes)."""
    return hashlib.sha256(str(item).encode()).hexdigest()


class Graphband(t.Generic[_T]):
    def __init__(
        self,
        graph_fn: GraphTraversalProtocol,
        *,
        hash_fn: Callable[[_T], str] | None = None,
        lock: Lock | None = None,
        lock_path: Path | str | None = None,
        com: Path | str | None = None,
        identifier: str | t.Callable | None = None,
        cleanup: bool = False,
        failure_policy: t.Literal["continue", "stop"] = "continue",
        heartbeat_timeout: int | None = None,
        max_died_retries: int | None = None,
        disable: bool | None = None,
        tqdm_kwargs: dict[str, t.Any] | None = None,
    ):
        """Graphband generator for parallel processing of DAGs using file-based locking.

        Arguments
        ---------
        graph_fn : GraphTraversalProtocol
            Object that implements the GraphTraversalProtocol, yielding tuples of
            (node, predecessors) for graph traversal. Nodes are tasks and must be
            hashable.

        hash_fn : Callable[[Any], str] | None
            Function to compute task IDs from graph nodes. If None, uses a default
            hash function. For UUID-mapped items, this should operate on the UUID keys.
        lock : Lock | None
            A lock object to ensure thread safety. If None, a new lock will be created.
        lock_path : Path | str | None
            The path to the lock file used for synchronization.
            Defaults to "graphband.lock".
        com : Path | str | None
            The path to the db file used to store the state.
            If given, the file will not be removed.
            If not provided, a file named "graphband.sqlite" will be
            used and removed after completion.
        identifier : str | callable, optional
            A unique identifier for the worker. If not set, the process ID will be used.
            If a callable is provided, it will be called to generate the identifier.
            Must be unique across all workers. Can be set via the environment variable
            ``LAUFBAND_IDENTIFIER``.
        cleanup : bool
            If True, the database file will be removed after processing is complete.
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
        disable : bool
            If True, disable Graphband features and return a simple iterator.
            Can also be set via the environment variable
            ``LAUFBAND_DISABLE``.
        tqdm_kwargs : dict
            Additional arguments to pass to tqdm.
        """
        if heartbeat_timeout is None:
            heartbeat_timeout = int(os.getenv("LAUFBAND_HEARTBEAT_TIMEOUT", 60 * 60))
        if max_died_retries is None:
            max_died_retries = int(os.getenv("LAUFBAND_MAX_DIED_RETRIES", 0))
        if identifier is None:
            identifier = os.getenv("LAUFBAND_IDENTIFIER", str(os.getpid()))
        if lock_path is not None and lock is not None:
            raise ValueError(
                "You cannot set both `lock` and `lock_path`. Use one or the other."
            )
        if disable is None:
            self.disabled = os.getenv("LAUFBAND_DISABLE", "0") == "1"
        else:
            self.disabled = disable

        if lock_path is None:
            lock_path = "graphband.lock"

        self.graph_fn = graph_fn
        self.hash_fn = hash_fn or _default_hash_fn

        if self.disabled:
            self.lock = nullcontext()
        else:
            self.lock = lock if lock is not None else Lock(Path(lock_path).as_posix())
        self.com = Path(com or "graphband.sqlite")

        if callable(identifier):
            self.db = GraphbandDB(
                self.com,
                worker=identifier(),
                heartbeat_timeout=heartbeat_timeout,
                max_died_retries=max_died_retries,
            )
        else:
            self.db = GraphbandDB(
                self.com,
                worker=identifier,
                heartbeat_timeout=heartbeat_timeout,
                max_died_retries=max_died_retries,
            )
        self.cleanup = cleanup
        self.tqdm_kwargs = tqdm_kwargs or {}

        self._close_trigger = False
        self.failure_policy = failure_policy

    def close(self):
        """Exit out of the graphband generator.

        If you use ``break`` inside a graphband loop,
        it will be registered as a failed job.
        Instead, you can use this function to exit
        the graphband generator marking the job as completed.
        """
        self._close_trigger = True

    @property
    def identifier(self) -> str:
        """Unique identifier of this worker"""
        return self.db.worker

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
    def pending(self) -> list[str]:
        """Return the task IDs that are pending processing."""
        with self.lock:
            return self.db.list_state("pending")

    @property
    @_check_disabled
    def died(self) -> list[str]:
        """Return the task IDs that have been marked as 'died'."""
        with self.lock:
            return self.db.list_state("died")

    def __iter__(self) -> Generator[_T, None, None]:
        """The generator that handles the iteration logic."""
        if self.disabled:
            # For disabled mode, iterate over the graph protocol
            yield from tqdm((node for node, _ in self.graph_fn), **self.tqdm_kwargs)
            return

        # Store node mapping and graph structure during database initialization to avoid re-consuming generator
        node_mapping = {}
        graph_data = []
        
        with self.lock:
            # Initialize database with the graph protocol
            if not self.com.exists():
                # Consume the generator once and store all data
                for node, predecessors in self.graph_fn:
                    task_id = self.hash_fn(node)
                    node_mapping[task_id] = node
                    graph_data.append((node, predecessors))
                
                # Create a new generator for database creation that uses our stored data
                def stored_graph():
                    for node, predecessors in graph_data:
                        yield (node, predecessors)
                
                self.db.create_from_graph(stored_graph(), self.hash_fn)
            else:
                # For update case, consume generator and store nodes
                for node, predecessors in self.graph_fn:
                    task_id = self.hash_fn(node)
                    node_mapping[task_id] = node
                    graph_data.append((node, predecessors))
                
                def stored_graph():
                    for node, predecessors in graph_data:
                        yield (node, predecessors)
                
                self.db.update_from_graph(stored_graph(), self.hash_fn)

        # Get initial task count for progress bar
        # Check if the protocol supports __len__ for efficient counting
        with self.lock:
            try:
                total_tasks = len(self.graph_fn)
            except TypeError:
                # Protocol doesn't support __len__, use database count or node_mapping count
                total_tasks = len(node_mapping) if node_mapping else len(self.db)

        tbar = tqdm(total=total_tasks, **self.tqdm_kwargs)

        while True:
            with self.lock:
                completed = self.db.list_state("completed")

                if self.failure_policy == "stop" and self.db.list_state("failed"):
                    raise RuntimeError(
                        "Another worker has failed. "
                        "Stopping due to failure_policy='stop'."
                    )

                # Get ready task using dependency-aware logic with stored graph data
                task_id = None
                
                # Get all completed tasks
                completed_task_ids = set(self.db.list_state("completed"))
                
                # Find a ready task from our stored graph data
                for node, predecessors in graph_data:
                    candidate_task_id = self.hash_fn(node)
                    predecessor_task_ids = {self.hash_fn(pred) for pred in predecessors}
                    
                    # Check if this task is ready (no predecessors or all predecessors completed)
                    if not predecessors or predecessor_task_ids.issubset(completed_task_ids):
                        # Try to claim this task
                        with self.db.connect() as conn:
                            cursor = conn.cursor()
                            cursor.execute(
                                """
                                UPDATE progress_table
                                SET state = 'running', worker = ?, count = count + 1
                                WHERE task_id = ? AND
                                (
                                    state = 'pending'
                                    OR
                                    (state = 'died' AND count - 1 < ?)
                                )
                                RETURNING task_id
                                """,
                                (self.db.worker, candidate_task_id, self.db.max_died_retries),
                            )
                            row = cursor.fetchone()
                            if row:
                                conn.commit()
                                task_id = row[0]
                                break
                
                if task_id is None:
                    # No more ready tasks
                    break

            # Update progress bar for completed items
            tbar.n = len(completed)
            tbar.refresh()

            # Get the actual task object from our stored mapping
            task_item = node_mapping.get(task_id)
            if task_item is None:
                # This shouldn't happen, but handle gracefully
                with self.lock:
                    self.db.finalize(task_id, "failed")
                continue

            try:
                yield task_item
            except GeneratorExit:
                with self.lock:
                    self.db.finalize(task_id, "failed")
                raise

            tbar.update(1)

            with self.lock:
                # After processing, mark as completed
                self.db.finalize(task_id, "completed")
                completed = self.db.list_state("completed")
                total_tasks = len(self.db)
                if len(completed) == total_tasks:
                    if self.cleanup:
                        self.com.unlink()
                    return

            if self._close_trigger:
                self._close_trigger = False
                break
