import typing as t
import uuid
from collections.abc import Generator, Iterable
from pathlib import Path

from flufl.lock import Lock

from laufband.graphband import Graphband

_T = t.TypeVar("_T", covariant=True)


class Laufband(t.Generic[_T]):
    def __init__(
        self,
        data: Iterable[_T],
        *,
        lock: Lock | None = None,
        lock_path: Path | str | None = None,
        com: Path | str | None = None,
        identifier: str | t.Callable | None = None,
        cleanup: bool = False,
        failure_policy: t.Literal["continue", "stop"] = "continue",
        heartbeat_timeout: int | None = None,
        max_died_retries: int | None = None,
        disable: bool | None = None,
        hash_fn: t.Callable[[t.Any], str] | None = None,
        tqdm_kwargs: dict[str, t.Any] | None = None,
    ):
        """Laufband generator for parallel processing using file-based locking.

        Arguments
        ---------
        data : Sequence
            The data to process. Any object implementing ``__len__`` and ``__getitem__``
            if supported.
        lock : Lock | None
            A lock object to ensure thread safety. If None, a new lock will be created.
        lock_path : Path | str
            The path to the lock file used for synchronization.
            Defaults to "laufband.lock".
        com : Path | str | None
            The path to the db file used to store the state.
            If given, the file will not be removed.
            If not provided, a file named "laufband.sqlite" will be
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
            while with the "stop" policy, the other process will stop
            and raise an exception that one process failed.
        heartbeat_timeout : int
            The timeout in seconds to consider a worker as dead if it has not been seen
            in the last `heartbeat_timeout` seconds. This is used to mark jobs
            as "died" if the worker process is killed unexpectedly. Set to a value
            greater than what you expect the runtime of the longest iteration to be.
            Defaults to 1 hour or the value of the environment variable
            ``LAUFBAND_HEARTBEAT_TIMEOUT`` if set.
        max_died_retries : int
            The number of times to retry processing items that have been marked as died.
            If set to 0, no retries will be attempted.
            Defaults to 0 or the value of the environment variable
            ``LAUFBAND_MAX_DIED_RETRIES`` if set.
        disable : bool
            If True, disable Laufband features and return a tqdm iterator.
            Can also be set via the environment variable
            ``LAUFBAND_DISABLE``.
        tqdm_kwargs : dict
            Additional arguments to pass to tqdm.

        Example
        -------
        >>> import json
        >>> import time
        >>> from pathlib import Path
        >>> from laufband import Laufband
        ...
        >>> output_file = Path("data.json")
        >>> output_file.write_text(json.dumps({"processed_data": []}))
        >>> data = list(range(100))
        >>> worker = Laufband(data, lock=lock, desc="using Laufband")
        ...
        >>> for item in worker:
        ...    # Simulate some computationally intensive task
        ...    time.sleep(0.1)
        ...    with worker.lock:
        ...        # Access and modify a shared resource (e.g., a file)
        ...        # safely using the lock
        ...        file_content = json.loads(output_file.read_text())
        ...        file_content["processed_data"].append(item)
        ...        output_file.write_text(json.dumps(file_content))

        """
        self.data = data

        # Store the data source for lazy evaluation
        self._data_source = data
        self._item_mapping = {}
        self._mapping_created = False

        # Create a graph_fn that implements GraphTraversalProtocol with lazy evaluation
        class LazyGraphProtocol:
            def __init__(self, parent):
                self.parent = parent

            def __len__(self):
                return len(self.parent._data_source)

            def __iter__(self):
                # Lazy evaluation - create mapping only when iteration is requested
                if not self.parent._mapping_created:
                    for item in self.parent._data_source:
                        item_uuid = str(uuid.uuid4())
                        self.parent._item_mapping[item_uuid] = item
                    self.parent._mapping_created = True

                # Yield nodes with no predecessors (disconnected graph)
                for item_uuid in self.parent._item_mapping.keys():
                    yield (item_uuid, set())

        graph_fn = LazyGraphProtocol(self)

        # Use default Laufband hash function if none provided
        if hash_fn is None:

            def laufband_hash_fn(item_uuid):
                # For lazy evaluation, we can't rely on iterating over original data
                # Instead, use the UUID positions in the mapping
                original_item = self._item_mapping[item_uuid]
                uuid_list = list(self._item_mapping.keys())
                try:
                    index = uuid_list.index(item_uuid)
                    return str(index)
                except ValueError:
                    return str(id(original_item))  # Fallback if not found

            hash_fn = laufband_hash_fn

        # Fix default lock path for backwards compatibility
        if lock_path is None and lock is None:
            lock_path = "laufband.lock"

        # Create internal Graphband instance
        self._graphband = Graphband(
            graph_fn=graph_fn,
            hash_fn=hash_fn,
            lock=lock,
            lock_path=lock_path,
            com=com,
            identifier=identifier,
            cleanup=cleanup,
            failure_policy=failure_policy,
            heartbeat_timeout=heartbeat_timeout,
            max_died_retries=max_died_retries,
            disable=disable,
            tqdm_kwargs=tqdm_kwargs,
        )

    def close(self):
        """Exit out of the laufband generator.

        If you use ``break`` inside a laufband loop,
        it will be registered as a failed job.
        Instead, you can use this function to exit
        the laufband generator marking the job as completed.
        """
        self._graphband.close()

    @property
    def identifier(self) -> str:
        """Unique identifier of this worker"""
        return self._graphband.identifier

    @property
    def lock(self):
        """Access to the underlying lock"""
        return self._graphband.lock

    @property
    def disabled(self) -> bool:
        """Whether Laufband is disabled"""
        return self._graphband.disabled

    @property
    def com(self):
        """Path to the communication/database file"""
        return self._graphband.com

    @property
    def completed(self) -> list[int]:
        """Return the indices of items that have been completed."""
        # Convert string task IDs back to integers for backwards compatibility
        task_ids = self._graphband.completed
        return [int(task_id) for task_id in task_ids if task_id.isdigit()]

    @property
    def failed(self) -> list[int]:
        """Return the indices of items that have failed processing."""
        task_ids = self._graphband.failed
        return [int(task_id) for task_id in task_ids if task_id.isdigit()]

    @property
    def running(self) -> list[int]:
        """Return the indices of items that are currently being processed."""
        task_ids = self._graphband.running
        return [int(task_id) for task_id in task_ids if task_id.isdigit()]

    @property
    def pending(self) -> list[int]:
        """Return the indices of items that are pending processing."""
        task_ids = self._graphband.pending
        return [int(task_id) for task_id in task_ids if task_id.isdigit()]

    @property
    def died(self) -> list[int]:
        """Return the indices of items that have been marked as 'died'."""
        task_ids = self._graphband.died
        return [int(task_id) for task_id in task_ids if task_id.isdigit()]

    def __len__(self) -> int:
        """Return the length of the data."""
        return len(self.data)

    def __iter__(self) -> Generator[_T, None, None]:
        """The generator that handles the iteration logic."""
        for item_uuid in self._graphband:
            # Convert UUID back to original item
            yield self._item_mapping[item_uuid]
