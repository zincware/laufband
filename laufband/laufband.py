import os
import typing as t
from collections.abc import Generator, Sequence
from pathlib import Path

from flufl.lock import Lock
from tqdm import tqdm

from laufband.db import LaufbandDB

_T = t.TypeVar("_T", covariant=True)


class Laufband(t.Generic[_T]):
    def __init__(
        self,
        data: Sequence[_T],
        lock: Lock | None = None,
        com: Path | str | None = None,
        identifier: str | t.Callable = os.getpid,
        cleanup: bool = False,
        failure_policy: t.Literal["continue", "stop"] = "continue",
        heartbeat_timeout: int = 60,
        retry_died: int = 0,
        **kwargs,
    ):
        """Laufband generator for parallel processing using file-based locking.

        Arguments
        ---------
        data : Sequence
            The data to process. Any object implementing ``__len__`` and ``__getitem__``
            if supported.
        lock : Lock | None
            A lock object to ensure thread safety. If None, a new lock will be created.
        com : Path | str | None
            The path to the db file used to store the state. If given, the file will not be removed.
            If not provided, a file named "laufband.sqlite" will be used and removed after completion.
        identifier : str | callable, optional
            A unique identifier for the worker. If not set, the process ID will be used.
            If a callable is provided, it will be called to generate the identifier.
            Must be unique across all workers.
        cleanup : bool
            If True, the database file will be removed after processing is complete.
        failure_policy : str
            If an error occurs, the generator will always yield that error.
            With the "continue" policy, other processes will continue,
            while with the "raise" policy, the other process will stop
            and raise an exception that one process failed.
        heartbeat_timeout : int
            The timeout in seconds to consider a worker as dead if it has not been seen
            in the last `heartbeat_timeout` seconds. This is used to mark jobs as "died" if the
            worker process is killed unexpectedly. Set to a value greater than what you expect
            the runtime of the longest iteration to be.
        retry_died : int
            The number of times to retry processing items that have been marked as "died".
        kwargs : dict
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
        ...        # Access and modify a shared resource (e.g., a file) safely using the lock
        ...        file_content = json.loads(output_file.read_text())
        ...        file_content["processed_data"].append(item)
        ...        output_file.write_text(json.dumps(file_content))

        """
        self.data = data
        self.lock = lock if lock is not None else Lock("laufband.lock")
        self.com = Path(com or "laufband.sqlite")

        if callable(identifier):
            self.db = LaufbandDB(
                self.com,
                worker=identifier(),
                heartbeat_timeout=heartbeat_timeout,
                retry_died=retry_died,
            )
        elif identifier is None:
            raise ValueError(
                "Identifier must be a string or callable that returns a string."
            )
        else:
            self.db = LaufbandDB(
                self.com,
                worker=identifier,
                heartbeat_timeout=heartbeat_timeout,
                retry_died=retry_died,
            )

        self.cleanup = cleanup
        self.kwargs = kwargs

        self._close_trigger = False
        self.failure_policy = failure_policy

    def close(self):
        """Exit out of the laufband generator.

        If you use ``break`` inside a laufband loop,
        it will be registered as a failed job.
        Instead, you can use this function to exit
        the laufband generator marking the job as completed.
        """
        self._close_trigger = True

    @property
    def completed(self) -> list[int]:
        """Return the indices of items that have been completed."""
        with self.lock:
            return self.db.list_state("completed")

    @property
    def failed(self) -> list[int]:
        """Return the indices of items that have failed processing."""
        with self.lock:
            return self.db.list_state("failed")

    @property
    def running(self) -> list[int]:
        """Return the indices of items that are currently being processed."""
        with self.lock:
            return self.db.list_state("running")

    @property
    def pending(self) -> list[int]:
        """Return the indices of items that are pending processing."""
        with self.lock:
            return self.db.list_state("pending")

    def __len__(self) -> int:
        """Return the length of the data."""
        with self.lock:
            return len(self.data)

    def __iter__(self) -> Generator[_T, None, None]:
        """The generator that handles the iteration logic."""
        with self.lock:
            size = len(self.data)
            if not self.com.exists():
                self.db.create(size)
            else:
                if len(self.data) != len(self.db):
                    raise ValueError(
                        "The size of the data does not match the size of the database."
                    )
        # adapt tqdm to properly work with died jobs
        tbar = tqdm(total=size, **self.kwargs)
        while True:
            with self.lock:
                completed = self.db.list_state("completed")

                if self.failure_policy == "stop" and self.db.list_state("failed"):
                    raise RuntimeError(
                        "Another worker has failed. Stopping due to failure_policy='stop'."
                    )

                try:
                    idx = next(self.db)
                except StopIteration:
                    # No more items to process
                    break

            # Update progress bar for completed items
            tbar.n = len(completed)
            tbar.refresh()

            try:
                yield self.data[idx]
            except GeneratorExit:
                with self.lock:
                    self.db.finalize(idx, "failed")
                raise

            tbar.update(1)

            with self.lock:
                # After processing, mark as completed
                self.db.finalize(idx, "completed")
                completed = self.db.list_state("completed")
                if len(completed) == size:
                    if self.cleanup:
                        self.com.unlink()
                    return

            if self._close_trigger:
                self._close_trigger = False
                break
