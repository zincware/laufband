import os
import typing as t
from collections.abc import Generator, Sequence
from contextlib import nullcontext
from pathlib import Path

from flufl.lock import Lock
from tqdm import tqdm

from laufband.db import LaufbandDB

_T = t.TypeVar("_T", covariant=True)


def _check_disabled(func: t.Callable) -> t.Callable:
    """Decorator to raise an error if Laufband is disabled."""

    def wrapper(self, *args, **kwargs):
        if self.disabled:
            raise RuntimeError("Laufband is disabled. Cannot call this method.")
        return func(self, *args, **kwargs)

    return wrapper


class Laufband(t.Generic[_T]):
    def __init__(
        self,
        data: Sequence[_T],
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
            while with the "raise" policy, the other process will stop
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
            lock_path = "laufband.lock"

        self.data = data
        if self.disabled:
            self.lock = nullcontext()
        else:
            self.lock = lock if lock is not None else Lock(Path(lock_path).as_posix())
        self.com = Path(com or "laufband.sqlite")

        if callable(identifier):
            self.db = LaufbandDB(
                self.com,
                worker=identifier(),
                heartbeat_timeout=heartbeat_timeout,
                max_died_retries=max_died_retries,
            )
        else:
            self.db = LaufbandDB(
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
        """Exit out of the laufband generator.

        If you use ``break`` inside a laufband loop,
        it will be registered as a failed job.
        Instead, you can use this function to exit
        the laufband generator marking the job as completed.
        """
        self._close_trigger = True

    @property
    @_check_disabled
    def completed(self) -> list[int]:
        """Return the indices of items that have been completed."""
        with self.lock:
            return self.db.list_state("completed")

    @property
    @_check_disabled
    def failed(self) -> list[int]:
        """Return the indices of items that have failed processing."""
        with self.lock:
            return self.db.list_state("failed")

    @property
    @_check_disabled
    def running(self) -> list[int]:
        """Return the indices of items that are currently being processed."""
        with self.lock:
            return self.db.list_state("running")

    @property
    @_check_disabled
    def pending(self) -> list[int]:
        """Return the indices of items that are pending processing."""
        with self.lock:
            return self.db.list_state("pending")

    @property
    @_check_disabled
    def died(self) -> list[int]:
        """Return the indices of items that have been marked as 'died'."""
        with self.lock:
            return self.db.list_state("died")

    @property
    def identifier(self) -> str:
        """Return the identifier of the worker."""
        return self.db.worker

    def __len__(self) -> int:
        """Return the length of the data."""
        with self.lock:
            return len(self.data)

    def __iter__(self) -> Generator[_T, None, None]:
        """The generator that handles the iteration logic."""
        if self.disabled:
            # we create the database anyhow, e.g. no issues with DVC outs
            size = len(self.data)
            if not self.com.exists():
                self.db.create(size)

            # If Laufband is disabled, yield from a simple tqdm iterator
            yield from tqdm(self.data, **self.tqdm_kwargs)
            return

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
        tbar = tqdm(total=size, **self.tqdm_kwargs)
        while True:
            with self.lock:
                completed = self.db.list_state("completed")

                if self.failure_policy == "stop" and self.db.list_state("failed"):
                    raise RuntimeError(
                        "Another worker has failed. "
                        "Stopping due to failure_policy='stop'."
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
