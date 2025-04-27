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
        identifier: str | t.Callable | None = os.getpid,
        cleanup: bool = False,
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
        cleanup : bool
            If True, the database file will be removed after processing is complete.
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
            self.db = LaufbandDB(self.com, worker=identifier())
        else:
            self.db = LaufbandDB(self.com, worker=identifier)
        self.cleanup = cleanup
        self.kwargs = kwargs

        self._close_trigger = False

    def close(self):
        """Exit out of the laufband generator.

        If you use ``break`` inside a laufband loop,
        it will be registered as a failed job.
        Instead, you can use this function to exit
        the laufband generator marking the job as completed.
        """
        self._close_trigger = True

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

        tbar = tqdm(total=size, **self.kwargs)
        while True:
            with self.lock:
                completed = self.db.list_state("completed")
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
