import os
import typing as t
from collections.abc import Generator, Sequence
from pathlib import Path

from rich.console import Console
from rich.progress import Progress, BarColumn, TimeRemainingColumn, TextColumn


from flufl.lock import Lock
from tqdm import tqdm

from laufband.db import LaufbandDB


from rich.console import Console
from rich.progress import Progress, TextColumn, BarColumn, TimeRemainingColumn
from tqdm import tqdm
from typing import Generator, Optional, Any

_T = t.TypeVar("_T", covariant=True)


class _SmartProgress:
    def __init__(self, total: int, description: str = "Processing", **kwargs):
        self.console = Console()
        self.total = total
        self.description = description
        self._rich_task_id = None
        self._is_rich = self.console.is_terminal
        self._kwargs = kwargs

        if self._is_rich:
            self._progress = Progress(
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                "[progress.percentage]{task.percentage:>3.0f}%",
                TimeRemainingColumn(),
                console=self.console,
            )
        else:
            self._tqdm_bar = tqdm(total=total, desc=description, **kwargs)

    def __enter__(self):
        if self._is_rich:
            self._progress.__enter__()
            self._rich_task_id = self._progress.add_task(self.description, total=self.total)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._is_rich:
            self._progress.__exit__(exc_type, exc_val, exc_tb)
        else:
            self._tqdm_bar.close()

    def update(self, completed: int, worker: list[str]):
        if self._is_rich:
            self._progress.update(self._rich_task_id, completed=completed, description=f"{self.description} (1 / {len(worker)})")
        else:
            self._tqdm_bar.n = completed
            self._tqdm_bar.refresh()



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
                    raise ValueError("The size of the data does not match the size of the database.")

        with _SmartProgress(total=size, description="lalal") as progress:
            while True:
                with self.lock:
                    completed = self.db.list_state("completed")
                    worker = self.db.list_workers()
                    try:
                        idx = next(self.db)
                    except StopIteration:
                        break

                progress.update(len(completed), worker=worker)

                try:
                    yield self.data[idx]
                except GeneratorExit:
                    with self.lock:
                        self.db.finalize(idx, "failed")
                    raise

                with self.lock:
                    self.db.finalize(idx, "completed")
                    completed = self.db.list_state("completed")
                    if len(completed) == size:
                        if self.cleanup:
                            self.com.unlink()
                        return

                if self._close_trigger:
                    self._close_trigger = False
                    break