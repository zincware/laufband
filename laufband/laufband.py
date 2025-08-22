import typing as t
from collections.abc import Iterable

from laufband.graphband import Graphband
from laufband.task import Task

_T = t.TypeVar("_T", covariant=True)


class SequentialGraphIterator:
    def __init__(self, data):
        self.data = data

    def __len__(self):
        return len(self.data)

    def __iter__(self):
        for idx, item in enumerate(self.data):
            yield Task(id=str(idx), data=item)


class Laufband(Graphband[_T]):
    def __init__(
        self,
        data: Iterable[_T],
        **kwargs,
    ):
        """Laufband generator for parallel processing using file-based locking.

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
        if "db" not in kwargs:
            kwargs["db"] = "sqlite:///laufband.sqlite"
        super().__init__(graph_fn=SequentialGraphIterator(data=data), **kwargs)

    def __iter__(self):
        for task in super().__iter__():
            yield task.data
