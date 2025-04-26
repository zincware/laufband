import json
import typing as t
from collections.abc import Generator, Sequence
from pathlib import Path

from flufl.lock import Lock
from tqdm import tqdm

_T = t.TypeVar("_T")


def laufband(
    data: Sequence[_T], lock: Lock | None = None, com: Path | None = None, **kwargs
) -> Generator[_T, None, None]:
    """Laufband generator for parallel processing using file-based locking.

    Arguments
    ---------
    data : Sequence
        The data to process. Any object implementing ``__len__`` and ``__getitem__``
        if supported.
    lock : Lock | None
        A lock object to ensure thread safety. If None, a new lock will be created.
    com : Path | None
        The path to the JSON file used to store the state. If None, a new file will be created.
        If not provided, a file named "laufband.json" will be used and removed after completion.
    kwargs : dict
        Additional arguments to pass to tqdm.

    Example
    -------
    >>> import json
    >>> import time
    >>> from pathlib import Path
    >>> from flufl.lock import Lock
    >>> from laufband import laufband
    ...
    >>> output_file = Path("data.json")
    >>> output_file.write_text(json.dumps({"processed_data": []}))
    >>> data = list(range(100))
    >>> lock = Lock("laufband.lock")
    ...
    >>> for item in laufband(data, lock=lock, desc="using Laufband"):
    ...    # Simulate some computationally intensive task
    ...    time.sleep(0.1)
    ...    with lock:
    ...        # Access and modify a shared resource (e.g., a file) safely using the lock
    ...        file_content = json.loads(output_file.read_text())
    ...        file_content["processed_data"].append(item)
    ...        output_file.write_text(json.dumps(file_content))

    """
    remove_com = com is None
    if com is None:
        com = Path("laufband.json")
    if lock is None:
        lock = Lock("laufband.lock")
    if not com.exists():
        com.write_text(json.dumps({"active": [], "completed": []}))
    tbar = tqdm(total=len(data), **kwargs)
    while True:
        with lock:
            state = json.loads(com.read_text())
            # find the next index to process
            for idx in range(len(data)):
                if idx not in state["active"] + state["completed"]:
                    state["active"].append(idx)
                    com.write_text(json.dumps(state))
                    break
            else:
                # No more work left
                tbar.n = len(state["completed"])
                tbar.refresh()
                return

        # Update progress bar for completed items
        tbar.n = len(state["completed"])
        tbar.refresh()

        try:
            yield data[idx]
        finally:
            with lock:
                # After processing, mark as completed
                state = json.loads(com.read_text())
                if idx in state["active"]:
                    state["active"].remove(idx)
                if idx not in state["completed"]:
                    state["completed"].append(idx)
                com.write_text(json.dumps(state))
                tbar.update(1)

        if remove_com:
            with lock:
                state = json.loads(com.read_text())
                if len(state["active"]) == 0 and len(state["completed"]) == len(data):
                    com.unlink()
                    return
