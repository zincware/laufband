import json
from collections.abc import Generator, Sequence
from pathlib import Path

from flufl.lock import Lock
from tqdm import tqdm


def laufband(
    data: Sequence, lock: Lock | None = None, com: Path | None = None, **kwargs
) -> Generator:
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
                print("All items processed.")
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
                    print("All items processed. Communication file removed.")
                    return
