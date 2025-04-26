from laufband import laufband
from flufl.lock import Lock
from tqdm import tqdm
from pathlib import Path


def test_iter(tmp_path):
    # import time
    # import json
    # from pathlib import Path

    lock = Lock("ptqdm.lock")
    data = list(range(100))
    com = Path(tmp_path) / "laufband.json"

    # if com.exists():
    #     com.unlink()  # Remove old state file if it exists

    # com.write_text(json.dumps({"active": [], "completed": []}))

    results = []

    for point in laufband(lock, data, com):
        results.append(point)

    assert results == data


# from flufl.lock import Lock
# import time
# from tqdm import tqdm
# import json
# from pathlib import Path
# from collections.abc import Sequence


# def ptqdm(lock: Lock, data: Sequence, com: Path):
#     tbar = tqdm(total=len(data), desc="Processing items", unit="item")
#     while True:
#         with lock:
#             state = json.loads(com.read_text())
#             # find the next index to process
#             for idx in range(len(data)):
#                 if idx not in state["active"] + state["completed"]:
#                     state["active"].append(idx)
#                     com.write_text(json.dumps(state))
#                     break
#             else:
#                 # No more work left
#                 tbar.n = len(state["completed"])
#                 tbar.refresh()
#                 print("All items processed.")
#                 return

#         # Update progress bar for completed items
#         tbar.n = len(state["completed"])
#         tbar.refresh()

#         # Process the item
#         yield data[idx]

#         with lock:
#             # After processing, mark as completed
#             state = json.loads(com.read_text())
#             if idx in state["active"]:
#                 state["active"].remove(idx)
#             if idx not in state["completed"]:
#                 state["completed"].append(idx)
#             com.write_text(json.dumps(state))
#             tbar.update(1)


# lock = Lock("ptqdm.lock")
# data = list(range(100))
# com = Path("ptqdm.json")

# if com.exists():
#     com.unlink()  # Remove old state file if it exists

# com.write_text(json.dumps({"active": [], "completed": []}))

# for idx, atoms in enumerate(ptqdm(lock, data, com)):
#     time.sleep(0.1)  # Simulate some processing time
