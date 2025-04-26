from laufband import laufband
from flufl.lock import Lock
from pathlib import Path
import json


def test_iter(tmp_path):
    lock = Lock("ptqdm.lock")
    data = list(range(100))
    com = Path(tmp_path) / "laufband.json"

    output = Path(tmp_path) / "data.json"
    output.write_text(json.dumps({"data": []}))

    for point in laufband(lock, data, com):
        with lock:
            filecontent = json.loads(output.read_text())
            filecontent["data"].append(point)
            output.write_text(json.dumps(filecontent))

    results = json.loads(output.read_text())["data"]
    assert results == data
