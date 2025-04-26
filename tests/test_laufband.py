import json
import multiprocessing
import os
import time
from pathlib import Path

from flufl.lock import Lock

from laufband import laufband


def test_iter_default(tmp_path):
    os.chdir(tmp_path)

    output = tmp_path / "data.json"
    output.write_text(json.dumps({"data": []}))
    data = list(range(100))

    com_file = tmp_path / "laufband.json"

    for point in laufband(data):
        filecontent = json.loads(output.read_text())
        filecontent["data"].append(point)
        output.write_text(json.dumps(filecontent))

        assert com_file.exists()

    assert not com_file.exists()
    assert json.loads(output.read_text())["data"] == data


def test_iter(tmp_path):
    lock = Lock("ptqdm.lock")
    data = list(range(100))
    com = tmp_path / "laufband.json"

    output = tmp_path / "data.json"
    output.write_text(json.dumps({"data": []}))

    for point in laufband(data, lock, com):
        with lock:
            filecontent = json.loads(output.read_text())
            filecontent["data"].append(point)
            output.write_text(json.dumps(filecontent))
            assert len(json.loads(com.read_text())["active"]) == 1

    results = json.loads(output.read_text())["data"]
    assert results == data


def worker(lock_path: Path, com_path: Path, output_path: Path, name):
    lock = Lock(str(lock_path))
    data = list(range(100))
    for point in laufband(data, lock, com_path):
        with lock:
            filecontent = json.loads(output_path.read_text())
            filecontent[name].append(point)
            output_path.write_text(json.dumps(filecontent))
        time.sleep(0.1)  # simulate some processing time


def test_worker(tmp_path):
    """Test worker function."""
    lock_path = tmp_path / "ptqdm.lock"
    com_path = tmp_path / "laufband.json"
    output_path = tmp_path / "data.json"

    # Setup files
    output_path.write_text(json.dumps({"data": []}))

    worker(lock_path, com_path, output_path, "data")

    # Validate output
    final_data = json.loads(output_path.read_text())["data"]
    assert final_data == list(range(100))


def test_multiprocessing_pool(tmp_path):
    """Test laufband using a multiprocessing pool."""
    lock_path = tmp_path / "ptqdm.lock"
    com_path = tmp_path / "laufband.json"
    output_path = tmp_path / "data.json"
    num_processes = 4  # Let's use a pool of 4 workers
    total_iterations = 100

    # Setup files
    output_path.write_text(json.dumps({"a": [], "b": [], "c": [], "d": []}))
    keys = ["a", "b", "c", "d"]

    # Create a multiprocessing pool
    with multiprocessing.Pool(processes=num_processes) as pool:
        # Prepare arguments for each worker
        tasks = [(lock_path, com_path, output_path, key) for key in keys]
        # Apply the worker function asynchronously to the tasks
        pool.starmap(worker, tasks)

    # Validate output
    results = []
    final_data = json.loads(output_path.read_text())
    for key in keys:
        assert len(final_data[key]) > 0
        results.extend(final_data[key])

    results.sort()
    assert results == list(range(total_iterations))


def test_resume_progress(tmp_path):
    """Test if laufband can resume from partial progress."""
    lock = Lock("ptqdm.lock")
    data = list(range(10))
    com = tmp_path / "laufband.json"

    output = tmp_path / "data.json"
    output.write_text(json.dumps({"data": []}))

    for idx, point in enumerate(laufband(data, lock, com)):
        with lock:
            filecontent = json.loads(output.read_text())
            filecontent["data"].append(point)
            output.write_text(json.dumps(filecontent))
        if idx == 5:
            # Simulate a crash or interruption
            break

    assert len(json.loads(output.read_text())["data"]) == 6
    assert len(json.loads(com.read_text())["active"]) == 0
    assert len(json.loads(com.read_text())["completed"]) == 6

    # resume processing
    for point in laufband(data, lock, com):
        with lock:
            filecontent = json.loads(output.read_text())
            filecontent["data"].append(point)
            output.write_text(json.dumps(filecontent))

    assert len(json.loads(com.read_text())["completed"]) == 10
    assert len(json.loads(output.read_text())["data"]) == 10
    assert len(json.loads(com.read_text())["active"]) == 0
