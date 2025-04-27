import json
import multiprocessing
import os
import time
from pathlib import Path

import pytest
from flufl.lock import Lock

from laufband import Laufband
from laufband.db import LaufbandDB


def test_iter_default(tmp_path):
    os.chdir(tmp_path)

    output = tmp_path / "data.json"
    output.write_text(json.dumps({"data": []}))
    data = list(range(100))

    com_file = tmp_path / "laufband.sqlite"

    for point in Laufband(data, cleanup=True):
        filecontent = json.loads(output.read_text())
        filecontent["data"].append(point)
        output.write_text(json.dumps(filecontent))

        assert com_file.exists()

    assert not com_file.exists()
    assert json.loads(output.read_text())["data"] == data


def test_iter(tmp_path):
    lock = Lock("ptqdm.lock")
    data = list(range(100))
    db = LaufbandDB(tmp_path / "laufband.sqlite")

    output = tmp_path / "data.json"
    output.write_text(json.dumps({"data": []}))

    for point in Laufband(data, lock, db.db_path):
        with lock:
            filecontent = json.loads(output.read_text())
            filecontent["data"].append(point)
            output.write_text(json.dumps(filecontent))
            assert db.list_state("running") == [point]

    results = json.loads(output.read_text())["data"]
    assert results == data


def worker(lock_path: Path, com_path: Path, output_path: Path, name):
    lock = Lock(str(lock_path))
    data = list(range(100))
    for point in Laufband(data, lock, com_path):
        with lock:
            filecontent = json.loads(output_path.read_text())
            filecontent[name].append(point)
            output_path.write_text(json.dumps(filecontent))
        time.sleep(0.1)  # simulate some processing time


def test_worker(tmp_path):
    """Test worker function."""
    lock_path = tmp_path / "ptqdm.lock"
    com_path = tmp_path / "laufband.sqlite"
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
    db_path = tmp_path / "laufband.sqlite"
    output_path = tmp_path / "data.json"
    num_processes = 4  # Let's use a pool of 4 workers
    total_iterations = 100

    # Setup files
    output_path.write_text(json.dumps({"a": [], "b": [], "c": [], "d": []}))
    keys = ["a", "b", "c", "d"]

    # Create a multiprocessing pool
    with multiprocessing.Pool(processes=num_processes) as pool:
        # Prepare arguments for each worker
        tasks = [(lock_path, db_path, output_path, key) for key in keys]
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
    db_path = tmp_path / "laufband.sqlite"
    db = LaufbandDB(db_path)

    output = tmp_path / "data.json"
    output.write_text(json.dumps({"data": []}))

    pbar = Laufband(data, lock, db_path)

    for idx, point in enumerate(pbar):
        with lock:
            filecontent = json.loads(output.read_text())
            filecontent["data"].append(point)
            output.write_text(json.dumps(filecontent))
        if idx == 5:
            # Simulate a crash or interruption
            pbar.close()

    assert len(json.loads(output.read_text())["data"]) == 6
    assert db.list_state("running") == []
    assert db.list_state("completed") == list(range(6))
    assert db.list_state("failed") == []
    assert db.list_state("pending") == list(range(6, 10))

    # resume processing
    for point in Laufband(data, lock, db_path):
        with lock:
            filecontent = json.loads(output.read_text())
            filecontent["data"].append(point)
            output.write_text(json.dumps(filecontent))

    assert len(json.loads(output.read_text())["data"]) == 10
    assert db.list_state("running") == []
    assert db.list_state("completed") == list(range(10))
    assert db.list_state("failed") == []
    assert db.list_state("pending") == []


def test_failed(tmp_path):
    """Test if laufband can handle failed jobs."""
    com = tmp_path / "laufband.sqlite"
    db = LaufbandDB(com)

    with pytest.raises(ValueError):
        data = list(range(100))
        for idx, item in enumerate(Laufband(data, com=com)):
            # Process each item in the dataset
            if idx == 50:
                print("raising error")
                raise ValueError("Simulated failure")

    assert db.list_state("running") == []
    assert db.list_state("completed") == list(range(50))
    assert db.list_state("failed") == [50]
    assert db.list_state("pending") == list(range(51, 100))


def test_failed_via_break(tmp_path):
    """Test if laufband can handle failed jobs."""
    com = tmp_path / "laufband.sqlite"
    db = LaufbandDB(com)

    data = list(range(100))
    for idx, item in enumerate(Laufband(data, com=com)):
        # Process each item in the dataset
        if idx == 50:
            break

    assert db.list_state("running") == []
    assert db.list_state("completed") == list(range(50))
    assert db.list_state("failed") == [50]
    assert db.list_state("pending") == list(range(51, 100))


def test_inconsistent_db(tmp_path):
    """Test if laufband can handle inconsistent database."""
    db = LaufbandDB(tmp_path / "laufband1.sqlite")

    for i in Laufband(list(range(10)), com=db.db_path):
        pass

    # same database, different size is not allowed
    with pytest.raises(
        ValueError,
        match="The size of the data does not match the size of the database.",
    ):
        for i in Laufband(list(range(100)), com=db.db_path):
            pass


def test_identifier(tmp_path):
    """Test if laufband can handle multiple workers."""
    lock = Lock("ptqdm.lock")
    data = list(range(100))
    com = tmp_path / "laufband.sqlite"
    db = LaufbandDB(com)

    pbar = Laufband(data, lock, com, identifier="worker_1")

    for idx in pbar:
        if idx == 50:
            pbar.close()

    pbar = Laufband(data, lock, com, identifier="worker_2")
    for idx in pbar:
        if idx == 75:
            pbar.close()
    for idx in Laufband(data, lock, com, identifier=None):
        pass

    for idx in range(51):
        assert db.get_worker(idx) == "worker_1"
    for idx in range(51, 76):
        assert db.get_worker(idx) == "worker_2"
    for idx in range(76, 100):
        assert db.get_worker(idx) is None


def test_iter_access_lock(tmp_path):
    """Test if iter access lock works."""
    os.chdir(tmp_path)
    data = list(range(100))

    pbar = Laufband(data)
    assert isinstance(pbar.lock, Lock)
    assert not pbar.lock.is_locked
    for idx in pbar:
        with pbar.lock:
            assert pbar.lock.is_locked
    assert not pbar.lock.is_locked
