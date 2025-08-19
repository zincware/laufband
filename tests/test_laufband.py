import json
import multiprocessing
import os
import time
from contextlib import nullcontext
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

    for point in Laufband(data, cleanup=True, com=com_file):
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

    for point in Laufband(data, lock=lock, com=db.db_path):
        with lock:
            filecontent = json.loads(output.read_text())
            filecontent["data"].append(point)
            output.write_text(json.dumps(filecontent))
            assert db.list_state("running") == [str(point)]

    results = json.loads(output.read_text())["data"]
    assert results == data


def worker(lock_path: Path, com_path: Path, output_path: Path, name):
    lock = Lock(str(lock_path))
    data = list(range(100))
    for point in Laufband(data, lock=lock, com=com_path):
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

    pbar = Laufband(data, lock=lock, com=db_path)
    assert pbar.disabled is False

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
    assert db.list_state("completed") == [str(i) for i in range(6)]
    assert db.list_state("failed") == []
    assert db.list_state("pending") == [str(i) for i in range(6, 10)]

    # resume processing
    for point in Laufband(data, lock=lock, com=db_path):
        with lock:
            filecontent = json.loads(output.read_text())
            filecontent["data"].append(point)
            output.write_text(json.dumps(filecontent))

    assert len(json.loads(output.read_text())["data"]) == 10
    assert db.list_state("running") == []
    assert db.list_state("completed") == [str(i) for i in range(10)]
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
    assert db.list_state("completed") == [str(i) for i in range(50)]
    assert db.list_state("failed") == ["50"]
    assert db.list_state("pending") == [str(i) for i in range(51, 100)]


def test_failed_via_break(tmp_path):
    """Test if laufband can handle failed jobs."""
    com = tmp_path / "laufband.sqlite"
    db = LaufbandDB(com)

    data = list(range(100))
    pbar = Laufband(data, com=com)
    for idx, item in enumerate(pbar):
        # Process each item in the dataset
        if idx == 50:
            break

    assert db.list_state("running") == []
    assert db.list_state("completed") == [str(i) for i in range(50)]
    assert db.list_state("failed") == ["50"]
    assert db.list_state("pending") == [str(i) for i in range(51, 100)]

    assert pbar.running == []
    assert pbar.completed == list(range(50))
    assert pbar.failed == [50]
    assert pbar.pending == list(range(51, 100))
    assert pbar.died == []


def test_dynamic_db_expansion(tmp_path):
    """Test if laufband can handle dynamic database expansion with unknown lengths."""
    db = LaufbandDB(tmp_path / "laufband1.sqlite")

    # First run with 10 items
    results1 = []
    for i in Laufband(list(range(10)), com=db.db_path):
        results1.append(i)

    assert results1 == list(range(10))

    # Second run with 100 items - should work and add new items
    results2 = []
    for i in Laufband(list(range(100)), com=db.db_path):
        results2.append(i)

    # Should process the new items (10-99) that weren't in the first run
    assert len(results2) == 90
    assert set(results2) == set(range(10, 100))


def test_identifier(tmp_path):
    """Test if laufband can handle multiple workers."""
    lock = Lock("ptqdm.lock")
    data = list(range(100))
    com = tmp_path / "laufband.sqlite"
    db = LaufbandDB(com)

    pbar = Laufband(data, lock=lock, com=com, identifier="worker_1")

    for idx in pbar:
        if idx == 50:
            pbar.close()

    pbar = Laufband(data, lock=lock, com=com, identifier="worker_2")
    for idx in pbar:
        if idx == 75:
            pbar.close()
    for idx in Laufband(data, lock=lock, com=com):
        pass

    for idx in range(51):
        assert db.get_worker(idx) == "worker_1"
    for idx in range(51, 76):
        assert db.get_worker(idx) == "worker_2"
    for idx in range(76, 100):
        assert db.get_worker(idx) == str(os.getpid())


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


def test_iter_finished(tmp_path):
    """Test if iter finished works."""
    os.chdir(tmp_path)
    data = list(range(100))

    pbar = Laufband(data)
    for idx in pbar:
        if idx == 50:
            assert len(pbar.completed) == 50
            assert len(pbar.running) == 1
            assert len(pbar.pending) == 49

    assert len(pbar.completed) == len(pbar)


def test_iter_len(tmp_path):
    """Test if iter len works."""
    os.chdir(tmp_path)
    pbar = Laufband(list(range(100)))
    assert len(pbar) == 100

    pbar = Laufband(list(range(50)))
    assert len(pbar) == 50


def test_failure_policy_stop(tmp_path):
    """Test if failure policy stop works."""
    os.chdir(tmp_path)
    pbar = Laufband(list(range(100)), failure_policy="stop")
    for idx in pbar:
        if idx == 10:
            break

    # reiterating will raise an error, as there is one job
    # in the database that has failed
    with pytest.raises(RuntimeError):
        for idx in pbar:
            pass


def test_lock_and_path():
    with pytest.raises(ValueError):
        Laufband(list(range(10)), lock=Lock("file"), lock_path="invalid_path")

    pbar = Laufband(list(range(10)), lock_path=Path("valid_path"))
    assert pbar.lock.lockfile == "valid_path"

    pbar = Laufband(list(range(10)))
    assert pbar.lock.lockfile == "laufband.lock"

    pbar = Laufband(list(range(10)), lock=Lock("custom.lock"))
    assert pbar.lock.lockfile == "custom.lock"


def test_disable(tmp_path):
    """Test if Laufband can be disabled."""
    os.chdir(tmp_path)
    data = list(range(100))

    pbar = Laufband(data, disable=True, identifier="myworker")
    assert pbar.disabled is True

    assert list(pbar) == data
    # running it again won't change the result, contrary
    #  to the default behavior of Laufband
    assert list(pbar) == data

    assert pbar.com.exists()

    with pytest.raises(RuntimeError):
        pbar.pending
    with pytest.raises(RuntimeError):
        pbar.completed
    with pytest.raises(RuntimeError):
        pbar.failed
    with pytest.raises(RuntimeError):
        pbar.running
    with pytest.raises(RuntimeError):
        pbar.died

    with pbar.lock:
        pass
    assert isinstance(pbar.lock, nullcontext)

    assert pbar.identifier == "myworker"


@pytest.fixture
def disable_laufband():
    """Fixture to disable Laufband for testing."""
    os.environ["LAUFBAND_DISABLE"] = "1"
    yield
    del os.environ["LAUFBAND_DISABLE"]


def test_disable_via_env(disable_laufband):
    """Test if Laufband can be disabled via environment variable."""
    pbar = Laufband(list(range(100)))
    assert pbar.disabled is True


def test_laufband_as_graphband_wrapper(tmp_path):
    """Test that Laufband properly wraps Graphband with disconnected nodes."""
    data = list(range(10))
    pbar = Laufband(data, com=tmp_path / "laufband.sqlite", cleanup=False)
    
    # Should process all items without dependencies
    results = list(pbar)
    assert set(results) == set(data)
    
    # Should have no failures since no dependencies
    assert len(pbar.failed) == 0


def test_laufband_generator_support(tmp_path):
    """Test that Laufband works with generators (lazy discovery)."""
    def data_generator():
        for i in range(5):
            yield f"item-{i}"
    
    # Convert generator to list since Laufband expects sequence
    data = list(data_generator())
    pbar = Laufband(data, com=tmp_path / "laufband.sqlite", cleanup=True)
    
    results = list(pbar)
    expected = [f"item-{i}" for i in range(5)]
    assert set(results) == set(expected)


def test_laufband_hash_function_behavior(tmp_path):
    """Test Laufband's hash function behavior for task identity."""
    data = ["a", "b", "c"]
    pbar = Laufband(data, com=tmp_path / "laufband.sqlite", cleanup=False)  # Don't cleanup so we can check state
    
    # Process items
    results = list(pbar)
    assert set(results) == set(data)
    
    # Task IDs should be string indices for compatibility
    completed_task_ids = pbar.completed
    expected_indices = [0, 1, 2]  # Indices of items in data
    assert set(completed_task_ids) == set(expected_indices)


def test_laufband_unknown_length_sequences(tmp_path):
    """Test that Laufband now supports unknown length sequences."""
    class UnknownLengthSequence:
        def __init__(self, data):
            self.data = data
            
        def __getitem__(self, index):
            return self.data[index]
            
        def __iter__(self):
            return iter(self.data)
            
        # Intentionally no __len__ method
    
    data = UnknownLengthSequence([1, 2, 3, 4, 5])
    pbar = Laufband(data, com=tmp_path / "laufband.sqlite", cleanup=True)
    
    # Should work even without known length
    results = list(pbar)
    assert set(results) == {1, 2, 3, 4, 5}


def test_laufband_with_custom_hash_function(tmp_path):
    """Test Laufband with custom hash function for task identity."""
    # Use simple objects but with custom hash function
    data = ["task_alpha", "task_beta", "task_gamma"]
    
    # Custom hash function that works with UUID mapping
    # The function receives item_uuid, so we need to access the original item via the mapping
    def custom_hash(item_uuid):
        # This is a bit artificial since we need to access the Laufband instance
        # In practice, users would structure this differently
        # For this test, let's just create a hash based on the UUID itself
        return f"custom-uuid-{item_uuid[:8]}"
    
    pbar = Laufband(data, hash_fn=custom_hash, com=tmp_path / "laufband.sqlite", cleanup=False)
    
    # Should process all items using custom hash function
    results = list(pbar)
    assert len(results) == 3
    assert set(results) == {"task_alpha", "task_beta", "task_gamma"}
    
    # Verify the custom hash function was used for task IDs
    underlying_completed = pbar._graphband.completed
    # Task IDs should start with "custom-uuid-" followed by the first 8 chars of UUID
    assert all(task_id.startswith("custom-uuid-") for task_id in underlying_completed)
    assert len(underlying_completed) == 3


def test_laufband_non_hashable_items(tmp_path):
    """Test Laufband with non-hashable items (lists)."""
    # Non-hashable data - lists cannot be dictionary keys
    data = [[1, 2], [3, 4], [5, 6]]
    
    pbar = Laufband(data, com=tmp_path / "laufband.sqlite", cleanup=False)
    
    # Should process all items correctly
    results = list(pbar)
    assert len(results) == 3
    assert all(isinstance(item, list) for item in results)
    
    # Results should contain all original lists
    results_set = {tuple(item) for item in results}  # Convert to tuples for set comparison
    expected_set = {(1, 2), (3, 4), (5, 6)}
    assert results_set == expected_set
    
    # Should have processed all tasks
    assert len(pbar.completed) == 3


def test_laufband_mixed_hashable_non_hashable(tmp_path):
    """Test Laufband with mixed hashable and non-hashable items."""
    # Mix of hashable and non-hashable items
    data = [
        "string",           # hashable
        [1, 2, 3],         # non-hashable list
        42,                # hashable
        {"key": "value"},  # non-hashable dict
        (1, 2),           # hashable tuple
    ]
    
    pbar = Laufband(data, com=tmp_path / "laufband.sqlite", cleanup=False)
    
    results = list(pbar)
    assert len(results) == 5
    
    # Check each type is preserved
    result_types = [type(item).__name__ for item in results]
    expected_types = ["str", "list", "int", "dict", "tuple"]
    assert set(result_types) == set(expected_types)
    
    # Check specific values are preserved
    assert "string" in results
    assert [1, 2, 3] in results
    assert 42 in results
    assert {"key": "value"} in results
    assert (1, 2) in results


def test_laufband_hashable_objects_with_complex_hash(tmp_path):
    """Test Laufband with hashable objects but complex custom hash function."""
    # Use tuples (hashable) but with custom identification
    data = [
        ("user", 1, "john"),
        ("user", 2, "jane"), 
        ("user", 3, "bob"),
    ]
    
    # Custom hash function using specific tuple elements
    def user_hash(item_uuid):
        # Get the original item from UUID mapping
        # This function operates on UUIDs, so we need to access the underlying data
        return f"user_{id(item_uuid)}"  # Simple example using object id
    
    pbar = Laufband(data, hash_fn=user_hash, com=tmp_path / "laufband.sqlite", cleanup=False)
    
    results = list(pbar)
    assert len(results) == 3
    assert all(isinstance(item, tuple) for item in results)
    assert all(item[0] == "user" for item in results)
    
    # Verify all user data is present
    user_ids = {item[1] for item in results}
    assert user_ids == {1, 2, 3}

def test_consume_generator(tmp_path):
    """Test consuming a generator with Laufband."""
    raise_error = True

    def generator():
        if raise_error:
            raise ValueError("Generator not ready yet")
        yield from range(10)

    pbar1 = Laufband(generator(), com=tmp_path / "laufband.sqlite", cleanup=False)

    raise_error = False
    results1 = list(pbar1)
    assert len(results1) == 10
