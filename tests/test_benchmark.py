"""Benchmark tests for Graphband DAG iteration operations."""

import random
import uuid

import pytest
from flufl.lock import Lock

from laufband import Graphband, Task

def generate_dag_tasks(num_nodes: int, seed: int = 42) -> list[Task]:
    """Generate a DAG structure of tasks with multiple dependencies.

    Creates a balanced DAG where:
    - Root tasks (~10%): no dependencies
    - Middle tasks (~80%): multiple dependencies
    - Leaf tasks (~10%): depend on middle tasks

    Parameters
    ----------
    num_nodes : int
        Total number of tasks to generate.
    seed : int
        Random seed for reproducibility.

    Returns
    -------
    list[Task]
        List of Task objects in topological order.
    """
    rng = random.Random(seed)
    tasks = []

    # Calculate layer sizes
    num_root = max(1, int(num_nodes * 0.1))
    num_leaf = max(1, int(num_nodes * 0.1))
    num_middle = num_nodes - num_root - num_leaf

    # Calculate number of middle layers
    num_layers = max(2, int(num_middle**0.5) // 10)
    nodes_per_layer = num_middle // num_layers if num_layers > 0 else 0

    # Create root tasks (no dependencies)
    root_names = []
    for i in range(num_root):
        name = f"root_{i}"
        tasks.append(Task(id=name, data=f"root_data_{i}", dependencies=set()))
        root_names.append(name)

    # Create middle layers with dependencies
    previous_layer = root_names
    current_idx = 0

    for layer_num in range(num_layers):
        layer_size = nodes_per_layer
        # Last middle layer gets any remaining nodes
        if layer_num == num_layers - 1:
            layer_size = num_middle - current_idx

        current_layer = []
        for _ in range(layer_size):
            name = f"middle_{current_idx}"

            # Pick 1-3 random parents from previous layer
            num_parents = min(len(previous_layer), rng.randint(1, 3))
            parents = set(rng.sample(previous_layer, num_parents))

            tasks.append(
                Task(id=name, data=f"middle_data_{current_idx}", dependencies=parents)
            )

            current_layer.append(name)
            current_idx += 1

        previous_layer = current_layer

    # Create leaf tasks (depend on middle tasks)
    for i in range(num_leaf):
        name = f"leaf_{i}"

        # Pick 1-2 random parents from last middle layer
        num_parents = min(len(previous_layer), rng.randint(1, 2))
        parents = set(rng.sample(previous_layer, num_parents))

        tasks.append(Task(id=name, data=f"leaf_data_{i}", dependencies=parents))

    return tasks


@pytest.mark.benchmark(group="dag-iteration")
@pytest.mark.parametrize(
    "num_nodes",
    [
        100,
        1_000,
    ],
)
def test_dag_iteration_benchmark(benchmark, tmp_path, num_nodes):
    """Benchmark iterating through a DAG with Graphband.

    Parameters
    ----------
    benchmark : BenchmarkFixture
        pytest-benchmark fixture.
    tmp_path : Path
        Temporary directory for test.
    num_nodes : int
        Number of tasks to process (100, 1_000, 10_000).
    """
    # Generate tasks once (not part of benchmark)
    tasks = generate_dag_tasks(num_nodes)

    def create_task_generator():
        """Create a generator that yields tasks."""
        yield from tasks

    def iterate_and_measure():
        run_id = uuid.uuid4().hex[:8]
        db_path = tmp_path / f"bench_{num_nodes}_{run_id}.sqlite"
        lock_path = tmp_path / f"bench_{num_nodes}_{run_id}.lock"
        db_lock_path = tmp_path / f"bench_{num_nodes}_{run_id}_db.lock"

        worker = Graphband(
            create_task_generator(),
            db=f"sqlite:///{db_path}",
            lock=Lock(str(lock_path)),
            db_lock=Lock(str(db_lock_path)),
            tqdm_kwargs={"disable": True},
        )

        list(worker) # iterate

    benchmark(iterate_and_measure)


def test_dag_structure_validation(tmp_path):
    """Validate that generated DAG has correct structure.

    This is not a benchmark test, but validates the DAG generator.
    """
    num_nodes = 100
    tasks = generate_dag_tasks(num_nodes)

    # Verify we got the right number of tasks
    assert len(tasks) == num_nodes

    # Create task lookup
    task_dict = {task.id: task for task in tasks}

    # Verify structure
    root_count = sum(1 for task in tasks if task.id.startswith("root_"))
    middle_count = sum(1 for task in tasks if task.id.startswith("middle_"))
    leaf_count = sum(1 for task in tasks if task.id.startswith("leaf_"))

    assert root_count + middle_count + leaf_count == num_nodes

    # Verify root tasks have no dependencies
    for task in tasks:
        if task.id.startswith("root_"):
            assert len(task.dependencies) == 0

    # Verify leaf tasks have dependencies
    for task in tasks:
        if task.id.startswith("leaf_"):
            assert len(task.dependencies) > 0

    # Verify all dependencies exist
    for task in tasks:
        for dep_id in task.dependencies:
            assert dep_id in task_dict, f"Dependency {dep_id} not found for {task.id}"
