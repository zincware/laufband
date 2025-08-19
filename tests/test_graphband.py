import networkx as nx
from laufband import Graphband
import time
import random
import multiprocessing as mp
from pathlib import Path


def simple_graph():
    G = nx.DiGraph()
    G.add_edges_from([
        ("A", "B"),  # A → B
        ("A", "C"),  # A → C
        ("B", "D"),  # B → D
        ("C", "D"),  # C → D
    ])
    return G


def test_graphband_sequential(tmp_path):
    """Tasks should follow dependency order in a static DAG."""
    db_path = tmp_path / "graph.sqlite"
    pbar = Graphband(graph_fn=simple_graph, com=db_path, cleanup=False)

    results = []
    for node in pbar:
        results.append(node)

    # The order may differ, but constraints must be respected
    assert results.index("A") < results.index("B")
    assert results.index("A") < results.index("C")
    assert results.index("B") < results.index("D")
    assert results.index("C") < results.index("D")

    # Check that all tasks were completed by using the same DB instance
    completed_tasks = pbar.completed
    expected_tasks = {pbar.hash_fn(node) for node in ["A", "B", "C", "D"]}
    assert set(completed_tasks) == expected_tasks


def test_graphband_custom_hash_fn(tmp_path):
    """Support for user-provided hash function."""
    def graph_fn():
        G = nx.DiGraph()
        G.add_edges_from([(1, 2), (2, 3)])
        return G

    db_path = tmp_path / "graph.sqlite"
    hash_fn = lambda x: f"task-{x}"

    pbar = Graphband(graph_fn=graph_fn, com=db_path, hash_fn=hash_fn, cleanup=False)
    results = list(pbar)

    # Check using the Graphband instance
    completed_tasks = pbar.completed
    assert set(completed_tasks) == {"task-1", "task-2", "task-3"}
    assert results == [1, 2, 3]

def test_graphband_double_worker(tmp_path):
    def graph_fn():
        G = nx.DiGraph()
        G.add_edges_from([(1, 2),(2, 4), (1, 3), (2, 5)])
        return G
    
    db_path = tmp_path / "graph.sqlite"
    pbar1 = Graphband(graph_fn=graph_fn, com=db_path)
    pbar2 = Graphband(graph_fn=graph_fn, com=db_path)

    pbar1_iter = iter(pbar1)
    pbar2_iter = iter(pbar2)
    
    x1 = next(pbar1_iter)
    x2 = next(pbar1_iter)
    x3 = next(pbar2_iter)

    assert x1 == 1
    assert x2 in {2, 3}
    assert x3 in {2, 3}


def test_graphband_dynamic_graph_updates(tmp_path):
    """Test that graph state is re-evaluated before processing each task."""
    tasks_to_add = []
    
    def dynamic_graph_fn():
        G = nx.DiGraph()
        G.add_edges_from([("A", "B"), ("A", "C")])
        # Add dynamic tasks if they exist
        for task in tasks_to_add:
            G.add_node(task)
            G.add_edge("A", task)
        return G
    
    db_path = tmp_path / "graph.sqlite"
    pbar = Graphband(graph_fn=dynamic_graph_fn, com=db_path, cleanup=False)
    
    results = []
    pbar_iter = iter(pbar)
    
    # Get first task (should be A)
    task1 = next(pbar_iter)
    results.append(task1)
    assert task1 == "A"
    
    # Add new task dynamically
    tasks_to_add.append("D")
    
    # Get remaining tasks - should include the dynamically added task
    remaining = list(pbar_iter)
    results.extend(remaining)
    
    # Should have processed all tasks including dynamically added one
    assert set(results) == {"A", "B", "C", "D"}
    assert results.index("A") < results.index("B")
    assert results.index("A") < results.index("C") 
    assert results.index("A") < results.index("D")


def test_graphband_generator_input():
    """Test Graphband with generator-based lazy task discovery."""
    def task_generator():
        for i in range(5):
            yield f"task-{i}"
    
    def graph_from_generator():
        G = nx.DiGraph()
        # Add nodes from generator
        for task in task_generator():
            G.add_node(task)
        return G
    
    pbar = Graphband(graph_fn=graph_from_generator, cleanup=True)
    results = list(pbar)
    
    expected = [f"task-{i}" for i in range(5)]
    assert set(results) == set(expected)


def test_graphband_task_identity_determinism(tmp_path):
    """Test that task identity is deterministic across workers."""
    def graph_fn():
        G = nx.DiGraph()
        G.add_nodes_from([1, 2, 3])
        return G
    
    # Custom hash function to ensure determinism
    def deterministic_hash(item):
        return f"id-{item}"
    
    db_path = tmp_path / "graph.sqlite"
    
    # Create two workers with same graph and hash function
    worker1 = Graphband(graph_fn=graph_fn, hash_fn=deterministic_hash, com=db_path)
    worker2 = Graphband(graph_fn=graph_fn, hash_fn=deterministic_hash, com=db_path)
    
    # Both should see the same task IDs
    w1_results = list(worker1)
    w2_results = list(worker2)
    
    # All tasks should be processed by one worker or the other, no duplicates
    all_results = w1_results + w2_results
    assert set(all_results) == {1, 2, 3}
    
    # Verify that tasks were split between workers (no duplicates)
    # Since they share a database, total unique completions should be 3
    all_completed = set(worker1.completed + worker2.completed)
    assert len(all_completed) == 3


def test_graphband_termination_conditions(tmp_path):
    """Test termination when no tasks ready/in_progress and generator exhausted."""
    call_count = 0
    
    def limited_graph_fn():
        nonlocal call_count
        call_count += 1
        G = nx.DiGraph()
        
        # Always provide the same tasks - termination will happen when they're all complete
        G.add_nodes_from([f"task-{i}" for i in range(1, 4)])  # task-1, task-2, task-3
        
        return G
    
    db_path = tmp_path / "graph.sqlite"
    pbar = Graphband(graph_fn=limited_graph_fn, com=db_path, cleanup=False)
    
    results = list(pbar)
    
    # Should process all 3 tasks, then terminate
    assert len(results) == 3
    assert set(results) == {"task-1", "task-2", "task-3"}
    
    # Should terminate cleanly when all tasks completed
    completed_tasks = pbar.completed
    assert len(completed_tasks) == 3


def test_graphband_runtime_dependency_injection(tmp_path):
    """Test injecting new dependencies at runtime."""
    dependencies = []
    
    def dynamic_deps_graph():
        G = nx.DiGraph()
        G.add_nodes_from(["A", "B", "C"])
        
        # Add dynamic dependencies
        for src, dst in dependencies:
            G.add_edge(src, dst)
            
        return G
    
    db_path = tmp_path / "graph.sqlite"
    pbar = Graphband(graph_fn=dynamic_deps_graph, com=db_path, cleanup=False)
    
    results = []
    pbar_iter = iter(pbar)
    
    # Initially all tasks are independent
    task1 = next(pbar_iter)
    results.append(task1)
    
    # Inject dependency: B depends on A
    dependencies.append(("A", "B"))
    
    # Continue processing
    remaining = list(pbar_iter)
    results.extend(remaining)
    
    # Should respect the runtime-injected dependency
    assert set(results) == {"A", "B", "C"}
    if "A" in results and "B" in results:
        # If both A and B were processed, A should come before B
        assert results.index("A") < results.index("B")


def test_graphband_large_dag_performance():
    """Test performance with larger DAG."""
    def large_graph():
        G = nx.DiGraph()
        # Create a chain of dependencies: 0 -> 1 -> 2 -> ... -> 99
        for i in range(100):
            G.add_node(i)
            if i > 0:
                G.add_edge(i-1, i)
        return G
    
    pbar = Graphband(graph_fn=large_graph, cleanup=True)
    results = list(pbar)
    
    # Should process all tasks in dependency order
    assert len(results) == 100
    assert results == list(range(100))  # Should be in order due to dependencies


def test_graphband_non_hashable_items(tmp_path):
    """Test Graphband with non-hashable items using UUID mapping as per requirements."""
    import uuid
    
    # Create non-hashable items (dictionaries)
    task_data = [
        {"name": "task_a", "deps": []},
        {"name": "task_b", "deps": ["task_a"]},
        {"name": "task_c", "deps": ["task_a"]},
        {"name": "task_d", "deps": ["task_b", "task_c"]},
    ]
    
    # Create UUID mapping as required by graphband.md
    uuid_mapping = {}
    name_to_uuid = {}
    for task in task_data:
        task_uuid = str(uuid.uuid4())
        uuid_mapping[task_uuid] = task
        name_to_uuid[task["name"]] = task_uuid
    
    def graph_with_uuid_mapping():
        G = nx.DiGraph()
        
        # Add UUID nodes (hashable)
        for task_uuid in uuid_mapping.keys():
            G.add_node(task_uuid)
            # Store the actual task data in node attributes
            G.nodes[task_uuid]['value'] = uuid_mapping[task_uuid]
        
        # Add edges based on dependencies using UUIDs
        for task_uuid, task_data_item in uuid_mapping.items():
            for dep_name in task_data_item["deps"]:
                dep_uuid = name_to_uuid[dep_name]
                G.add_edge(dep_uuid, task_uuid)
        
        return G
    
    # Custom hash function using task name from UUID mapping
    def task_hash(task_uuid):
        task_data_item = uuid_mapping[task_uuid]
        return f"task_{task_data_item['name']}"
    
    db_path = tmp_path / "graph.sqlite"
    pbar = Graphband(
        graph_fn=graph_with_uuid_mapping, 
        hash_fn=task_hash, 
        com=db_path, 
        cleanup=False
    )
    
    results = list(pbar)
    
    # Should process all UUIDs
    assert len(results) == 4
    assert all(isinstance(result_uuid, str) for result_uuid in results)
    
    # Get the actual task data from UUIDs
    result_tasks = [uuid_mapping[result_uuid] for result_uuid in results]
    result_names = [task["name"] for task in result_tasks]
    
    # Should respect dependencies
    assert result_names.index("task_a") < result_names.index("task_b")
    assert result_names.index("task_a") < result_names.index("task_c") 
    assert result_names.index("task_b") < result_names.index("task_d")
    assert result_names.index("task_c") < result_names.index("task_d")
    
    # Verify custom hash function was used
    completed_task_ids = pbar.completed
    expected_task_ids = {"task_task_a", "task_task_b", "task_task_c", "task_task_d"}
    assert set(completed_task_ids) == expected_task_ids


def worker_process(worker_id: int, db_path: Path, results_queue: mp.Queue):
    """Worker function that processes a Graphband graph."""
    digraph = nx.DiGraph()
    for chain in range(4):  # 4 independent chains
        for level in range(6):  # 6 levels deep
            node_name = f"chain_{chain}_level_{level}"
            digraph.add_node(node_name)
            if level > 0:
                digraph.add_edge(f"chain_{chain}_level_{level-1}", node_name)

    processed_nodes = []
    
    try:
        pbar = Graphband(
            graph_fn=lambda: digraph, 
            com=db_path,
            identifier=f"worker_{worker_id}",
            cleanup=False
        )
        
        for node in pbar:
            processed_nodes.append(node)
            time.sleep(random.uniform(0, 0.2))
            
    except Exception as e:
        results_queue.put((worker_id, "error", str(e)))
        return
    
    results_queue.put((worker_id, "success", processed_nodes))


def test_parallel_graphband_processing(tmp_path):
    """Test that multiple Graphband workers don't process duplicate nodes."""
    db_path = tmp_path / "parallel_test.db"
    results_queue = mp.Queue()
    
    # Expected total nodes: 4 chains * 6 levels = 24 nodes
    expected_total_nodes = 4 * 6
    
    processes = []
    for worker_id in range(2):
        p = mp.Process(target=worker_process, args=(worker_id, db_path, results_queue))
        processes.append(p)
        p.start()
    
    for p in processes:
        p.join()
    
    # Collect results
    results = {}
    while not results_queue.empty():
        worker_id, status, data = results_queue.get()
        results[worker_id] = (status, data)
    
    assert len(results) == 2
    for worker_id, (status, data) in results.items():
        assert status == "success", f"Worker {worker_id} failed: {data}"
    
    all_processed_nodes = []
    for worker_id, (status, processed_nodes) in results.items():
        all_processed_nodes.extend(processed_nodes)
    

    assert len(all_processed_nodes) == expected_total_nodes
    assert len(set(all_processed_nodes)) == expected_total_nodes
    
    # Verify each worker processed at least some nodes
    for worker_id, (status, processed_nodes) in results.items():
        assert len(processed_nodes) > 0
