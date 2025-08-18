import networkx as nx
from laufband import Graphband
from laufband.db import LaufbandDB


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