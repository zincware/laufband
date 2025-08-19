"""Test for lazy consumption behavior in GraphBand."""

from laufband import Graphband


def test_graphband_lazy_consumption(tmp_path):
    """Test that GraphBand lazily consumes the generator instead of consuming it all upfront."""
    consumed_items = []

    def lazy_graph():
        """Generator that tracks what items have been consumed."""
        items = [
            ("A", set()),
            ("B", {"A"}),
            ("C", {"A"}),
            ("D", {"B", "C"}),
            ("E", {"D"}),
        ]
        for item, deps in items:
            consumed_items.append(item)
            yield (item, deps)

    db_path = tmp_path / "lazy_test.sqlite"
    pbar = Graphband(graph_fn=lazy_graph(), com=db_path, cleanup=False)

    # Before starting iteration, no items should be consumed
    assert consumed_items == []

    # Start iteration
    iterator = iter(pbar)

    # First next() call should only consume items until finding a ready task
    first_task = next(iterator)

    # Should have found "A" as the first ready task, only consuming "A"
    assert first_task == "A"
    assert consumed_items == ["A"]  # Only "A" should be consumed

    # Second next() call should consume more items to find the next ready task
    second_task = next(iterator)

    # Should have found "B" or "C", only consuming what's needed
    assert second_task in ["B", "C"]
    # Should have consumed A and the second task only (efficient lazy consumption)
    assert len(consumed_items) == 2
    assert consumed_items == ["A", second_task]

    # Continue until we get all tasks
    remaining_tasks = list(iterator)
    all_tasks = [first_task, second_task] + remaining_tasks

    # All tasks should be processed
    assert set(all_tasks) == {"A", "B", "C", "D", "E"}

    # By the end, all items should be consumed, but not all at once upfront
    assert set(consumed_items) == {"A", "B", "C", "D", "E"}


def test_graphband_lazy_consumption_with_early_termination(tmp_path):
    """Test that early termination doesn't consume the entire generator."""
    consumed_items = []

    def lazy_graph():
        """Generator with many items."""
        for i in range(100):  # Large number of tasks
            consumed_items.append(f"task-{i}")
            yield (f"task-{i}", set())  # No dependencies, all ready immediately

    db_path = tmp_path / "lazy_early_term.sqlite"
    pbar = Graphband(graph_fn=lazy_graph(), com=db_path, cleanup=False)

    # Process only first 3 tasks
    iterator = iter(pbar)
    processed_tasks = []
    for _ in range(3):
        task = next(iterator)
        processed_tasks.append(task)
        # Close after getting 3 tasks
        if len(processed_tasks) == 3:
            pbar.close()
            break

    # Should have processed exactly 3 tasks
    assert len(processed_tasks) == 3

    # Should have consumed only a small number of items, not all 100
    # The exact number depends on implementation, but should be much less than 100
    assert len(consumed_items) < 20, f"Consumed too many items: {len(consumed_items)}"
    assert len(consumed_items) >= 3, (
        f"Should have consumed at least 3 items: {len(consumed_items)}"
    )


def test_graphband_lazy_consumption_complex_dependencies(tmp_path):
    """Test lazy consumption with complex dependency graph."""
    consumed_items = []

    def complex_graph():
        """Generator with complex dependencies that requires lazy evaluation."""
        # This creates a diamond dependency pattern that should be consumed lazily
        tasks = [
            ("root", set()),
            ("left_1", {"root"}),
            ("left_2", {"root"}),
            ("right_1", {"root"}),
            ("right_2", {"root"}),
            ("left_merge", {"left_1", "left_2"}),
            ("right_merge", {"right_1", "right_2"}),
            ("final", {"left_merge", "right_merge"}),
            # Add more tasks that shouldn't be consumed unless needed
            ("extra_1", {"final"}),
            ("extra_2", {"final"}),
            ("extra_final", {"extra_1", "extra_2"}),
        ]

        for task, deps in tasks:
            consumed_items.append(task)
            yield (task, deps)

    db_path = tmp_path / "lazy_complex.sqlite"
    pbar = Graphband(graph_fn=complex_graph(), com=db_path, cleanup=False)

    # Process tasks one by one and verify consumption pattern
    iterator = iter(pbar)

    # First task should be root
    first = next(iterator)
    assert first == "root"

    # Should have consumed items progressively, not all at once
    consumed_so_far = len(consumed_items)
    assert consumed_so_far >= 1  # At least root
    assert consumed_so_far < 11  # But not all 11 tasks

    # Process a few more tasks
    second = next(iterator)
    third = next(iterator)

    # More items should be consumed, but still not all
    new_consumed = len(consumed_items)
    assert new_consumed > consumed_so_far
    assert new_consumed < 11  # Still shouldn't have consumed everything

    # Verify dependency order is respected
    processed_tasks = [first, second, third]
    for task in processed_tasks:
        if task in ["left_1", "left_2", "right_1", "right_2"]:
            assert "root" in processed_tasks  # root must be processed first
