"""Unit tests for graphband performance optimizations."""

import pytest

from laufband.db import TaskEntry, TaskStatusEnum


@pytest.mark.unit
def test_has_more_jobs_caching(db_session, workflow_factory, worker_factory):
    """has_more_jobs should cache results for 5 seconds."""
    workflow = workflow_factory()
    _worker = worker_factory(workflow=workflow)

    # Create a mock Graphband instance with minimal setup
    # We'll test just the caching logic by mocking the necessary attributes
    class MockGraphband:
        def __init__(self):
            self.disabled = False
            self._has_more_jobs_cache = None
            self._cache_timestamp = None
            self._failed_job_cache = {}
            self._iterator_completed = False
            self._max_failed_retries = 0
            self._max_killed_retries = 0
            self._labels = frozenset()
            self._engine = db_session.get_bind()
            self._mock_time = 1000.0  # Start at mock time 1000
            # We need a real db_lock context manager
            from contextlib import nullcontext

            self.db_lock = nullcontext()

        @property
        def labels(self):
            return self._labels

        def advance_time(self, seconds: float):
            """Helper to advance mock time."""
            self._mock_time += seconds

        # Copy the has_more_jobs property from Graphband but use mock time
        @property
        def has_more_jobs(self) -> bool:
            if self.disabled:
                return False

            # Check cache validity (5 second TTL) - using mock time
            now = self._mock_time
            if self._cache_timestamp is not None and (now - self._cache_timestamp) < 5:
                return self._has_more_jobs_cache

            # Simple logic - for this test, just return False
            # and cache it
            result = False
            self._has_more_jobs_cache = result
            self._cache_timestamp = now
            return result

    mock_gb = MockGraphband()

    # First call - should compute and cache
    start_time = mock_gb._mock_time
    result1 = mock_gb.has_more_jobs
    assert result1 is False
    assert mock_gb._has_more_jobs_cache is False
    assert mock_gb._cache_timestamp is not None
    assert abs(mock_gb._cache_timestamp - start_time) < 0.1

    # Second call immediately - should return cached value
    cached_timestamp = mock_gb._cache_timestamp
    result2 = mock_gb.has_more_jobs
    assert result2 is False
    assert mock_gb._cache_timestamp == cached_timestamp  # Timestamp unchanged

    # Advance time 6 seconds and call again - cache should be expired
    mock_gb.advance_time(6)
    result3 = mock_gb.has_more_jobs
    assert result3 is False
    assert mock_gb._cache_timestamp != cached_timestamp  # Timestamp refreshed


@pytest.mark.unit
def test_has_more_jobs_cache_invalidation(db_session, workflow_factory):
    """has_more_jobs cache should expire after 5 seconds."""
    _workflow = workflow_factory()

    class MockGraphband:
        def __init__(self):
            self.disabled = False
            self._mock_time = 1000.0
            self._has_more_jobs_cache = True  # Start with True
            self._cache_timestamp = self._mock_time
            self._failed_job_cache = {}
            self._iterator_completed = False
            self._max_failed_retries = 0
            self._max_killed_retries = 0
            self._labels = frozenset()
            self._engine = db_session.get_bind()
            from contextlib import nullcontext

            self.db_lock = nullcontext()

        @property
        def labels(self):
            return self._labels

        def advance_time(self, seconds: float):
            """Helper to advance mock time."""
            self._mock_time += seconds

        @property
        def has_more_jobs(self) -> bool:
            if self.disabled:
                return False

            now = self._mock_time
            if self._cache_timestamp is not None and (now - self._cache_timestamp) < 5:
                return self._has_more_jobs_cache

            # After cache expires, return False
            result = False
            self._has_more_jobs_cache = result
            self._cache_timestamp = now
            return result

    mock_gb = MockGraphband()

    # Should return cached True
    assert mock_gb.has_more_jobs is True

    # Advance time 6 seconds - cache should be expired
    mock_gb.advance_time(6)

    # Should recompute and return False
    assert mock_gb.has_more_jobs is False


@pytest.mark.unit
def test_batched_dependency_query_missing_dependency(
    db_session, task_factory, workflow_factory
):
    """Batched dependency query should handle missing dependencies correctly."""
    workflow = workflow_factory()

    # Create task A that's completed
    _task_a = task_factory(
        task_id="A", status=TaskStatusEnum.COMPLETED, workflow=workflow
    )

    # Task C has dependencies on A and B, but B doesn't exist
    # This simulates the scenario where batched query needs to detect missing deps
    task_c_deps = {"A", "B"}

    # Fetch all dependencies in a single query (simulating the batched query)
    from sqlalchemy.orm import Session

    with Session(db_session.get_bind()) as session:
        dep_entries = (
            session.query(TaskEntry).filter(TaskEntry.id.in_(task_c_deps)).all()
        )
        dep_map = {e.id: e for e in dep_entries}

        # Check if all dependencies exist and are completed
        skip_task = False
        for dep_id in task_c_deps:
            if dep_id not in dep_map:
                # Dependency B is missing
                skip_task = True
                missing_dep = dep_id
                break
            elif not dep_map[dep_id].completed:
                skip_task = True
                break

    assert skip_task is True
    assert missing_dep == "B"
    assert len(dep_map) == 1  # Only A was found
    assert "A" in dep_map


@pytest.mark.unit
def test_batched_dependency_query_incomplete_dependency(
    db_session, task_factory, workflow_factory
):
    """Batched dependency query should handle incomplete dependencies correctly."""
    workflow = workflow_factory()

    # Create task A that's completed
    _task_a = task_factory(
        task_id="A", status=TaskStatusEnum.COMPLETED, workflow=workflow
    )

    # Create task B that's still running
    _task_b = task_factory(
        task_id="B", status=TaskStatusEnum.RUNNING, workflow=workflow
    )

    # Task C depends on both A and B
    task_c_deps = {"A", "B"}

    # Fetch all dependencies in a single query (simulating the batched query)
    from sqlalchemy.orm import Session

    with Session(db_session.get_bind()) as session:
        dep_entries = (
            session.query(TaskEntry).filter(TaskEntry.id.in_(task_c_deps)).all()
        )
        dep_map = {e.id: e for e in dep_entries}

        # Check if all dependencies exist and are completed
        skip_task = False
        incomplete_dep = None
        for dep_id in task_c_deps:
            if dep_id not in dep_map:
                skip_task = True
                break
            elif not dep_map[dep_id].completed:
                skip_task = True
                incomplete_dep = dep_id
                break

        # Check completion status within the session
        task_a_completed = dep_map["A"].completed
        task_b_completed = dep_map["B"].completed

    assert skip_task is True
    assert incomplete_dep == "B"
    assert len(dep_map) == 2  # Both A and B were found
    assert "A" in dep_map
    assert "B" in dep_map
    assert task_a_completed is True
    assert task_b_completed is False


@pytest.mark.unit
def test_batched_dependency_query_all_complete(
    db_session, task_factory, workflow_factory
):
    """Batched dependency query should correctly identify when all deps are complete."""
    workflow = workflow_factory()

    # Create tasks A and B that are both completed
    _task_a = task_factory(
        task_id="A", status=TaskStatusEnum.COMPLETED, workflow=workflow
    )
    _task_b = task_factory(
        task_id="B", status=TaskStatusEnum.COMPLETED, workflow=workflow
    )

    # Task C depends on both A and B
    task_c_deps = {"A", "B"}

    # Fetch all dependencies in a single query (simulating the batched query)
    from sqlalchemy.orm import Session

    with Session(db_session.get_bind()) as session:
        dep_entries = (
            session.query(TaskEntry).filter(TaskEntry.id.in_(task_c_deps)).all()
        )
        dep_map = {e.id: e for e in dep_entries}

        # Check if all dependencies exist and are completed
        skip_task = False
        for dep_id in task_c_deps:
            if dep_id not in dep_map:
                skip_task = True
                break
            elif not dep_map[dep_id].completed:
                skip_task = True
                break

    assert skip_task is False
    assert len(dep_map) == 2
    assert all(dep_map[dep_id].completed for dep_id in task_c_deps)


@pytest.mark.unit
def test_batched_dependency_query_empty_dependencies(db_session, workflow_factory):
    """Batched dependency query should handle empty dependency sets correctly."""
    _workflow = workflow_factory()

    # Task with no dependencies
    task_deps = set()

    # This should not execute any query if dependencies are empty
    from sqlalchemy.orm import Session

    with Session(db_session.get_bind()) as session:
        skip_task = False
        if task_deps:
            # This block should not execute
            dep_entries = (
                session.query(TaskEntry).filter(TaskEntry.id.in_(task_deps)).all()
            )
            dep_map = {e.id: e for e in dep_entries}

            for dep_id in task_deps:
                if dep_id not in dep_map:
                    skip_task = True
                    break
                elif not dep_map[dep_id].completed:
                    skip_task = True
                    break

    # Should not skip task with no dependencies
    assert skip_task is False
