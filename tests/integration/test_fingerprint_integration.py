"""Integration tests for Phase 2: Fingerprint-based invalidation end-to-end."""

import pytest
from flufl.lock import Lock

from laufband import Graphband, Task


@pytest.mark.integration
def test_fingerprint_disabled_by_default(tmp_path):
    """Fingerprints should be disabled by default (backward compatibility)."""

    def graph_tasks():
        yield Task(id="A", fingerprint="v1")
        yield Task(id="B", dependencies={"A"}, fingerprint="v1")

    db = f"sqlite:///{tmp_path}/test.sqlite"
    lock = Lock(str(tmp_path / "test.lock"))

    # First run
    processed = []
    worker = Graphband(graph_tasks(), db=db, lock=lock)
    for task in worker:
        processed.append(task.id)

    assert processed == ["A", "B"]

    # Second run with changed fingerprints - should skip (disabled by default)
    def graph_tasks_v2():
        yield Task(id="A", fingerprint="v2")  # Changed
        yield Task(id="B", dependencies={"A"}, fingerprint="v2")  # Changed

    processed = []
    worker = Graphband(graph_tasks_v2(), db=db, lock=lock)
    for task in worker:
        processed.append(task.id)

    # Should skip both (fingerprints disabled)
    assert processed == []


@pytest.mark.integration
def test_fingerprint_invalidation_enabled(tmp_path):
    """When enabled, tasks with changed fingerprints should be re-executed."""

    def graph_tasks():
        yield Task(id="A", fingerprint="v1")
        yield Task(id="B", dependencies={"A"}, fingerprint="v1")

    db = f"sqlite:///{tmp_path}/test.sqlite"
    lock = Lock(str(tmp_path / "test.lock"))

    # First run
    processed = []
    worker = Graphband(graph_tasks(), db=db, lock=lock, enable_fingerprints=True)
    for task in worker:
        processed.append(task.id)

    assert processed == ["A", "B"]

    # Second run with changed fingerprints
    def graph_tasks_v2():
        yield Task(id="A", fingerprint="v2")  # Changed
        yield Task(id="B", dependencies={"A"}, fingerprint="v1")  # Unchanged

    processed = []
    worker = Graphband(graph_tasks_v2(), db=db, lock=lock, enable_fingerprints=True)
    for task in worker:
        processed.append(task.id)

    # Should re-execute A (fingerprint changed), skip B (unchanged)
    assert processed == ["A"]


@pytest.mark.integration
def test_fingerprint_no_fingerprint_provided(tmp_path):
    """Tasks without fingerprints should work normally."""

    def graph_tasks():
        yield Task(id="A")  # No fingerprint
        yield Task(id="B", dependencies={"A"})  # No fingerprint

    db = f"sqlite:///{tmp_path}/test.sqlite"
    lock = Lock(str(tmp_path / "test.lock"))

    # First run
    processed = []
    worker = Graphband(graph_tasks(), db=db, lock=lock, enable_fingerprints=True)
    for task in worker:
        processed.append(task.id)

    assert processed == ["A", "B"]

    # Second run - should skip (no fingerprints to compare)
    processed = []
    worker = Graphband(graph_tasks(), db=db, lock=lock, enable_fingerprints=True)
    for task in worker:
        processed.append(task.id)

    assert processed == []


@pytest.mark.integration
def test_fingerprint_mixed_mode(tmp_path):
    """Some tasks with fingerprints, some without."""

    def graph_tasks():
        yield Task(id="A", fingerprint="v1")
        yield Task(id="B", dependencies={"A"})  # No fingerprint
        yield Task(id="C", dependencies={"B"}, fingerprint="v1")

    db = f"sqlite:///{tmp_path}/test.sqlite"
    lock = Lock(str(tmp_path / "test.lock"))

    # First run
    processed = []
    worker = Graphband(graph_tasks(), db=db, lock=lock, enable_fingerprints=True)
    for task in worker:
        processed.append(task.id)

    assert processed == ["A", "B", "C"]

    # Second run with A's fingerprint changed
    def graph_tasks_v2():
        yield Task(id="A", fingerprint="v2")  # Changed
        yield Task(id="B", dependencies={"A"})  # No fingerprint
        yield Task(id="C", dependencies={"B"}, fingerprint="v1")  # Unchanged

    processed = []
    worker = Graphband(graph_tasks_v2(), db=db, lock=lock, enable_fingerprints=True)
    for task in worker:
        processed.append(task.id)

    # Should re-execute A (changed), skip B and C
    assert processed == ["A"]


@pytest.mark.integration
def test_fingerprint_multiple_changes(tmp_path):
    """Multiple tasks with changed fingerprints."""

    def graph_tasks():
        yield Task(id="A", fingerprint="v1")
        yield Task(id="B", fingerprint="v1")
        yield Task(id="C", dependencies={"A", "B"}, fingerprint="v1")

    db = f"sqlite:///{tmp_path}/test.sqlite"
    lock = Lock(str(tmp_path / "test.lock"))

    # First run
    processed = []
    worker = Graphband(graph_tasks(), db=db, lock=lock, enable_fingerprints=True)
    for task in worker:
        processed.append(task.id)

    assert set(processed) == {"A", "B", "C"}

    # Second run with all fingerprints changed
    def graph_tasks_v2():
        yield Task(id="A", fingerprint="v2")
        yield Task(id="B", fingerprint="v2")
        yield Task(id="C", dependencies={"A", "B"}, fingerprint="v2")

    processed = []
    worker = Graphband(graph_tasks_v2(), db=db, lock=lock, enable_fingerprints=True)
    for task in worker:
        processed.append(task.id)

    # All should be re-executed
    assert set(processed) == {"A", "B", "C"}


@pytest.mark.integration
def test_fingerprint_audit_trail(tmp_path):
    """Fingerprint changes should be recorded in audit trail."""
    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session

    from laufband.db import TaskEntry, TaskStatusEnum

    def graph_tasks():
        yield Task(id="A", fingerprint="v1")

    db = f"sqlite:///{tmp_path}/test.sqlite"
    lock = Lock(str(tmp_path / "test.lock"))

    # First run
    worker = Graphband(graph_tasks(), db=db, lock=lock, enable_fingerprints=True)
    for task in worker:
        pass

    # Check audit trail
    engine = create_engine(db)
    with Session(engine) as session:
        task_entry = session.get(TaskEntry, "A")
        assert task_entry is not None
        assert task_entry.last_fingerprint == "v1"

        # Should have one COMPLETED status
        completed_statuses = [
            s for s in task_entry.statuses if s.status == TaskStatusEnum.COMPLETED
        ]
        assert len(completed_statuses) == 1
        assert completed_statuses[0].fingerprint == "v1"

    # Second run with changed fingerprint
    def graph_tasks_v2():
        yield Task(id="A", fingerprint="v2")

    worker = Graphband(graph_tasks_v2(), db=db, lock=lock, enable_fingerprints=True)
    for task in worker:
        pass

    # Check audit trail updated
    with Session(engine) as session:
        task_entry = session.get(TaskEntry, "A")
        assert task_entry.last_fingerprint == "v2"

        # Should have: RUNNING (v1), COMPLETED (v1), INVALIDATED (v2), RUNNING (v2), COMPLETED (v2)
        statuses = list(task_entry.statuses)
        assert len(statuses) == 5

        assert statuses[0].status == TaskStatusEnum.RUNNING
        assert statuses[0].fingerprint is None

        assert statuses[1].status == TaskStatusEnum.COMPLETED
        assert statuses[1].fingerprint == "v1"

        assert statuses[2].status == TaskStatusEnum.INVALIDATED
        assert statuses[2].fingerprint == "v2"

        assert statuses[3].status == TaskStatusEnum.RUNNING
        assert statuses[3].fingerprint is None

        assert statuses[4].status == TaskStatusEnum.COMPLETED
        assert statuses[4].fingerprint == "v2"


@pytest.mark.integration
def test_fingerprint_env_var(tmp_path, monkeypatch):
    """LAUFBAND_ENABLE_FINGERPRINTS environment variable should work."""
    monkeypatch.setenv("LAUFBAND_ENABLE_FINGERPRINTS", "1")

    def graph_tasks():
        yield Task(id="A", fingerprint="v1")

    db = f"sqlite:///{tmp_path}/test.sqlite"
    lock = Lock(str(tmp_path / "test.lock"))

    # First run
    processed = []
    worker = Graphband(graph_tasks(), db=db, lock=lock)  # No explicit flag
    for task in worker:
        processed.append(task.id)

    assert processed == ["A"]

    # Second run with changed fingerprint
    def graph_tasks_v2():
        yield Task(id="A", fingerprint="v2")

    processed = []
    worker = Graphband(graph_tasks_v2(), db=db, lock=lock)  # No explicit flag
    for task in worker:
        processed.append(task.id)

    # Should re-execute A (fingerprints enabled via env var)
    assert processed == ["A"]


@pytest.mark.integration
def test_fingerprint_with_dependencies(tmp_path):
    """Fingerprint invalidation should work with task dependencies."""

    def graph_tasks():
        yield Task(id="A", fingerprint="v1")
        yield Task(id="B", dependencies={"A"}, fingerprint="v1")
        yield Task(id="C", dependencies={"B"}, fingerprint="v1")

    db = f"sqlite:///{tmp_path}/test.sqlite"
    lock = Lock(str(tmp_path / "test.lock"))

    # First run
    processed = []
    worker = Graphband(graph_tasks(), db=db, lock=lock, enable_fingerprints=True)
    for task in worker:
        processed.append(task.id)

    assert processed == ["A", "B", "C"]

    # Second run - change only C's fingerprint
    def graph_tasks_v2():
        yield Task(id="A", fingerprint="v1")  # Unchanged
        yield Task(id="B", dependencies={"A"}, fingerprint="v1")  # Unchanged
        yield Task(id="C", dependencies={"B"}, fingerprint="v2")  # Changed

    processed = []
    worker = Graphband(graph_tasks_v2(), db=db, lock=lock, enable_fingerprints=True)
    for task in worker:
        processed.append(task.id)

    # Should re-execute only C
    assert processed == ["C"]
