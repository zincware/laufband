# Laufband: Embarrassingly parallel, embarrassingly simple!

Laufband is a Python library that enables parallel iteration over datasets from multiple processes using (file-base) locking and communication to ensure each item is processed exactly once. It includes both `Laufband` for simple parallel processing and `Graphband` for dependency-aware task processing.

**Always reference these instructions first and fallback to search or bash commands only when you encounter unexpected information that does not match the info here.**

## Working Effectively

This is a new application and you must not consider migrations or backwards compatibility.
Design all new features with maintainability and performance in mind.
Use KISS, DRY, SOLID and YAGNI principles.
When refactoring, you can break backwards compatibility.
Always consider a better design approach compared to the existing one.
Consider multiple approaches, review them against the principles above, the existing methods and the overall architecture - and choose the best one.
When in doubt, ask for a review of your design approach before implementing it.

### Build and Test Commands
- **Run tests**: `uv run pytest --cov --tb=short`
- **Code formatting and linting**: `uvx prek run --all-files` formats and lints all files

### Pre-commit Validation
**ALWAYS run tests and code formatting before committing to ensure CI passes:**

### CLI Tool Usage
The project includes a CLI tool `laufband` with two main commands:

- **Status**: `uv run laufband status [--db DATABASE] [--lock LOCKFILE]`
  - Shows current task statistics and worker information
  - Default database: `laufband.sqlite`, default lock: `laufband.lock`

- **Watch**: `uv run laufband watch [--db DATABASE] [--lock LOCKFILE] [--interval SECONDS]`
  - Real-time monitoring of task progress
  - Default interval: 2.0 seconds
  - Press Ctrl+C to exit

## Validation Scenarios

**CRITICAL**: Always test functionality after making changes by running complete user scenarios:

### Basic Laufband Test
```python
from laufband import Laufband
from flufl.lock import Lock
import json
from pathlib import Path

# Create test data
data = list(range(5))
output_file = Path("output.json")
output_file.write_text(json.dumps({"processed_data": []}))

# Create worker with proper database URL and Lock object
worker = Laufband(data, db="sqlite:///test.sqlite", lock=Lock("test.lock"))

for item in worker:
    # Process item with shared resource access
    with worker.lock:
        file_content = json.loads(output_file.read_text())
        file_content["processed_data"].append(f"processed_{item}")
        output_file.write_text(json.dumps(file_content))

# Verify: Check that all items were processed
result = json.loads(output_file.read_text())
assert len(result["processed_data"]) == 5
```

### Graphband Test (Dependency-Aware Tasks)
```python
from laufband import Graphband, Task
from flufl.lock import Lock


def create_tasks():
    yield Task(id="A", data="task_a", dependencies=set())
    yield Task(id="B", data="task_b", dependencies={"A"})
    yield Task(id="C", data="task_c", dependencies={"A"})
    yield Task(id="D", data="task_d", dependencies={"B", "C"})


worker = Graphband(create_tasks(), db="sqlite:///graph.sqlite", lock=Lock("graph.lock"))

processed_tasks = []
for task in worker:
    processed_tasks.append(task.id)

# Verify dependency order was respected
assert processed_tasks.index("A") < processed_tasks.index("B")
assert processed_tasks.index("A") < processed_tasks.index("C")
```

### CLI Monitoring Test
After running either test above:
```bash
uv run laufband status --db test.sqlite --lock test.lock
# Should show completed tasks and worker statistics
```

## Development Guidelines

### Key Code Locations
- **Core library**: `laufband/` directory
  - `laufband.py` - Simple parallel processing
  - `graphband.py` - Dependency-aware task processing
  - `cli.py` - Command-line interface
  - `db.py` - Database models and operations
  - `monitor.py` - Monitoring and statistics
  - `task.py` - Task data structures

- **Tests**: `tests/` directory
  - `test_laufband.py` - Basic functionality tests
  - `test_graphband.py` - Graph-based task tests
  - `test_monitor.py` - Monitoring functionality tests

### Important Implementation Details
- **Database URLs**: Always use `sqlite:///path/to/file.sqlite` format (note the three slashes)
- **Lock Objects**: Import and use `from flufl.lock import Lock`, pass `Lock("path")` not string paths
- **Context Managers**: Use `with worker.lock:` for thread-safe operations
- **Task Dependencies**: In Graphband, dependencies are sets of task IDs that must complete first

### Common Patterns
- **Thread-safe file access**: Always use `with worker.lock:` when modifying shared resources
- **Progress monitoring**: All workers show progress bars via tqdm integration
- **Database cleanup**: Test databases are automatically cleaned up in temp directories
- **Error handling**: Tasks can fail gracefully, use `.close()` method for clean exits

### Testing Strategy

The test suite is organized into **unit tests** and **integration tests** for optimal speed and maintainability:

#### Test Organization
- `tests/unit/` - Fast unit tests (<1s total, 32 tests)
  - No multiprocessing, no time delays, no file I/O
  - Test business logic, models, and algorithms in isolation
  - Use `MockTimeProvider` for instant time control
  - Use factories for consistent test data

- `tests/integration/` - Integration tests (34 tests)
  - Real multiprocessing, database I/O, and coordination
  - Test end-to-end scenarios and worker interactions
  - Use polling helpers to avoid fixed sleep delays
  - Tests marked with `@pytest.mark.human_reviewed` should not be modified by automated tools

#### Test Fixtures and Factories
Available in `tests/conftest.py`:
- `mock_time` - Controllable time provider (advance time instantly)
- `db_engine` / `db_session` - In-memory database with auto-rollback
- `workflow_factory` - Create test workflows
- `worker_factory` - Create test workers (auto-incremented IDs)
- `task_factory` - Create test tasks with various states
- `wait_for_condition` - Polling utility for async operations
- `db_wait_helpers` - Wait for database state changes

#### Running Tests
```bash
# Fast unit tests only (development)
uv run pytest -m unit  # ~0.3s

# All tests with coverage (pre-commit)
uv run pytest --cov --tb=short  # ~50s

# Integration tests only
uv run pytest tests/integration/  # ~50s

# Specific test file
uv run pytest tests/unit/test_heartbeat_logic.py -v
```

#### Writing New Tests

**For Business Logic (Unit Tests)**:
```python
@pytest.mark.unit
def test_heartbeat_expiration(worker_factory, mock_time):
    """Test heartbeat logic without real delays."""
    worker = worker_factory(heartbeat_timeout=5)

    # Instantly advance time
    mock_time.advance(6)

    # Test logic
    assert worker.is_heartbeat_expired(mock_time)
```

**For End-to-End Scenarios (Integration Tests)**:
```python
@pytest.mark.integration
def test_worker_coordination(tmp_path, db_wait_helpers):
    """Test real multiprocessing scenario."""
    proc = multiprocessing.Process(...)
    proc.start()

    # Use polling instead of sleep
    db_wait_helpers.wait_for_task_count(expected=5, timeout=3)

    proc.join()
```

#### Testable Business Logic
Core logic extracted to `laufband/worker_logic.py`:
- `check_and_mark_expired_workers()` - Heartbeat monitoring
- `should_retry_task()` - Retry policy decisions
- `update_worker_heartbeat()` - Heartbeat updates
- `create_worker_entry()` - Worker initialization

These functions accept `TimeProvider` for controllable time in tests.

### Troubleshooting
- If import errors occur, ensure running with `uv run` prefix
- If tests timeout, ensure proper timeout settings (2+ minutes for test suite)
- Unit test failures: Check factory usage and mock_time advancement
- Integration test failures: Check for race conditions, use polling helpers
