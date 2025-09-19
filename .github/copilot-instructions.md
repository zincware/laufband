# Laufband: Embarrassingly parallel, embarrassingly simple!

Laufband is a Python library that enables parallel iteration over datasets from multiple processes using file-based locking and communication to ensure each item is processed exactly once. It includes both `Laufband` for simple parallel processing and `Graphband` for dependency-aware task processing.

**Always reference these instructions first and fallback to search or bash commands only when you encounter unexpected information that does not match the info here.**

## Working Effectively

### Initial Setup
- Install uv package manager: `pip install uv`
- Bootstrap and setup the development environment:
  - `uv sync` -- installs all dependencies, takes ~10 seconds. NEVER CANCEL.
- **Requirements**: Python 3.11+ (project uses modern Python features)

### Build and Test Commands
- **Build package**: `uv build` -- takes ~0.5 seconds, creates wheel and source distribution
- **Run tests**: `uv run pytest --cov --tb=short` -- takes ~40 seconds, runs 23 tests with 96% coverage. NEVER CANCEL. Set timeout to 2+ minutes.
- **Code formatting**: `uv run ruff format` -- takes ~0.05 seconds, formats all Python files
- **Code linting**: `uv run ruff check` -- takes ~0.05 seconds, checks code quality and style
- **Type checking**: `uv run mypy laufband/` -- takes ~1.5 seconds, has some known type errors but runs successfully

### Pre-commit Validation
**ALWAYS run these commands before committing to ensure CI passes:**
```bash
uv run ruff format .      # Format code
uv run ruff check .       # Check linting
uv run pytest --cov      # Run full test suite - NEVER CANCEL, takes ~40 seconds
```

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
- Tests use temporary directories (`tmp_path` fixture)
- Database files are created with full paths in test temp directories
- Mock scenarios test various failure modes and recovery patterns
- Tests marked with `@pytest.mark.human_reviewed` should not be modified by automated tools

### Performance Characteristics
- Setup time: ~10 seconds for fresh environment
- Build time: ~0.5 seconds
- Test time: ~40 seconds for full suite
- Linting time: ~0.05 seconds each for format/check
- CLI operations: Near-instant for status/monitoring

### Troubleshooting
- If database errors occur, ensure URL uses `sqlite:///` prefix
- If lock errors occur, ensure using `Lock()` objects not string paths
- If import errors occur, ensure running with `uv run` prefix
- If tests timeout, ensure proper timeout settings (2+ minutes for test suite)
- Type checking has known issues but doesn't block development

## Common Development Workflows

1. **Making changes**:
   ```bash
   # Make your code changes
   uv run ruff format .          # Format code
   uv run ruff check .           # Check linting  
   uv run pytest --cov          # Run tests - NEVER CANCEL
   ```

2. **Testing functionality**:
   ```bash
   # Create and run test script with validation scenarios above
   uv run python test_script.py
   uv run laufband status --db test.sqlite --lock test.lock
   ```

3. **Before committing**:
   ```bash
   uv run ruff format .
   uv run ruff check .
   uv run pytest --cov --tb=short   # Full test suite
   ```

**Remember**: The project uses modern Python tooling (uv, ruff) for fast development cycles. All build and test operations are very fast except the test suite which takes ~40 seconds but provides comprehensive coverage.