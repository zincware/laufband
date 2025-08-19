import contextlib
import os
import sqlite3
import typing as t
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

T_STATE = t.Literal["running", "failed", "completed", "died"]
# failed: the job failed with some exit code
# died: the python process was killed and the worker could not update the
#       state of the job. Detected by another worker updating the database


@dataclass
class GraphbandDB:
    """Graphband database interface for managing task progress and worker states.

    Supports both sequential tasks (Laufband) and DAG tasks (Graphband).

    Attributes
    ----------
    db_path : str | Path
        Path to the SQLite database file.
    worker : str
        Unique identifier for the worker, defaults to the process ID.
    heartbeat_timeout : int
        Timeout in seconds to mark jobs as 'died' if the worker has not been seen.
    max_died_retries : int
        Number of retries for jobs that are marked as 'died'.
    _worker_checked : bool
        Internal flag to check if the worker has been registered.
    """

    db_path: str | Path
    worker: str = field(default_factory=lambda: str(os.getpid()))
    heartbeat_timeout: int = 60
    max_died_retries: int = 0
    _worker_checked: bool = field(default=False, init=False)

    def __post_init__(self):
        if self.worker is None:
            raise ValueError("Worker name must be set before iterating.")

    def set_worker(self, worker_name: str):
        """Set the worker name and reset worker checking state."""
        self.worker = worker_name
        self._worker_checked = False

    def __len__(self):
        with self.connect() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM progress_table")
            count = cursor.fetchone()[0]
        return count

    def __iter__(self) -> Iterator[int]:
        """Iterate over the progress table, yielding job IDs.

        This will check if the worker is already registered and update the worker state.
        """
        if not self._worker_checked:
            with self.connect() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    SELECT worker FROM worker_table
                    WHERE worker = ?
                    """,
                    (self.worker,),
                )
                if cursor.fetchone() is not None:
                    raise ValueError(
                        f"Worker with identifier '{self.worker}' already exists."
                    )
                else:
                    self.update_worker(cursor)
                    self.mark_died(cursor)
                    conn.commit()
            self._worker_checked = True
        return self

    def update_worker(self, cursor: sqlite3.Cursor):
        # update if exists
        cursor.execute(
            """
            UPDATE worker_table
            SET last_seen = CURRENT_TIMESTAMP
            WHERE worker = ?
            """,
            (self.worker,),
        )
        # insert if not exists
        cursor.execute(
            """
            INSERT OR IGNORE INTO worker_table (worker)
            VALUES (?)
            """,
            (self.worker,),
        )

    def mark_died(self, cursor: sqlite3.Cursor):
        """Mark jobs as died if the worker has not been seen in
        the last `heartbeat_timeout` seconds."""
        cursor.execute(
            """
            UPDATE progress_table
            SET state = 'died'
            WHERE state = 'running' AND worker IN (
                SELECT worker FROM worker_table
                WHERE last_seen < datetime('now', ?)
            )
            """,
            (f"-{self.heartbeat_timeout} seconds",),
        )

    def __next__(self) -> str:
        """Get next available task ID for sequential processing."""
        with self.connect() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE progress_table
                SET state = 'running', worker = ?, count = count + 1
                WHERE task_id = (
                    SELECT task_id
                    FROM progress_table
                    WHERE (state IS NULL OR (state = 'died' AND count - 1 < ?))
                    ORDER BY task_id
                    LIMIT 1
                )
                RETURNING task_id
                """,
                (self.worker, self.max_died_retries),
            )
            row = cursor.fetchone()

            self.update_worker(cursor)
            self.mark_died(cursor)

            conn.commit()

            if row:
                return row[0]
            else:
                raise StopIteration

    def get_ready_task(self, graph, hash_fn: t.Callable[[t.Any], str]) -> Iterator[str]:
        """Get next ready task that has all dependencies completed."""
        with self.connect() as conn:
            cursor = conn.cursor()

            # Get all completed tasks and map them back to original nodes
            cursor.execute(
                "SELECT task_id FROM progress_table WHERE state = 'completed'"
            )
            completed_task_ids = {row[0] for row in cursor.fetchall()}

            # Process GraphTraversalProtocol
            for node, predecessors in graph:
                task_id = hash_fn(node)
                predecessor_task_ids = {hash_fn(pred) for pred in predecessors}

                if not predecessors or predecessor_task_ids.issubset(
                    completed_task_ids
                ):
                    # Try to claim this task
                    cursor.execute(
                        """
                        UPDATE progress_table
                        SET state = 'running', worker = ?, count = count + 1
                        WHERE task_id = ? AND
                        (
                            state = 'pending'
                            OR
                            (state = 'died' AND count - 1 < ?)
                        )
                        RETURNING task_id
                        """,
                        (self.worker, task_id, self.max_died_retries),
                    )
                    row = cursor.fetchone()
                    if row:
                        conn.commit()
                        yield row[0]
                        return

            self.update_worker(cursor)
            self.mark_died(cursor)
            conn.commit()
            return

    @contextlib.contextmanager
    def connect(self):
        conn = sqlite3.connect(self.db_path)
        try:
            yield conn
        finally:
            conn.close()

    def create(self, size: int):
        """Create database for sequential tasks (backwards compatibility)."""
        with self.connect() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE progress_table (
                    task_id TEXT PRIMARY KEY,
                    state TEXT,
                    worker TEXT,
                    count INTEGER DEFAULT 0
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS worker_table (
                    worker TEXT PRIMARY KEY,
                    last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # For sequential tasks, add all tasks to progress_table immediately
            # This maintains backward compatibility with old Laufband tests
            for i in range(size):
                cursor.execute(
                    """
                    INSERT OR IGNORE INTO progress_table (task_id, state, worker)
                    VALUES (?, ?, ?)
                    """,
                    (str(i), None, None),  # No initial state for sequential tasks
                )
            conn.commit()

    def create_empty(self):
        """Create empty database with tables."""
        with self.connect() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE progress_table (
                    task_id TEXT PRIMARY KEY,
                    state TEXT DEFAULT 'pending',
                    worker TEXT,
                    count INTEGER DEFAULT 0
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS dependencies (
                    task_id TEXT,
                    predecessor_id TEXT,
                    PRIMARY KEY (task_id, predecessor_id),
                    FOREIGN KEY (task_id) REFERENCES progress_table (task_id),
                    FOREIGN KEY (predecessor_id) REFERENCES progress_table (task_id)
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS worker_table (
                    worker TEXT PRIMARY KEY,
                    last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.commit()

    def add_task(self, task_id: str, predecessor_ids: set[str]):
        """Add a single task with its dependencies."""
        with self.connect() as conn:
            cursor = conn.cursor()
            # Only add dependencies - don't add to progress_table until task is claimed
            for predecessor_id in predecessor_ids:
                cursor.execute(
                    """
                    INSERT OR IGNORE INTO dependencies (task_id, predecessor_id)
                    VALUES (?, ?)
                    """,
                    (task_id, predecessor_id),
                )
            conn.commit()

    def create_from_graph(self, graph, hash_fn: t.Callable[[t.Any], str]):
        """Create database from a GraphTraversalProtocol."""
        with self.connect() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE progress_table (
                    task_id TEXT PRIMARY KEY,
                    state TEXT,
                    worker TEXT,
                    count INTEGER DEFAULT 0
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS dependencies (
                    task_id TEXT,
                    predecessor_id TEXT,
                    PRIMARY KEY (task_id, predecessor_id)
                )
            """)

            # Process GraphTraversalProtocol - only store dependencies
            for node, predecessors in graph:
                task_id = hash_fn(node)

                # Add dependencies
                for predecessor in predecessors:
                    predecessor_id = hash_fn(predecessor)
                    cursor.execute(
                        """
                        INSERT OR IGNORE INTO dependencies (task_id, predecessor_id)
                        VALUES (?, ?)
                        """,
                        (task_id, predecessor_id),
                    )

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS worker_table (
                    worker TEXT PRIMARY KEY,
                    last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.commit()

    def update_from_graph(self, graph, hash_fn: t.Callable[[t.Any], str]):
        """Update database with new tasks from GraphTraversalProtocol."""
        with self.connect() as conn:
            cursor = conn.cursor()
            # Check if progress_table exists, create if it doesn't
            cursor.execute("""
                SELECT name FROM sqlite_master WHERE type='table' AND name='progress_table'
            """)
            if not cursor.fetchone():
                # Table doesn't exist, create it
                cursor.execute("""
                    CREATE TABLE progress_table (
                        task_id TEXT PRIMARY KEY,
                        state TEXT DEFAULT 'pending',
                        worker TEXT,
                        count INTEGER DEFAULT 0
                    )
                """)
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS dependencies (
                        task_id TEXT,
                        predecessor_id TEXT,
                        PRIMARY KEY (task_id, predecessor_id),
                        FOREIGN KEY (task_id) REFERENCES progress_table (task_id),
                        FOREIGN KEY (predecessor_id) REFERENCES progress_table (task_id)
                    )
                """)
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS worker_table (
                        worker TEXT PRIMARY KEY,
                        last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)

            # Process GraphTraversalProtocol - only store dependencies
            for node, predecessors in graph:
                task_id = hash_fn(node)

                # Add dependencies
                for predecessor in predecessors:
                    predecessor_id = hash_fn(predecessor)
                    cursor.execute(
                        """
                        INSERT OR IGNORE INTO dependencies (task_id, predecessor_id)
                        VALUES (?, ?)
                        """,
                        (task_id, predecessor_id),
                    )
            conn.commit()

    def finalize(self, task_id: str, state: T_STATE = "completed"):
        with self.connect() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE progress_table
                SET state = ?
                WHERE task_id = ?
                """,
                (state, task_id),
            )
            conn.commit()

    def list_state(self, state: T_STATE) -> list[str]:
        with self.connect() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT task_id FROM progress_table
                WHERE state = ?
                """,
                (state,),
            )
            rows = cursor.fetchall()
        return [row[0] for row in rows]

    def get_worker(self, task_id: str) -> Optional[str]:
        with self.connect() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT worker FROM progress_table
                WHERE task_id = ?
                """,
                (task_id,),
            )
            row = cursor.fetchone()
        return row[0] if row else None

    def get_task_item(self, task_id: str) -> t.Any:
        """Get the original task item from task_id."""
        with self.connect() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT task_data FROM progress_table
                WHERE task_id = ?
                """,
                (task_id,),
            )
            row = cursor.fetchone()
        if row:
            return row[0].decode()
        return None

    def get_job_stats(self) -> dict[str, int]:
        """Get counts of jobs in each state"""
        stats = {}
        states: list[T_STATE] = ["running", "failed", "completed", "died"]

        with self.connect() as conn:
            cursor = conn.cursor()
            for state in states:
                cursor.execute(
                    "SELECT COUNT(*) FROM progress_table WHERE state = ?", (state,)
                )
                stats[state] = cursor.fetchone()[0]

        return stats

    def get_worker_info(self) -> list[dict]:
        """Get information about all workers with their job statistics"""
        with self.connect() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT
                    w.worker,
                    w.last_seen,
                    COUNT(CASE WHEN p.state = 'running' THEN 1 END) as active_jobs,
                    COUNT(CASE WHEN p.state = 'completed' THEN 1 END) as completed_jobs,
                    COUNT(CASE WHEN p.state = 'failed' THEN 1 END) as failed_jobs,
                    MAX(p.count) as max_retries
                FROM worker_table w
                LEFT JOIN progress_table p ON w.worker = p.worker
                GROUP BY w.worker, w.last_seen
                ORDER BY w.last_seen DESC
            """)

            workers = []
            for row in cursor.fetchall():
                (
                    worker,
                    last_seen,
                    active_jobs,
                    completed_jobs,
                    failed_jobs,
                    max_retries,
                ) = row
                workers.append(
                    {
                        "worker": worker,
                        "last_seen": last_seen,
                        "active_jobs": active_jobs or 0,
                        "completed_jobs": completed_jobs or 0,
                        "failed_jobs": failed_jobs or 0,
                        "processed_jobs": (completed_jobs or 0) + (failed_jobs or 0),
                        "max_retries": max_retries or 0,
                    }
                )

        return workers
