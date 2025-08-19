import contextlib
import os
import sqlite3
import threading
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
        With the background heartbeat thread updating regularly, defaults to 30 seconds.
        No longer needs to account for long iteration times.
    heartbeat_interval : float
        Interval in seconds between heartbeat updates. Defaults to 10.0 seconds.
    max_died_retries : int
        Number of retries for jobs that are marked as 'died'.
    _worker_checked : bool
        Internal flag to check if the worker has been registered.
    """

    db_path: str | Path
    worker: str = field(default_factory=lambda: str(os.getpid()))
    heartbeat_timeout: int = 30
    heartbeat_interval: float = 10.0
    max_died_retries: int = 0
    _worker_checked: bool = field(default=False, init=False)
    _heartbeat_thread: threading.Thread | None = field(default=None, init=False)
    _heartbeat_stop_event: threading.Event = field(
        default_factory=threading.Event, init=False
    )
    _heartbeat_active: bool = field(default=False, init=False)

    def __post_init__(self):
        if self.worker is None:
            raise ValueError("Worker name must be set before iterating.")

    def _heartbeat_loop(self):
        """Background thread updating worker heartbeat and marking died workers."""
        while not self._heartbeat_stop_event.wait(timeout=self.heartbeat_interval):
            try:
                with self.connect() as conn:
                    cursor = conn.cursor()
                    self.update_worker(cursor)
                    self.mark_died(cursor)
                    conn.commit()
            except Exception:
                # Continue running even if heartbeat update fails
                pass

    def start_heartbeat(self):
        """Start the background heartbeat thread."""
        if not self._heartbeat_active:
            self._heartbeat_active = True
            self._heartbeat_stop_event.clear()
            self._heartbeat_thread = threading.Thread(
                target=self._heartbeat_loop,
                daemon=True,
                name=f"heartbeat-{self.worker}",
            )
            self._heartbeat_thread.start()

    def stop_heartbeat(self):
        """Stop the background heartbeat thread."""
        if self._heartbeat_active:
            self._heartbeat_stop_event.set()
            if self._heartbeat_thread and self._heartbeat_thread.is_alive():
                self._heartbeat_thread.join(timeout=1.0)
            self._heartbeat_active = False

    def __del__(self):
        """Ensure heartbeat thread is stopped when object is destroyed."""
        self.stop_heartbeat()

    def set_metadata(self, key: str, value: t.Any):
        """Set a metadata key-value pair."""
        with self.connect() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )
            """
            )
            cursor.execute(
                "INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)",
                (key, str(value)),
            )
            conn.commit()

    def get_metadata(self, key: str) -> Optional[str]:
        """Get a metadata value by key."""
        with self.connect() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("SELECT value FROM metadata WHERE key = ?", (key,))
                row = cursor.fetchone()
                return row[0] if row else None
            except sqlite3.OperationalError as e:
                if "no such table" in str(e):
                    return None
                raise

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

    def __iter__(self) -> Iterator[str]:
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
            # Start the background heartbeat thread
            self.start_heartbeat()
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

            conn.commit()

            if row:
                return row[0]
            else:
                raise StopIteration

    def get_ready_task(self, graph, hash_fn: t.Callable[[t.Any], str]) -> Iterator[str]:
        """Get next ready task that has all dependencies completed."""
        # Ensure heartbeat thread is running
        if not self._heartbeat_active:
            self.start_heartbeat()
            
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
                        # Register worker when claiming graph tasks
                        self.update_worker(cursor)
                        conn.commit()
                        yield row[0]
                        return

            conn.commit()
            return

    @contextlib.contextmanager
    def connect(self):
        conn = sqlite3.connect(self.db_path)
        try:
            yield conn
        finally:
            conn.close()

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

    def get_job_stats(self) -> dict[str, int]:
        """Get counts of jobs in each state"""
        stats = {}
        states: list[str] = ["running", "failed", "completed", "died", "pending"]

        with self.connect() as conn:
            cursor = conn.cursor()
            for state in states:
                if state == "pending":
                    cursor.execute(
                        "SELECT COUNT(*) FROM progress_table "
                        "WHERE state = 'pending' OR state IS NULL"
                    )
                else:
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

    def claim_task(self, task_id: str) -> str | None:
        """Claim a task by setting it as running for this worker.

        Returns the task_id if successfully claimed, None otherwise.
        """
        # Ensure heartbeat thread is running
        if not self._heartbeat_active:
            self.start_heartbeat()
            
        with self.connect() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT OR IGNORE INTO progress_table (task_id, state, worker, count)
                VALUES (?, 'running', ?, 1)
                RETURNING task_id
                """,
                (task_id, self.worker),
            )
            row = cursor.fetchone()
            if row:
                # Register worker when claiming tasks
                self.update_worker(cursor)
                conn.commit()
                return row[0]
            return None

    def find_ready_died_task(self) -> str | None:
        """Find a died task that has all dependencies completed and can be retried."""
        with self.connect() as conn:
            cursor = conn.cursor()
            # Find tasks that are died and have all dependencies completed
            cursor.execute(
                """
                SELECT pt.task_id
                FROM progress_table pt
                WHERE pt.state = 'died' AND pt.count - 1 < ?
                AND NOT EXISTS (
                    SELECT 1 FROM dependencies d
                    JOIN progress_table dep_pt ON d.predecessor_id = dep_pt.task_id
                    WHERE d.task_id = pt.task_id AND dep_pt.state != 'completed'
                )
                LIMIT 1
                """,
                (self.max_died_retries,),
            )
            row = cursor.fetchone()
            if row:
                return row[0]
            return None

    def claim_died_task(self, task_id: str) -> str | None:
        """Try to claim a died task by updating it to running state.

        Returns the task_id if successfully claimed, None otherwise.
        """
        with self.connect() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE progress_table
                SET state = 'running', worker = ?, count = count + 1
                WHERE task_id = ? AND state = 'died' AND count - 1 < ?
                RETURNING task_id
                """,
                (self.worker, task_id, self.max_died_retries),
            )
            row = cursor.fetchone()
            if row:
                conn.commit()
                return row[0]
            return None
