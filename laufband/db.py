import contextlib
import os
import sqlite3
import typing as t
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

T_STATE = t.Literal["running", "pending", "failed", "completed", "died"]
# failed: the job failed with some exit code
# died: the python process was killed and the worker could not update the
#       state of the job. Detected by another worker updating the database


@dataclass
class LaufbandDB:
    """Laufband database interface for managing job progress and worker states.

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

    def __next__(self) -> int:
        with self.connect() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE progress_table
                SET state = 'running', worker = ?, count = count + 1
                WHERE id = (
                    SELECT id
                    FROM progress_table
                    WHERE
                        (
                            state = 'pending'
                        )
                        OR
                        (
                            state = 'died' AND count - 1 < ?
                        )
                    ORDER BY id
                    LIMIT 1
                )
                RETURNING id
                """,
                (self.worker, self.max_died_retries),
            )
            row = cursor.fetchone()

            self.update_worker(cursor)
            self.mark_died(cursor)

            conn.commit()

            if row:
                return row[0] - 1  # Adjust for 0-based index
            else:
                raise StopIteration

    @contextlib.contextmanager
    def connect(self):
        conn = sqlite3.connect(self.db_path)
        try:
            yield conn
        finally:
            conn.close()

    def create(self, size: int):
        with self.connect() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE progress_table (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    state TEXT DEFAULT 'pending',
                    worker TEXT,
                    count INTEGER DEFAULT 0
                )
            """)
            for _ in range(size):
                cursor.execute(
                    """
                    INSERT INTO progress_table (state, worker)
                    VALUES (?, ?)
                    """,
                    ("pending", None),
                )
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS worker_table (
                    worker TEXT PRIMARY KEY,
                    last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.commit()

    def finalize(self, idx: int, state: T_STATE = "completed"):
        with self.connect() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE progress_table
                SET state = ?
                WHERE id = ?
                """,
                (state, idx + 1),  # Adjust for 0-based index
            )
            conn.commit()

    def list_state(self, state: T_STATE) -> list[int]:
        with self.connect() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT id FROM progress_table
                WHERE state = ?
                """,
                (state,),
            )
            rows = cursor.fetchall()
        return [row[0] - 1 for row in rows]

    def get_worker(self, idx: int) -> Optional[str]:
        with self.connect() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT worker FROM progress_table
                WHERE id = ?
                """,
                (idx + 1,),  # Adjust for 0-based index
            )
            row = cursor.fetchone()
        return row[0] if row else None
