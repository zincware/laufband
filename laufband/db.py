import contextlib
import sqlite3
import typing as t
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

T_STATE = t.Literal["running", "pending", "failed", "completed"]


@dataclass
class LaufbandDB:
    db_path: str | Path
    worker: Optional[str] = None

    def __len__(self):
        with self.connect() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM progress_table")
            count = cursor.fetchone()[0]
        return count

    def __iter__(self) -> Iterator[int]:
        return self

    def __next__(self) -> int:
        with self.connect() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE progress_table
                SET state = 'running', worker = ?
                WHERE id = (
                    SELECT id
                    FROM progress_table
                    WHERE state = 'pending'
                    ORDER BY id
                    LIMIT 1
                )
                RETURNING id
                """,
                (self.worker,),
            )
            row = cursor.fetchone()
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
                    worker TEXT
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
