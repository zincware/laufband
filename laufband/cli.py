import argparse
import datetime
from pathlib import Path
from typing import Dict, List

from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeRemainingColumn,
)
from rich.table import Table
from rich.text import Text

from laufband.db import LaufbandDB

ACTIVITY_TIMEOUT_SECONDS = 300  # 5 minutes


class LaufbandStatusDisplay:
    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)
        self.console = Console()
        self.db = None

    def _ensure_db_connection(self):
        """Ensure database connection exists, create if needed"""
        if self.db is None and self.db_path.exists():
            self.db = LaufbandDB(self.db_path, worker="cli_viewer")

    def get_job_stats(self) -> Dict[str, int] | None:
        """Get counts of jobs in each state"""
        self._ensure_db_connection()
        if self.db is None:
            return None

        try:
            return self.db.get_job_stats()
        except Exception:
            # Database was removed or corrupted, reset connection
            self.db = None
            return None

    def get_worker_info(self) -> List[Dict] | None:
        """Get information about all workers"""
        self._ensure_db_connection()
        if self.db is None:
            return None

        try:
            return self.db.get_worker_info()
        except Exception:
            # Database was removed or corrupted, reset connection
            self.db = None
            return None

    def create_progress_bar(self, stats: Dict[str, int]) -> Panel:
        """Create overall progress bar"""
        total = sum(stats.values())
        completed = stats["completed"]
        failed = stats["failed"]

        progress = Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            TimeRemainingColumn(),
        )

        progress.add_task(
            f"Overall Progress ({completed + failed}/{total} complete)",
            total=total,
            completed=completed + failed,
        )

        return Panel(progress, title="Overall Progress")

    def create_stats_table(self, stats: Dict[str, int]) -> Table:
        """Create job statistics table"""
        table = Table(title="Job Statistics")
        table.add_column("State", style="cyan")
        table.add_column("Count", justify="right", style="magenta")
        table.add_column("Percentage", justify="right", style="green")

        total = sum(stats.values())

        for state, count in stats.items():
            percentage = f"{(count / total) * 100:.1f}%" if total > 0 else "0.0%"

            # Color code by state
            state_color = {
                "completed": "green",
                "running": "yellow",
                "pending": "blue",
                "failed": "red",
                "died": "red bold",
            }.get(state, "white")

            table.add_row(
                Text(state.title(), style=state_color), str(count), percentage
            )

        return table

    def create_workers_table(self, workers: List[Dict]) -> Table:
        """Create workers information table"""
        table = Table(title="Workers")
        table.add_column("Worker ID", style="cyan")
        table.add_column("Last Seen", style="yellow")
        table.add_column("Active Jobs", justify="right", style="magenta")
        table.add_column("Processed", justify="right", style="green")
        table.add_column("Max Retries", justify="right", style="blue")
        table.add_column("Status", style="green")

        for worker in workers:
            # Determine if worker is active (seen in last 5 minutes)
            try:
                # SQLite CURRENT_TIMESTAMP format: '2024-01-01 12:00:00'
                last_seen_str = worker["last_seen"]
                if last_seen_str:
                    # Parse SQLite timestamp format
                    last_seen = datetime.datetime.strptime(
                        last_seen_str, "%Y-%m-%d %H:%M:%S"
                    )
                    # Assume UTC since SQLite CURRENT_TIMESTAMP is UTC
                    last_seen = last_seen.replace(tzinfo=datetime.timezone.utc)
                    now = datetime.datetime.now(datetime.timezone.utc)
                    time_diff = (now - last_seen).total_seconds()
                    status = (
                        "Active" if time_diff < ACTIVITY_TIMEOUT_SECONDS else "Inactive"
                    )
                    status_color = "green" if status == "Active" else "red"
                else:
                    status = "Unknown"
                    status_color = "yellow"
            except Exception:
                status = "Unknown"
                status_color = "yellow"

            table.add_row(
                worker["worker"],
                worker["last_seen"],
                str(worker["active_jobs"]),
                str(worker["processed_jobs"]),
                str(worker["max_retries"]),
                Text(status, style=status_color),
            )

        return table

    def create_waiting_panel(self) -> Panel:
        """Create panel for when database doesn't exist"""
        return Panel(
            f"[yellow]Waiting for database file '{self.db_path}' "
            + "to be created...[/yellow]\n"
            + "[dim]Start a laufband process to create the database.[/dim]",
            title="Waiting for Database",
        )

    def display_status(self):
        """Display the current status"""
        stats = self.get_job_stats()
        workers = self.get_worker_info()

        if stats is None or workers is None:
            # Database doesn't exist yet
            self.console.print(self.create_waiting_panel())
            return

        # Create layout
        layout = Layout()
        layout.split_column(Layout(name="progress", size=5), Layout(name="tables"))

        layout["tables"].split_row(Layout(name="stats"), Layout(name="workers"))

        # Add content
        layout["progress"].update(self.create_progress_bar(stats))
        layout["stats"].update(Panel(self.create_stats_table(stats)))
        layout["workers"].update(Panel(self.create_workers_table(workers)))

        self.console.print(layout)


def main() -> None:
    parser = argparse.ArgumentParser(description="Laufband CLI Tool")
    parser.add_argument("--version", action="version", version="Laufband CLI 1.0")
    parser.add_argument(
        "--db",
        type=str,
        default="laufband.sqlite",
        help="Path to the laufband database file (default: laufband.sqlite)",
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Status command
    status_parser = subparsers.add_parser("status", help="Show current laufband status")
    status_parser.add_argument(
        "--db", type=str, help="Path to the laufband database file"
    )

    # Watch command
    watch_parser = subparsers.add_parser(
        "watch", help="Watch laufband status in real-time"
    )
    watch_parser.add_argument(
        "--db", type=str, help="Path to the laufband database file"
    )
    watch_parser.add_argument(
        "--interval",
        type=float,
        default=2.0,
        help="Update interval in seconds (default: 2.0)",
    )

    args = parser.parse_args()

    # Default to status if no command given
    if args.command is None:
        args.command = "status"

    # Determine database path
    db_path = getattr(args, "db", None) or "laufband.sqlite"

    display = LaufbandStatusDisplay(db_path)

    if args.command == "status":
        display.display_status()
    elif args.command == "watch":
        import time

        console = Console()
        try:
            with Live(console=console, refresh_per_second=1 / args.interval) as live:
                while True:
                    stats = display.get_job_stats()
                    workers = display.get_worker_info()

                    if stats is None or workers is None:
                        # Database doesn't exist yet
                        live.update(display.create_waiting_panel())
                    else:
                        # Create layout
                        layout = Layout()
                        layout.split_column(
                            Layout(name="progress", size=5), Layout(name="tables")
                        )

                        layout["tables"].split_row(
                            Layout(name="stats"), Layout(name="workers")
                        )

                        # Add content
                        layout["progress"].update(display.create_progress_bar(stats))
                        layout["stats"].update(Panel(display.create_stats_table(stats)))
                        layout["workers"].update(
                            Panel(display.create_workers_table(workers))
                        )

                        live.update(layout)

                    time.sleep(args.interval)
        except KeyboardInterrupt:
            console.print("\n[yellow]Stopped watching.[/yellow]")


if __name__ == "__main__":
    main()
