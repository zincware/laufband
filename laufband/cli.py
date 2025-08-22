import datetime
import time
from pathlib import Path
from typing import Dict, List

import typer
from flufl.lock import Lock
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

from laufband.db import TaskStatusEnum
from laufband.monitor import Monitor

app = typer.Typer(help="Laufband CLI Tool")
ACTIVITY_TIMEOUT_SECONDS = 60  # 1 minute # can be handled better via the db expired directly.


class LaufbandStatusDisplay:
    def __init__(self, db_path: str | Path, lock_path: str | Path):
        self.db_path = Path(db_path)
        self.lock_path = Path(lock_path)
        self.console = Console()
        self.monitor = None

    def _ensure_monitor_connection(self):
        """Ensure monitor connection exists, create if needed"""
        if self.monitor is None and self.db_path.exists():
            try:
                self.monitor = Monitor(
                    lock=Lock(str(self.lock_path)), db=f"sqlite:///{self.db_path}"
                )
            except Exception:
                self.monitor = None

    def get_task_stats(self) -> Dict[str, int] | None:
        """Get counts of tasks in each state"""
        self._ensure_monitor_connection()
        if self.monitor is None:
            return None

        try:
            stats = {}
            for status in TaskStatusEnum:
                tasks = self.monitor.get_tasks(status)
                stats[status.value] = len(tasks)
            return stats
        except Exception:
            self.monitor = None
            return None

    def get_worker_info(self) -> List[Dict] | None:
        """Get information about all workers"""
        self._ensure_monitor_connection()
        if self.monitor is None:
            return None

        try:
            workers = self.monitor.get_workers()
            worker_info = []

            for worker in workers:
                # Count running tasks for this worker
                running_tasks = len(worker.running_tasks)

                worker_info.append(
                    {
                        "worker_id": worker.id,
                        "status": worker.status.value,
                        "last_heartbeat": worker.last_heartbeat.strftime(
                            "%Y-%m-%d %H:%M:%S"
                        ),
                        "running_tasks": running_tasks,
                        "hostname": worker.hostname or "Unknown",
                        "pid": worker.pid or 0,
                        "labels": ", ".join(worker.labels) if worker.labels else "None",
                    }
                )

            return worker_info
        except Exception:
            self.monitor = None
            return None

    def get_total_tasks(self) -> int | None:
        """Get total tasks from workflow"""
        self._ensure_monitor_connection()
        if self.monitor is None:
            return None

        try:
            workflow = self.monitor.get_workflow()
            return workflow.total_tasks
        except Exception:
            return None

    def create_progress_bar(self, stats: Dict[str, int]) -> Panel:
        """Create overall progress bar"""
        total_from_workflow = self.get_total_tasks()

        if total_from_workflow is not None:
            total = total_from_workflow
            progress = Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TaskProgressColumn(),
                TimeRemainingColumn(),
            )
            completed = stats.get("completed", 0)
            failed = stats.get("failed", 0)
            progress.add_task(
                f"Overall Progress ({completed + failed}/{total} complete)",
                total=total,
                completed=completed + failed,
            )
        else:
            total_discovered = sum(stats.values())
            completed = stats.get("completed", 0)
            failed = stats.get("failed", 0)
            progress = Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TaskProgressColumn(),
            )
            progress.add_task(
                f"Progress ({completed + failed}/{total_discovered} discovered)",
                total=None,
            )

        return Panel(progress, title="Overall Progress")

    def create_stats_table(self, stats: Dict[str, int]) -> Table:
        """Create task statistics table"""
        table = Table(title="Task Statistics")
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
                "failed": "red",
                "abandoned": "red bold",
                "cancelled": "orange1",
                "blocked": "blue",
                "killed": "red bold",
            }.get(state, "white")

            table.add_row(
                Text(state.title(), style=state_color), str(count), percentage
            )

        return table

    def create_workers_table(self, workers: List[Dict]) -> Table:
        """Create workers information table"""
        table = Table(title="Workers")
        table.add_column("Worker ID", style="cyan")
        table.add_column("Status", style="yellow")
        table.add_column("Last Heartbeat", style="white")
        table.add_column("Running Tasks", justify="right", style="magenta")
        table.add_column("Hostname", style="green")
        table.add_column("PID", justify="right", style="blue")
        table.add_column("Labels", style="dim")

        for worker in workers:
            # Determine if worker heartbeat is expired
            try:
                last_heartbeat_str = worker["last_heartbeat"]
                last_heartbeat = datetime.datetime.strptime(
                    last_heartbeat_str, "%Y-%m-%d %H:%M:%S"
                )
                now = datetime.datetime.now()
                time_diff = (now - last_heartbeat).total_seconds()

                status = worker["status"]
                if time_diff > ACTIVITY_TIMEOUT_SECONDS:
                    status = f"{status} (stale)"
                    status_color = "red"
                else:
                    status_color = {
                        "idle": "green",
                        "busy": "yellow",
                        "offline": "red",
                        "killed": "red bold",
                    }.get(worker["status"], "white")

            except Exception:
                status = worker["status"]
                status_color = "yellow"

            table.add_row(
                worker["worker_id"],
                Text(status, style=status_color),
                worker["last_heartbeat"],
                str(worker["running_tasks"]),
                worker["hostname"],
                str(worker["pid"]),
                worker["labels"],
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
        stats = self.get_task_stats()
        workers = self.get_worker_info()

        if stats is None or workers is None:
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


@app.command()
def status(
    db: str = typer.Option(
        "laufband.sqlite", "--db", help="Path to the laufband database file"
    ),
    lock: str = typer.Option(
        "laufband.lock", "--lock", help="Path to the laufband lock file"
    ),
):
    """Show current laufband status"""
    display = LaufbandStatusDisplay(db, lock)
    display.display_status()


@app.command()
def watch(
    db: str = typer.Option(
        "laufband.sqlite", "--db", help="Path to the laufband database file"
    ),
    lock: str = typer.Option(
        "laufband.lock", "--lock", help="Path to the laufband lock file"
    ),
    interval: float = typer.Option(
        2.0, "--interval", help="Update interval in seconds"
    ),
):
    """Watch laufband status in real-time"""
    display = LaufbandStatusDisplay(db, lock)
    console = Console()

    try:
        with Live(console=console, refresh_per_second=1 / interval) as live:
            while True:
                stats = display.get_task_stats()
                workers = display.get_worker_info()

                if stats is None or workers is None:
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

                time.sleep(interval)
    except KeyboardInterrupt:
        console.print("\n[yellow]Stopped watching.[/yellow]")


if __name__ == "__main__":
    app()
