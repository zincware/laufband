import dataclasses
import typing as t

TaskTypeVar = t.TypeVar("TaskTypeVar")


@dataclasses.dataclass(frozen=True)
class Task(t.Generic[TaskTypeVar]):
    """
    Represents a task with associated data, dependencies, and requirements.

    Parameters
    ----------
    id : str
        Unique identifier for the task.
    data : Any
        Arbitrary data associated with the task.
    dependencies : set of str, optional
        Set of task IDs that this task depends on. Defaults to an empty set.
    requirements : set of str, optional
        Set of requirements or prerequisites for this task.
        Could be something like {"cpu", "gpu"}
    info: dict[str, t.Any]
        Additional information about the task.
    max_parallel_workers: int, default=1
        Maximum number of workers that can be assigned to this task.
    """

    id: str
    data: TaskTypeVar | None = None
    dependencies: set[str] = dataclasses.field(default_factory=set)
    requirements: set[str] = dataclasses.field(default_factory=set)
    info: dict[str, t.Any] = dataclasses.field(default_factory=dict)
    max_parallel_workers: int = 1
