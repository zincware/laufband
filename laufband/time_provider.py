"""Time abstraction for testability."""

from abc import ABC, abstractmethod
from datetime import datetime, timedelta


class TimeProvider(ABC):
    """Abstract time provider for dependency injection."""

    @abstractmethod
    def now(self) -> datetime:
        """Get current datetime."""
        pass

    @abstractmethod
    def sleep(self, seconds: float) -> None:
        """Sleep for given seconds."""
        pass


class RealTimeProvider(TimeProvider):
    """Production time provider using real system time."""

    def now(self) -> datetime:
        return datetime.now()

    def sleep(self, seconds: float) -> None:
        import time

        time.sleep(seconds)


class MockTimeProvider(TimeProvider):
    """Controllable time provider for testing.

    Allows manual time advancement without actual waiting.
    """

    def __init__(self, start_time: datetime | None = None):
        self._current_time = start_time or datetime(2024, 1, 1, 12, 0, 0)

    def now(self) -> datetime:
        return self._current_time

    def advance(self, seconds: float) -> None:
        """Manually advance time by given seconds."""
        self._current_time += timedelta(seconds=seconds)

    def sleep(self, seconds: float) -> None:
        """Mock sleep that just advances time instantly."""
        self.advance(seconds)
