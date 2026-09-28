"""The clock port (goal.md:11-12).

Every time in this project is injected rather than read at the point of use, so
the timeout predicates of `goal.md:33-51` can be tested against exact epochs
instead of a real clock, and so a crawl's schedule does not depend on where it
runs. It is an ABC like every other port: the system-clock implementation and a
test fake both extend it, so the port is a nominal base class.
"""

from abc import ABC, abstractmethod
from datetime import datetime


class TimeProviderFactory(ABC):
    """Reads the current UTC time. A test seam, not a runtime extension point."""

    @abstractmethod
    def now(self) -> datetime:
        """Return the current time.

        Returns:
            datetime: The current instant as a timezone-aware UTC `datetime`,
                the only time representation that crosses a port.

        Synchronous: reads the clock only, no I/O (goal.md:14).
        """
