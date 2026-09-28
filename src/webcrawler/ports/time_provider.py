"""The clock port.

Every time here is injected rather than read at the point of use, so timeout predicates are
tested against exact epochs instead of a real clock. It is an ABC like every other port: the
system clock and a test fake both extend it.
"""

from abc import ABC, abstractmethod
from datetime import datetime


class TimeProviderFactory(ABC):
    """Reads the current UTC time. A test seam, not a runtime extension point."""

    @abstractmethod
    def now(self) -> datetime:
        """Return the current time.

        Returns:
            datetime: The current instant as a timezone-aware UTC `datetime`, the only representation that crosses a port.

        Synchronous: reads the clock only, no I/O.
        """
