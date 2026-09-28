"""The system clock, as a `TimeProviderFactory`.

Every time in this project is UTC, and the clock is a port so the timeout
predicates can be tested against exact epochs and a schedule does not depend on
where the crawl runs.
"""

from datetime import datetime, timezone

from webcrawler.ports.time_provider import TimeProviderFactory


class SystemTimeProvider(TimeProviderFactory):
    """Reports the current instant, read from the operating system's clock.

    The port is extended rather than merely matched, so this is the process's
    only clock: a subclass would be a second opinion about time, not a
    substitution.
    """

    def now(self) -> datetime:
        """Return the current UTC instant.

        `now(timezone.utc)`, not `utcnow`: a naive instant compares wrongly.

        Returns:
        datetime: The current instant as a timezone-aware UTC `datetime`,
        the only time representation that crosses a port.

        Synchronous: reads the clock only, no I/O.
        """
        return datetime.now(timezone.utc)
