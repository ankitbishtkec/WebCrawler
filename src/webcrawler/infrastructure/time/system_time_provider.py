"""The system clock, as a `TimeProviderFactory`.

`goal.md:25` puts every time in this project in UTC, and makes the
clock an injected port so the timeout predicates of `goal.md:76-91` can be
tested against exact epochs instead of a real clock, and so a crawl's schedule
does not depend on where it runs.

The port is extended rather than merely matched: the ABC is the single
definition of the clock seam. This is the only clock the
process will ever use, so a subclass of it would be a second opinion about
time rather than a substitution.
"""

from datetime import datetime, timezone

from webcrawler.ports.time_provider import TimeProviderFactory

class SystemTimeProvider(TimeProviderFactory):
    """Reports the current instant, read from the operating system's clock."""

    def now(self) -> datetime:
        """Return the current UTC instant.

        `datetime.now(timezone.utc)` rather than `utcnow` because a naive
        instant would compare wrongly against the timezone-aware instants the
        repository binds (`goal.md:25`).

        Returns:
        datetime: The current instant as a timezone-aware UTC `datetime`,
        the only time representation that crosses a port.

        Synchronous: reads the clock only, no I/O (goal.md:14).
        """
        return datetime.now(timezone.utc)
