"""The default politeness policy: no delay at all.

`goal.md:140` asks for a no-op implementation, and makes it the
default wiring, so an operator who configures nothing crawls as fast as the host
allows. The wait is a constant rather than a constructor argument because an
instance that could be told to wait would be a second policy: the delayed one
already exists and is chosen by configuration instead (composition over
inheritance, `goal.md:8`).
"""

from webcrawler.ports.politeness_policy import PolitenessPolicy

NO_WAIT_MS: int = 0

class NoOpPolitenessPolicy(PolitenessPolicy):
    """Always answers "call now", for a crawl that nobody asked to slow down.

    The class extends the `PolitenessPolicy` ABC and is extended by nothing,
    which is what keeps the worker's politeness decision substitutable
    (`goal.md:8`).
    """

    def before_fetch(self) -> int:
        """Report that no wait is configured, whatever the worker's threshold.

        `0` is the port's "call now" (`goal.md:140`), so the worker takes its
        sleep branch for every URL and a crawl proceeds at full speed.

        Returns:
        int: Always `NO_WAIT_MS`, the milliseconds to wait before the next
        request.

        Synchronous: a constant, no I/O.
        """
        return NO_WAIT_MS
