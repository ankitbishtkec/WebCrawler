"""The default politeness policy: no delay at all.

`goal.md:140` asks for a no-op implementation, and makes it the
default wiring, so an operator who configures nothing crawls as fast as the host
allows. The wait is a constant rather than a constructor argument because an
instance that could be told to wait would be a second policy: the delayed one
already exists and is chosen by configuration instead (composition over
inheritance, `goal.md:8`).
"""

from datetime import datetime

from webcrawler.domain.base_result import BaseResult
from webcrawler.domain.custom_url import CustomURL
from webcrawler.ports.politeness_policy import PolitenessPolicy

NO_WAIT_MS: int = 0

class NoOpPolitenessPolicy(PolitenessPolicy):
    """Always answers "call now", for a crawl that nobody asked to slow down.

    The class extends the `PolitenessPolicy` ABC and is extended by nothing,
    which is what keeps the worker's politeness decision substitutable
    (`goal.md:8`).
    """

    def before_fetch(self, url: CustomURL) -> int:
        """Report that no wait is configured, whatever the worker's threshold.

        `0` is the port's "call now" (`goal.md:140`), so the worker takes its
        sleep branch for every URL and a crawl proceeds at full speed.

        Args:
        url: The URL about to be fetched, ignored because no host is tracked.

        Returns:
        int: Always `NO_WAIT_MS`, the milliseconds to wait before the next
        request.

        Synchronous: a constant, no I/O.
        """
        return NO_WAIT_MS

    def record_fetch(self, now: datetime, url: CustomURL, result: BaseResult) -> None:
        """Discard one completed attempt, keeping no state to learn from.

        Args:
        now: When the attempt finished, in UTC, ignored.
        url: The URL that was fetched, ignored.
        result: How the attempt ended, ignored, because there is no delay to
        back off from.

        Synchronous: nothing is recorded, no I/O.
        """
        return None
