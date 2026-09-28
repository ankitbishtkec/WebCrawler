"""The default politeness policy: no delay at all.

A no-op is the default wiring, so an operator who configures nothing crawls as
fast as the host allows.
"""

from datetime import datetime

from webcrawler.domain.base_result import BaseResult
from webcrawler.domain.custom_url import CustomURL
from webcrawler.ports.politeness_policy import PolitenessPolicy

NO_WAIT_MS: int = 0

# The class extends the `PolitenessPolicy` ABC and is extended by nothing, which
# is what keeps the worker's politeness decision substitutable.
class NoOpPolitenessPolicy(PolitenessPolicy):
    """Always answers "call now", for a crawl that nobody asked to slow down.

    The wait is a constant, not a constructor argument: a configurable one would
    be a second policy, and the delayed policy already exists for configuration
    to choose (composition over inheritance).
    """

    def before_fetch(self, url: CustomURL) -> int:
        """Report that no wait is configured, whatever the worker's threshold.

        `0` is the port's "call now", so a crawl proceeds at full speed.

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
