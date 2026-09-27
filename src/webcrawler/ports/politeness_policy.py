"""The politeness port (goal.md:140, plan.md:299-305).

`goal.md:140` puts the decision in an interface with a no-op default, so the
crawl is as fast as the host permits while a courteous delay stays one
constructor argument away.
"""

from abc import ABC, abstractmethod
from datetime import datetime

from webcrawler.domain.base_result import BaseResult
from webcrawler.domain.custom_url import CustomURL


class PolitenessPolicy(ABC):
    """Decides how long a worker must wait before its next request.

    The policy returns a number and does not sleep: whether a wait is honoured
    or turned into a re-schedule is the worker's decision, because the worker
    alone knows its own sleep threshold (plan.md:97, plan.md:804).
    """
    @abstractmethod
    def before_fetch(self, url: CustomURL) -> int:
        """Return the milliseconds to wait before fetching `url`.

        `0` means fetch now, and any other value is the delay the worker either
        sleeps for or defers the URL by, per `goal.md:140`.

        Args:
        url: The URL about to be fetched, so a per-host policy can weigh it.

        Returns:
            int: Milliseconds to wait before issuing the request; `0` for no
                wait at all.

        Synchronous: pure computation, no I/O.
        """

    @abstractmethod
    def record_fetch(self, now: datetime, url: CustomURL, result: BaseResult) -> None:
        """Record one completed attempt, so the next delay can be better.

        The shipped no-op ignores everything; a learning policy uses `result`
        to back off after a failure.

        Args:
        now: When the attempt finished, in UTC.
        url: The URL that was fetched.
        result: How that attempt ended.

        Synchronous: pure bookkeeping, no I/O.
        """
