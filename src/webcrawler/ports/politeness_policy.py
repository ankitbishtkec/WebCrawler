"""The politeness port.

The decision lives in an interface with a no-op default, so the crawl is as fast
as the host permits while a courteous delay stays one constructor argument away.
"""

from abc import ABC, abstractmethod
from datetime import datetime

from webcrawler.domain.base_result import BaseResult
from webcrawler.domain.custom_url import CustomURL

#ankit: both method should be async type as io operation can be done for these
class PolitenessPolicy(ABC):
    """Decides how long a worker must wait before its next request.

    The policy returns a number and never sleeps: the worker defers the URL to
    that time, so one slow URL cannot stall its batch.
    """
    @abstractmethod
    def before_fetch(self, url: CustomURL) -> int:
        """Return the milliseconds to wait before fetching `url`.

        `0` means fetch now; any other value is the delay the worker sleeps for or defers the URL by.

        Args:
        url: The URL about to be fetched, so a per-host policy can weigh it.

        Returns:
            int: Milliseconds to wait before issuing the request; `0` for none.

        Synchronous: pure computation, no I/O.
        """

    @abstractmethod
    def record_fetch(self, now: datetime, url: CustomURL, result: BaseResult) -> None:
        """Record one completed attempt, so the next delay can be better.

        The shipped no-op ignores everything; a learning policy uses `result` to back off after a failure.

        Args:
        now: When the attempt finished, in UTC.
        url: The URL that was fetched.
        result: How that attempt ended.

        Synchronous: pure bookkeeping, no I/O.
        """
