"""The politeness port.

The decision lives in an interface with a no-op default, so a courteous delay
stays one constructor argument away. Both methods are coroutines because a
policy may read or write a shared rate limit, not only compute in memory.
"""

from abc import ABC, abstractmethod
from datetime import datetime

from webcrawler.domain.base_result import BaseResult
from webcrawler.domain.custom_url import CustomURL

class PolitenessPolicy(ABC):
    """Decides how long a worker must wait before its next request.

    Both methods may do I/O, so both are coroutines. The worker applies the
    returned delay by deferring the URL, so one slow URL cannot stall its batch.
    """
    @abstractmethod
    async def before_fetch(self, url: CustomURL) -> int:
        """Return the milliseconds to wait before fetching `url`.

        `0` means fetch now; any other value is the delay the worker sleeps for or defers the URL by.

        Args:
        url: The URL about to be fetched, so a per-host policy can weigh it.

        Returns:
            int: Milliseconds to wait before issuing the request; `0` for none.
        """

    @abstractmethod
    async def record_fetch(self, now: datetime, url: CustomURL, result: BaseResult) -> None:
        """Record one completed attempt, so the next delay can be better.

        The shipped no-op ignores everything; a learning policy uses `result` to back off after a failure.

        Args:
        now: When the attempt finished, in UTC.
        url: The URL that was fetched.
        result: How that attempt ended.
        """
