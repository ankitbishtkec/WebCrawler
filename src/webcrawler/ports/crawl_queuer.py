"""The queuer port.

One module polls the store and feeds the queue, behind this interface, `CrawlQueuer`. Both
APIs return `None` and raise on failure, so a partial queue cannot pass for success, and
both take a `request_id` so a retried call does not queue twice.
"""

from abc import ABC, abstractmethod
from datetime import datetime

from webcrawler.domain.custom_url import CustomURL


class CrawlQueuer(ABC):
    """Queues crawl work now, either URLs the caller names or due candidates."""
#ankit: missing run function which polls like in url poller
    @abstractmethod
    async def enqueue_urls(
        self, urls: list[CustomURL], request_id: str
    ) -> None:
        """Queue exactly the given URLs now, and nothing else.

        The URLs must already exist in the store, because the claim that moves them to `queued` only ever targets rows it holds; an empty list is a no-op.

        Args:
            urls: The URLs to queue, which the caller has already inserted.
            request_id: The caller-supplied dedupe key.

        Raises:
            Exception: A networked implementation may raise; the shipped in-memory one never does, it logs and swallows a claim failure because the poll loop re-claims the same rows.
        """

    @abstractmethod
    async def queue_candidates(
        self,
        now: datetime,
        request_id: str,
        max_items: int | None = None,
    ) -> None:
        """Queue the store's due rows now, up to a limit.

        Args:
            now: The instant the eligibility predicates are evaluated, in UTC.
            request_id: The caller-supplied dedupe key.
            max_items: The row limit, or `None` for the configured default; `-1` for no limit.

        Raises:
            Exception: A networked implementation may raise; the shipped in-memory one never does, it logs and swallows a claim failure because the poll loop re-claims the same rows.
        """
