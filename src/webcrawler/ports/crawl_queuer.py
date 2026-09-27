"""The queuer port (goal.md:56, plan.md:237-245).

`goal.md:56` asks for one module that polls the URL state store and feeds the
queue, and `goal.md:106` asks for it behind an interface, which is
`CrawlQueuer`. Both APIs return `None` and raise on failure, so a caller cannot
mistake a partial queue for a success, and both take an optional
`request_id` so a retried call does not queue a URL twice (goal.md:100).
"""

from abc import ABC, abstractmethod
from datetime import datetime

from webcrawler.domain.custom_url import CustomURL


class CrawlQueuer(ABC):
    """Queues crawl work now, either URLs the caller names or due candidates."""

    @abstractmethod
    async def enqueue_urls(
        self, urls: list[CustomURL], request_id: str | None = None
    ) -> None:
        """Queue exactly the given URLs now, and nothing else.

        The URLs must already exist in the store, because the claim that moves
        them to `queued` only ever targets rows it holds. An empty list is a
        no-op.

        Args:
            urls: The URLs to queue, which the caller has already inserted.
            request_id: The caller-supplied dedupe key. `None` mints a fresh
                one, so two calls with `None` are two genuine checks.

        Raises:
            Exception: Whatever the claim or the enqueue raises, propagated
                unchanged; a partial queue is not reported as success.
        """

    @abstractmethod
    async def queue_candidates(
        self,
        now: datetime,
        max_items: int | None = None,
        request_id: str | None = None,
    ) -> None:
        """Queue the store's due rows now, up to a limit.

        Args:
            now: The instant the eligibility predicates are evaluated against,
                in UTC.
            max_items: The row limit, or `None` to fall back to the
                configured default, `-1` for no limit (goal.md:59).
            request_id: The caller-supplied dedupe key. `None` mints a fresh
                one, so two calls with `None` are two genuine checks.

        Raises:
            Exception: Whatever the claim or the enqueue raises, propagated
                unchanged; a partial queue is not reported as success.
        """
