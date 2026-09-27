"""The crawl queuing port, the two APIs of `goal.md:55-113`.

`goal.md:106` requires an interface behind the poller so the worker and the
orchestrator ask for URLs to be queued without knowing how candidates are
found or how the topic is fed. The implementation is the single poller
instance, and it decides nothing that the repository does not already decide
atomically.
"""

from abc import ABC, abstractmethod
from datetime import datetime

from webcrawler.domain.custom_url import CustomURL

class CrawlQueuer(ABC):
    """Moves eligible URL rows from the state store into the queue.

    Both APIs return `None` and signal failure by raising; a claim that returns
    fewer rows than were asked for is not a failure. The dedupe
    key is threaded through `request_id` so a retried call cannot grow the
    queue, and both APIs are coroutines so the poller loop stays on the single
    event loop of `goal.md:13`.
    """

@abstractmethod
async def enqueue_urls(
    self, urls: list[CustomURL], request_id: str | None = None
    ) -> None:
    """Queue exactly the given URLs now, and nothing else.

    API (a) of `goal.md:103`, the operator seeding path. The URLs are
    already rows in the store, inserted by the worker or the orchestrator,
    so this only moves eligible ones to `queued`. A repeated
    `request_id` is skipped, which is what makes a retried call safe
    (goal.md:100).

    Args:
    urls: The URLs to queue. A URL that is already freshly `queued` or
    `started_crawl`, or that does not exist, is not enqueued.
    request_id: The caller-supplied dedupe key. `None` mints a fresh
    id inside the implementation, so a retried call with no id is
    not deduplicated against the original.

    Raises:
    Exception: Whatever the claim or the enqueue raises, propagated
    unchanged; a partial queue is not reported as success.
    """

@abstractmethod
async def queue_candidates(
    self,
    now: datetime,
    max_items: int | None = None,
    request_id: str | None = None) -> None:
    """Queue candidate rows on demand, up to the caller's limit.

    API (b) of `goal.md:104`. It exists for programmatic callers and is
    covered by tests rather than by the shipped run path, which seeds
    through `enqueue_urls`. A repeated `request_id` is
    skipped (goal.md:100).

    Args:
    now: The instant the eligibility predicates are evaluated against,
    in UTC.
    max_items: The row limit, or `None` to fall back to the
    constructor-supplied `max_items_to_queue`, itself defaulting to
    `-1`, meaning no limit (goal.md:59).
    request_id: The caller-supplied dedupe key. `None` mints a fresh
    id inside the implementation.

    Raises:
    Exception: Whatever the claim or the enqueue raises, propagated
    unchanged; a partial queue is not reported as success.
    """
