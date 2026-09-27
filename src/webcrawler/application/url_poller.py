"""The URL poller: the producer half of the crawl loop (goal.md:55-113).

`goal.md:56` asks for one module that polls the URL state store and feeds the
queue, and `goal.md:106` asks for it behind an interface, which is
`CrawlQueuer`. This module is that implementation: every claim is the
repository's own atomic `UPDATE ... RETURNING`, so the poller adds no state
machine of its own and decides nothing the store does not already decide
atomically (goal.md:109).

Three choices carry the design, and each is a decision rather than an obvious
line of code:

- One `asyncio.Lock`, acquired exactly once at each of the three entry
  points. `goal.md:109` needs no lock for the state transition, because the
  claim's timeout predicates cannot double-claim a row; the lock here solves
  a different problem, `goal.md:107`: an API call must not interleave with a
  periodic poll, so the two never issue overlapping claims or feeds.
- No dedupe gate. The queue's operations are not retried, so no request id is
  ever sent twice; the optional `request_id` on the ports stays as the
  extension point for a networked broker, where a retried send would
  otherwise duplicate the message.
- The first poll runs immediately and each later poll one
  `periodic_fetch_seconds` after the previous claim, so a crawl that starts
  with due rows queues them without first waiting out an interval.
"""

import asyncio
import logging
import uuid
from datetime import datetime, timedelta

from webcrawler.domain.custom_url import CustomURL
from webcrawler.domain.messages import BaseMessage
from webcrawler.ports.crawl_queuer import CrawlQueuer
from webcrawler.ports.time_provider import TimeProviderFactory
from webcrawler.ports.topic_producer import TopicProducer
from webcrawler.ports.url_state_repository import URLStateRepository


class URLPoller(CrawlQueuer):
    """Polls the state store and feeds the queue one claimed batch at a time.

    One instance is meant to run: `goal.md:107` rules out a second poller. The
    single lock upholds that without a registry — every entry point serializes
    through it, so no claim or feed interleaves with another.

    Args:
        repository: The crawl state store, the only judge of which rows are
            claimable, so the poller never re-implements a predicate
            (goal.md:109).
        producer: The queue's write side, fed in bulk so one poll costs one
            call (goal.md:123).
        periodic_fetch_seconds: The interval between two periodic polls.
        periodic_max_items: The row limit each periodic poll claims, `-1`
            meaning no limit (goal.md:59).
        max_items_to_queue: The row limit `queue_candidates` falls back to
            when its own `max_items` is None, defaulting to `-1`, no limit
            (goal.md:59).
        job_timeout: The `started_crawl` staleness timeout, shared with every
            claim so the poller and the store agree on one predicate.
        queue_timeout: The `queued` staleness timeout, which is also how a
        row whose queue send was rejected is recovered by a later poll.
        time_provider: The only source of "now" for a claim, so a poll's
        instant is the application's guarantee rather than a second
        clock's.
        logger: The injected logger. A rejected enqueue is WARNING, because the
            row's recovery depends on a later poll seeing it.
    """

    def __init__(
        self,
        repository: URLStateRepository,
        producer: TopicProducer,
        *,
        periodic_fetch_seconds: float = 1.0,
        periodic_max_items: int = -1,
        max_items_to_queue: int = -1,  # goal.md:59 default: no limit
        job_timeout: timedelta,
        queue_timeout: timedelta,
        time_provider: TimeProviderFactory,
        logger: logging.Logger,
    ) -> None:
        """Hold the ports, the schedule, and the one lock every entry shares.

        Args:
            repository: The crawl state store.
            producer: The queue's write side.
            periodic_fetch_seconds: The interval between two periodic polls.
            periodic_max_items: The row limit each periodic poll claims.
            max_items_to_queue: The `queue_candidates` fallback row limit.
            job_timeout: The `started_crawl` staleness timeout.
            queue_timeout: The `queued` staleness timeout.
            time_provider: The only source of "now".
            logger: The injected logger.
        """
        self._repository = repository
        self._producer = producer
        self._periodic_fetch_seconds = periodic_fetch_seconds
        self._periodic_max_items = periodic_max_items
        self._max_items_to_queue = max_items_to_queue
        self._job_timeout = job_timeout
        self._queue_timeout = queue_timeout
        self._time_provider = time_provider
        self._logger = logger
        self._lock = asyncio.Lock()

    async def run(self) -> None:
        """Poll the store forever, one claim and one bulk feed per poll.

        The first poll runs immediately and each later poll one
        `periodic_fetch_seconds` after the previous claim, so a crawl that
        starts with due rows queues them without waiting out an interval.

        Each poll mints a fresh request id and hands it to the one bulk feed
        for the rows the claim returned, so a networked broker could make that
        feed idempotent. Queue growth is bounded by that single `enqueue_many`.

        Raises:
            Exception: Whatever a claim or the bulk feed raises, propagated
                unchanged; the repository's own retries are already spent by
                then, so the loop does not swallow an exhausted store.
            asyncio.CancelledError: When the task is cancelled, which is how
                the orchestrator stops the poller.
        """
        while True:
            # The public APIs are never called from here: asyncio.Lock is not
            # re-entrant, so that would deadlock on the lock held.
            async with self._lock:
                now = self._time_provider.now()
                await self._claim_and_enqueue(
                    uuid.uuid4().hex,
                    now,
                    urls=None,
                    max_items=self._periodic_max_items,
                )
            await asyncio.sleep(self._periodic_fetch_seconds)

    async def _claim_and_enqueue(
        self,
        request_id: str | None,
        now: datetime,
        *,
        urls: list[CustomURL] | None = None,
        max_items: int = -1,
    ) -> None:
        """Claim this request's rows and feed them in one bulk call.

        The caller must already hold `_lock`: this method is the shared body
        of the three entry points and exists precisely so none of them
        reacquires the non-re-entrant lock.

        The bulk feed is issued even for an empty claim: one code path, so an
        empty poll costs the same single call a full one does and the
        producer's result list stays the only place a per-row outcome exists.

        Args:
            request_id: The id handed to the bulk feed, or None to mint one
                here. Only `run` supplies a stable id, one per poll check; the
                APIs default to a fresh id, because a caller retrying without
                an id is a genuinely new request, not a repeat.
            now: The instant the claim's predicates are evaluated against.
            urls: The caller's own URLs, or None to claim candidates. A
                caller's list is already the batch, so `enqueue_urls` passes
                it with no limit.
            max_items: The claim's row limit, already resolved by the
                caller, which is why this takes a plain `int` and not
                `int | None`; `-1` means no limit (goal.md:59).

        Raises:
            Exception: Whatever the claim or the bulk feed raises, propagated
                unchanged; a partial feed is not reported as success.
        """
        if request_id is None:
            request_id = uuid.uuid4().hex
        if urls is None:
            rows = await self._repository.claim_candidates(
                now,
                max_items,
                job_timeout=self._job_timeout,
                queue_timeout=self._queue_timeout,
            )
        else:
            rows = await self._repository.claim_urls(
                urls,
                now,
                max_items,
                job_timeout=self._job_timeout,
                queue_timeout=self._queue_timeout,
            )
        # partition_key is hash(url), stored verbatim: a negative key already
        # routes non-negatively under the producer's modulo (goal.md:113).
        # No retry policy here: the repository retries its own statements and
        # the in-memory producer cannot fail, so there is nothing left to retry.
        results = await self._producer.enqueue_many(
            [BaseMessage(url, partition_key=hash(url)) for url in rows],
            request_id,
        )
        for url, enqueued in zip(rows, results):
            if not enqueued:
                self._logger.warning(
                    "queueing %s was rejected, so it stays queued and the "
                    "first poll %s from now re-claims it",
                    url.get_url(),
                    self._queue_timeout,
                )

    async def enqueue_urls(
        self, urls: list[CustomURL], request_id: str | None = None
    ) -> None:
        """Queue the caller's URLs now, and nothing else (goal.md:103).

        Only the rows the claim returns are fed, so a URL that is already
        being processed, or that does not exist, is quietly left alone: the
        store is the only judge of eligibility (goal.md:109).

        Args:
            urls: The URLs to queue, expected to exist as rows already, which
                is why this claims and never inserts.
            request_id: The id handed to the bulk feed, or None to mint a
                fresh one inside.

        Raises:
            Exception: Whatever the claim or the bulk feed raises, propagated
                unchanged; a partial queue is not reported as success.
        """
        async with self._lock:
            await self._claim_and_enqueue(
                request_id,
                self._time_provider.now(),
                urls=urls,
                max_items=-1,
            )

    async def queue_candidates(
        self,
        now: datetime,
        max_items: int | None = None,
        request_id: str | None = None,
    ) -> None:
        """Queue candidate rows on demand, up to the resolved limit (goal.md:104).

        Args:
            now: The instant the claim's predicates are evaluated against.
            max_items: This call's row limit, or None to fall back to
                `max_items_to_queue` (`goal.md:59`).
            request_id: The id handed to the bulk feed, or None to mint a
                fresh one inside.

        Raises:
            Exception: Whatever the claim or the bulk feed raises, propagated
                unchanged; a partial queue is not reported as success.
        """
        async with self._lock:
            await self._claim_and_enqueue(
                request_id,
                now,
                urls=None,
                max_items=self._max_items_to_queue if max_items is None else max_items,
            )
