"""The URL poller: the producer half of the crawl loop, behind `CrawlQueuer`.

It polls the URL state store and feeds the queue in one bulk call per claim.
The claim is the repository's own atomic statement, so this module holds no
state machine, no lock, and no dedupe gate of its own.
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

    One instance is meant to run: a second poller is ruled out, and
    one construction in `main.py` is what ensures it. No lock serialises the
    entry points; the store's own claim is what stops two claims colliding.
    """

    def __init__(
        self,
        repository: URLStateRepository,
        producer: TopicProducer,
        *,
        periodic_fetch_seconds: float = 1.0,
        periodic_max_items: int = -1,
        max_items_to_queue: int = -1,  # default: no limit
        job_timeout: timedelta,
        queue_timeout: timedelta,
        time_provider: TimeProviderFactory,
    ) -> None:
        """Hold the ports and the schedule the poller was configured with.

        Args:
            repository: The crawl state store.
            producer: The queue's write side.
            periodic_fetch_seconds: The interval between two periodic polls.
            periodic_max_items: The row limit each periodic poll claims.
            max_items_to_queue: The `queue_candidates` fallback row limit.
            job_timeout: The `started_crawl` staleness timeout.
            queue_timeout: The `queued` staleness timeout.
            time_provider: The only source of "now".
        """
        self._logger = logging.getLogger(__name__)
        self._repository = repository
        self._producer = producer
        self._periodic_fetch_seconds = periodic_fetch_seconds
        self._periodic_max_items = periodic_max_items
        self._max_items_to_queue = max_items_to_queue
        # One source for both staleness timeouts and one for "now", so the
        # poller and the store agree on one predicate and on one instant.
        self._job_timeout = job_timeout
        self._queue_timeout = queue_timeout
        self._time_provider = time_provider

    async def run(self) -> None:
        """Poll the store forever, one claim and one bulk feed per poll.

        Each poll mints a fresh request id, so a broker could dedupe the feed.

        Raises:
            asyncio.CancelledError: When the task is cancelled, which is how
            the orchestrator stops the poller.
        """
        while True:
            # The first poll runs at once and each later one waits out an
            # interval, so a crawl that starts with due rows queues them
            # without first waiting.
            await self._claim_and_enqueue(
                None,
                self._time_provider.now(),
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

        The shared body of all three entry points, so one guard covers them all.

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
                `int | None`; `-1` means no limit.

        Raises:
        Exception: Nothing is raised
        """
        # Only the claim is guarded: real I/O against a store that documents
        # failures, and a failure strands rows in `queued` until `queue_timeout`
        # elapses. The feed is not: a networked producer may raise, while the
        # shipped in-memory one never does and reports a full queue as `False`.
        try:
            # The claim is the store's own atomic `UPDATE ... RETURNING` and
            # the only judge of claimability, so no lock and no predicate is
            # re-implemented here: the timeout predicates cannot double-claim
            # a row, and the store's own lock (`BEGIN IMMEDIATE`) serialises it.
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
        except Exception as error:
            # Logged and returned, so a failing poll costs one poll and neither
            # the poller nor any caller dies; the stranded row is re-claimed once
            # `queue_timeout` elapses, not on the next poll.
            self._logger.error(
                "claiming due urls failed, so nothing is queued this round and "
                "a stranded row is re-claimed once %s has elapsed: %s",
                self._queue_timeout,
                error,
            )
            return
        # partition_key is hash(url), stored verbatim: a negative key already
        # routes non-negatively under the producer's modulo. One bulk call per
        # poll, issued even for an empty claim, so one path serves both.
        results = await self._producer.enqueue_many(
            [BaseMessage(url, partition_key=hash(url)) for url in rows],
            request_id,
        )
        # The shipped feed holds no `await`, so nothing interleaves between the
        # claim committing and these messages landing. It is not retried, so no
        # dedupe gate and no request id sent twice; `request_id` is the ports'
        # extension point for a networked broker, whose retried send duplicates.
        for url, enqueued in zip(rows, results):
            if not enqueued:
                # WARNING, not ERROR: the row stays `queued` and a later poll
                # re-claims it, so its recovery depends on this being visible.
                self._logger.warning(
                    "queueing %s was rejected, so it stays queued and the "
                    "first poll %s from now re-claims it",
                    url.get_url(),
                    self._queue_timeout,
                )

    async def enqueue_urls(
        self, urls: list[CustomURL], request_id: str | None = None
    ) -> None:
        """Queue the caller's URLs now, and nothing else.

        Only the rows the claim returns are fed; a failed claim is swallowed.

        Args:
            urls: The URLs to queue, expected to exist as rows already, which
                is why this claims and never inserts.
            request_id: The id handed to the bulk feed, or None to mint a
                fresh one inside.

        Raises:
        Exception: Nothing is raised
        """
        await self._claim_and_enqueue(
            request_id,
            self._time_provider.now(),
            urls=urls,
            max_items=-1,
        )

    async def queue_candidates(
        self,
        now: datetime,
        request_id: str,
        max_items: int | None = None,
    ) -> None:
        """Queue candidate rows on demand, up to the resolved limit.

        Best effort: a failed claim is logged, so a later poll re-claims it.

        Args:
            now: The instant the claim's predicates are evaluated against.
            request_id: The id handed to the bulk feed.
            max_items: This call's row limit, or None to fall back to
                `max_items_to_queue`.

        Raises:
        Exception: Nothing is raised
        """
        await self._claim_and_enqueue(
            request_id,
            now,
            urls=None,
            max_items=self._max_items_to_queue if max_items is None else max_items,
        )
