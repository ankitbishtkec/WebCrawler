"""The crawl worker, the consumer half of the crawl loop, behind `CrawlWorker`.

It peeks a batch, detaches one crawl task per message, commits the batch at once,
and returns to the queue, so the fetcher is given more work without waiting on
the work in flight. Only the crawls are detached: their statuses, outcomes and
discovered links are accumulated, so a separate flush loop wakes up every
`flush_interval_seconds` and makes one bulk store write and one bulk queue feed
per window. Every collaborator is a port, so this module names no concrete class.
"""

import asyncio
import logging
from datetime import datetime, timedelta

from webcrawler.domain.base_result import BaseResult
from webcrawler.domain.custom_url import CustomURL
from webcrawler.domain.messages import BaseMessage
from webcrawler.ports.crawl_queuer import CrawlQueuer
from webcrawler.ports.crawl_worker import CrawlWorker
from webcrawler.ports.link_extractor import LinkExtractor
from webcrawler.ports.politeness_policy import PolitenessPolicy
from webcrawler.ports.time_provider import TimeProviderFactory
from webcrawler.ports.topic_producer import TopicProducer
from webcrawler.ports.topic_reader import TopicReader
from webcrawler.ports.url_state_repository import URLStateRepository
from webcrawler.ports.web_page_fetcher import WebPageFetcher

# The fetches allowed in flight at once, 1000 by default; a task holds a slot
# for its whole retry budget, so this bounds concurrent requests, not tasks.
DEFAULT_MAX_CONCURRENT_FETCHES: int = 1000
# The flush loop's period, 10ms by default. Short on purpose: the window is the
# wait between a page's links being found and those links becoming crawlable, so
# it paces a deep discovery chain, while a wide one never waits for it at all.
DEFAULT_FLUSH_INTERVAL_SECONDS: float = 0.01


class CrawlerWorkerV1(CrawlWorker):
    """Consumes the crawl queue forever, writing the store in flush windows.

    The `CrawlWorker` that does not wait for its batch; `CrawlerWorker` in
    `application/worker.py` is the other one, and `main.py` picks either.

    Why prefer this worker? `CrawlerWorker` picks a batch of URLs and waits for
    every one of them before it picks up new work, so a single slow site leaves
    the crawler idle until it answers. This one never waits: it keeps taking new
    crawl requests while the earlier ones are still in flight, and holds at most
    `max_concurrent_fetches` fetches at a time, 1000 by default. It also looks at
    the queue every 10ms rather than once a second, so a link found on one page
    is crawlable almost at once. The dependencies and their interactions are
    otherwise the same as `CrawlerWorker`'s. The bulk writes are the same idea on
    a timer instead of on a batch: `CrawlerWorker` saved to the database only when
    every URL of a batch was done, where this one collects the writes for 10ms
    and then makes them in bulk.

    `run` returns only on cancellation, since a crawl ends when the operator
    interrupts it, and `close` flushes what reached the buffers before it
    returns, without waiting for the crawls still detached. A flush that fails
    drops its snapshot, and the rows it held are reclaimed by `job_timeout`
    rather than being written twice.
    """

    def __init__(
        self,
        repository: URLStateRepository,
        reader: TopicReader,
        fetcher: WebPageFetcher,
        link_extractor: LinkExtractor,
        politeness_policy: PolitenessPolicy,
        queuer: CrawlQueuer,
        *,
        producer: TopicProducer,
        batch_size: int,
        # 10ms, where `CrawlerWorker` waits a second: this worker empties its
        # queue as fast as it fills it, and a long sleep here is the price of
        # that, since a message that arrives during the sleep waits for it.
        idle_sleep_seconds: float = 0.01,
        reschedule_delay: timedelta = timedelta(minutes=1),
        re_crawl_interval: timedelta | None = None,
        max_concurrent_fetches: int = DEFAULT_MAX_CONCURRENT_FETCHES,
        flush_interval_seconds: float = DEFAULT_FLUSH_INTERVAL_SECONDS,
        time_provider: TimeProviderFactory,
    ) -> None:
        """Hold the ports, the schedule, and the empty flush buffers.

        Args:
        repository: The crawl state store, written only by the flush loop.
        reader: The queue's read side.
        fetcher: Retrieves one page body.
        link_extractor: Parses a body into same-host links.
        politeness_policy: Reports the wait before the next call.
        queuer: Queues the discovered URLs, once the flush has stored them.
        producer: Parks the messages of URLs that could not be crawled.
        batch_size: The most messages one peek returns.
        idle_sleep_seconds: The wait after a peek returned nothing.
        reschedule_delay: The delay applied after a failed URL.
        re_crawl_interval: The re-crawl interval, or None for no re-crawl.
        max_concurrent_fetches: The fetches allowed in flight at once.
        flush_interval_seconds: The wait between two store writes.
        time_provider: The only source of "now".
        """
        self._logger = logging.getLogger(__name__)
        self._repository = repository
        self._reader = reader
        self._fetcher = fetcher
        self._link_extractor = link_extractor
        self._politeness_policy = politeness_policy
        self._queuer = queuer
        self._producer = producer
        self._batch_size = batch_size
        self._idle_sleep_seconds = idle_sleep_seconds
        self._reschedule_delay = reschedule_delay
        self._re_crawl_interval = re_crawl_interval
        self._flush_interval_seconds = flush_interval_seconds
        self._time_provider = time_provider
        # Acquired around a fetch and released once the body is in hand, so
        # this counts requests on the wire and not tasks waiting on one.
        self._fetch_slots = asyncio.Semaphore(max_concurrent_fetches)
        # The three flush buffers, swapped whole and never emptied in place, so
        # a crawl task always appends to the buffer the next flush reads.
        self._started: set[CustomURL] = set()
        self._finished: dict[CustomURL, datetime | None] = {}
        self._discovered: set[CustomURL] = set()
        # Held only so a detached crawl is not garbage collected mid-fetch.
        self._crawling: set[asyncio.Task[None]] = set()

    async def close(self) -> None:
        """Write the buffers one last time, then release the pooled session.

        `run` stops on cancellation, so whatever the detached crawls recorded
        after the last flush is still buffered here and this is its only
        writer; the session is released in `finally`, so a failed flush does
        not leak it.

        Raises:
        Exception: Whatever the fetcher's close raises, as the port allows.
        """
        try:
            await self._flush()
        finally:
            await self._fetcher.close()

    async def run(self) -> None:
        """Read and flush until cancelled, as two tasks of one group.

        A failed loop takes the other down with it, so the buffers are never
        written by two flusher-free windows; `close` then writes what is left.

        Raises:
        asyncio.CancelledError: When cancelled, which is how the orchestrator
        stops the crawl.
        Exception: Whatever a loop raises, logged here and propagated.
        """
        self._logger.debug("worker reading the crawl queue")
        try:
            async with asyncio.TaskGroup() as group:
                group.create_task(self._flush_forever())
                group.create_task(self._consume_forever())
        except Exception as error:
            self._logger.error("a worker task failed, so the crawl is over: %s", error)
            raise

    async def _consume_forever(self) -> None:
        """Peek, detach one crawl per message, commit, and peek again.

        Raises:
        asyncio.CancelledError: When cancelled, which is how `run` stops.
        """
        while True:
            batch = await self._reader.peek(self._batch_size)
            if not batch:
                # peek is CPU-only, so this sleep is the only yield on an empty
                # queue; without it the poller would starve.
                await asyncio.sleep(self._idle_sleep_seconds)
                continue
            # Each message becomes its own detached crawl, so one slow fetch
            # holds nothing: the semaphore, not the queue, is the bound.
            for message in batch:
                crawl = asyncio.create_task(self._crawl_one(message))
                self._crawling.add(crawl)
                crawl.add_done_callback(self._crawling.discard)
            # Committed as the tasks exist, not as they finish, so a fetch that
            # outlives this loop is not handed back and crawled twice. Left
            # unguarded: a failed ack leaves the same head in place, and
            # retrying it here would re-crawl that head for ever.
            await self._reader.commit(batch)

    async def _crawl_one(self, message: BaseMessage) -> None:
        """Crawl one message's URL, buffering its status and its outcome.

        One URL per task and every step inside one handler, because a detached
        task has no task group to contain a raise and its exception would be
        reported as never retrieved. A politeness wait is deferred, never
        slept, so no URL stalls the batch.

        Args:
        message: The message this task owns, whose URL is crawled and, on
            failure, parked.

        Raises:
        Exception: Nothing is raised
        """
        url = message.url
        now = self._time_provider.now()
        self._mark_started(url)
        try:
            wait_ms = await self._politeness_policy.before_fetch(url)
            if wait_ms > 0:
                self._complete_crawl(
                    url, now + timedelta(milliseconds=wait_ms), set()
                )
                return
            is_success = False
            try:
                # A slot is held for the whole retry budget and given back the
                # moment the body is in hand, so extracting links below never
                # counts as a fetch in flight.
                async with self._fetch_slots:
                    html = await self._fetcher.fetch(url)
                is_success = True
            except Exception as error:
                # A handled failure, not an abort: the row becomes due again
                # and the caller deadletters the message.
                self._logger.error(
                    "fetching %s failed once its retries were spent: %s",
                    url.get_url(),
                    error,
                )
            finally:
                # One outcome per attempt, given back even when it failed, so a
                # learning policy can back off from it.
                await self._politeness_policy.record_fetch(
                    now, url, BaseResult(is_success=is_success)
                )
            if not is_success:
                self._complete_crawl(url, now + self._reschedule_delay, set())
                await self._deadletter(message)
                return
            links = self._link_extractor.extract(html, url)
            # INFO, the crawl's own output: the page visited and its links.
            self._logger.info(
                "visited %s, found %d link(s): %s",
                url.get_url(),
                len(links),
                [link.get_url() for link in links],
            )
            # Compared against None, not truthiness, so a configured zero still
            # schedules a re-crawl.
            next_crawl_time = (
                None
                if self._re_crawl_interval is None
                else now + self._re_crawl_interval
            )
            self._complete_crawl(url, next_crawl_time, links)
        except Exception as error:
            # The one handler around the whole crawl: a deferral, an extract,
            # or a write that raises leaves the row due again and the message
            # parked, so a detached task cannot lose the work silently.
            self._logger.error(
                "crawling %s failed, so it is due again in %s and its message "
                "is deadlettered: %s",
                url.get_url(),
                self._reschedule_delay,
                error,
            )
            self._complete_crawl(url, now + self._reschedule_delay, set())
            await self._deadletter(message)

    def _mark_started(self, url: CustomURL) -> None:
        """Buffer one started URL; the flush loop writes them in one call.

        The attempt time is the flush's own instant, since a window is short and
        a row marked a little late still times out correctly.

        Args:
        url: The URL whose attempt begins, already held by the store.

        Raises:
        Exception: Nothing is raised
        """
        self._started.add(url)

    def _complete_crawl(
        self,
        url: CustomURL,
        next_crawl_time: datetime | None,
        links: set[CustomURL],
    ) -> None:
        """Buffer one outcome and the links it revealed; nothing is written.

        An unseen URL and a stored None both take the incoming value, so one
        `None` never displaces a real instant; two real instants collapse to
        the later one.

        Args:
        url: The URL this task crawled.
        next_crawl_time: The instant it becomes due again, None for no re-crawl.
        links: The unique same-host links the body revealed, empty on failure.

        Raises:
        Exception: Nothing is raised
        """
        known = self._finished.get(url)
        if known is None:
            self._finished[url] = next_crawl_time
        elif next_crawl_time is not None and next_crawl_time > known:
            self._finished[url] = next_crawl_time
        self._discovered.update(links)

    async def _flush_forever(self) -> None:
        """Write the buffers to the store every interval, until cancelled.

        Raises:
        asyncio.CancelledError: When cancelled, which is how `run` stops it.
        """
        while True:
            await asyncio.sleep(self._flush_interval_seconds)
            await self._flush()

    async def _flush(self) -> None:
        """Hand one snapshot of the buffers to the store and the queue.

        `mark_started` is called before `complete_crawl`, and one instant is
        shared by both, so every row in a window carries the same attempt and
        completion times. The queue is fed only after that write committed, so
        a discovered URL is queued once and by one claim.

        Raises:
        Exception: Nothing is raised
        """
        # Swapped whole, with no await in between, so no crawl task can append
        # to a buffer this flush has already taken. A failed call drops its
        # snapshot: the rows are reclaimed by the store's own timeouts, and
        # retrying here would write an outcome the crawl may have repeated.
        started, self._started = self._started, set()
        finished, self._finished = self._finished, {}
        discovered, self._discovered = self._discovered, set()
        # A URL that also finished in this window is written twice on purpose:
        # `started.difference_update(finished)` here would drop one write per
        # URL per window, since `complete_crawl` is the one final status, but
        # `mark_started` is the only writer of `last_crawl_time`, so the rows it
        # skipped would keep a NULL attempt time.
        now = self._time_provider.now()
        if started:
            try:
                # One call for the whole window, the port's bulk shape.
                await self._repository.mark_started(started, now)
            except Exception as error:
                self._logger.error(
                    "marking %d url(s) started failed, so their rows stay "
                    "claimable and a later claim re-queues them: %s",
                    len(started),
                    error,
                )
        if finished or discovered:
            try:
                # One transaction for both halves, so a reader never sees a
                # row finished while the URLs it discovered are missing. The
                # rows the claim missed are queued by the next poll.
                await self._repository.complete_crawl(finished, discovered, now)
            except Exception as error:
                self._logger.error(
                    "recording the outcome of %d url(s) failed, so their rows "
                    "stay started_crawl for the job_timeout branch to "
                    "reclaim: %s",
                    len(finished),
                    error,
                )
                return
            # One bulk feed for the window, after the write and not before it:
            # `enqueue_urls` claims the rows, so a URL whose row is not stored
            # yet is not queued. Left unguarded, as in `CrawlerWorker`: the
            # shipped queue is in-memory and cannot raise, and enqueue_urls
            # logs a rejected claim itself, which the poller's next poll then
            # re-claims.
            await self._queuer.enqueue_urls(list(discovered))

    async def _deadletter(self, message: BaseMessage) -> None:
        """Park one failed message so it is not retried for ever.

        A failure is logged, not raised, so one unreachable queue cannot take
        the crawl down.

        Args:
        message: The message whose URL did not crawl.

        Raises:
        Exception: Nothing is raised
        """
        try:
            parked = await self._producer.enqueue_to_deadletter(message)
        except Exception as error:
            self._logger.error(
                "parking the failed message for %s failed, so it is dropped "
                "with its batch already committed: %s",
                message.url.get_url(),
                error,
            )
            return
        if not parked:
            self._logger.error(
                "the deadletter queue is full, so the failed message for %s "
                "is dropped with its batch already committed",
                message.url.get_url(),
            )
