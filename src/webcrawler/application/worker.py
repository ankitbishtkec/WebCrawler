"""The crawl worker: the consumer half of the crawl loop (goal.md).

Reads a batch, crawls each URL concurrently, then records the outcome, queues
the discovered URLs, deadletters the failures, and commits the batch. Every
collaborator is a port, so this module names no concrete class.

No exception escapes the loop: a per-URL failure is one failed URL, and a failed
batch I/O call is one logged step, so neither can kill the worker.
"""

import asyncio
import logging
from datetime import datetime, timedelta

from webcrawler.domain.base_result import BaseResult
from webcrawler.domain.custom_url import CustomURL
from webcrawler.domain.messages import BaseMessage
from webcrawler.ports.crawl_queuer import CrawlQueuer
from webcrawler.ports.link_extractor import LinkExtractor
from webcrawler.ports.politeness_policy import PolitenessPolicy
from webcrawler.ports.time_provider import TimeProviderFactory
from webcrawler.ports.topic_producer import TopicProducer
from webcrawler.ports.topic_reader import TopicReader
from webcrawler.ports.url_state_repository import URLStateRepository
from webcrawler.ports.web_page_fetcher import WebPageFetcher

# One URL's outcome pair, the links its body revealed, and whether it was
# crawled; a False third element is what makes the caller deadletter it.
_CrawlResult = tuple[tuple[CustomURL, datetime | None], list[CustomURL], bool]


class CrawlerWorker:
    """Consumes the crawl queue one batch at a time, forever.

    `run` returns only on cancellation, since a crawl ends when the operator
    interrupts it. One instant is read per batch and shared by every write, so a
    batch cannot record two instants that disagree.

    Args:
    repository: The crawl state store, the only way a status is written.
    reader: The queue's read side, the only source of work.
    fetcher: Retrieves one page body under a supplied policy.
    link_extractor: Parses a body into the same-host URLs to crawl next.
    politeness_policy: Reports the wait before the next call, and records each
    attempt's outcome.
    queuer: Queues the discovered URLs once their rows are written.
    producer: Parks a message whose URL could not be crawled.
    batch_size: The most messages one `peek` returns, so the most URLs one
    task group holds.
    idle_sleep_seconds: The wait after a `peek` returned nothing.
    reschedule_delay: How far ahead a URL that failed is scheduled again.
    re_crawl_interval: When a crawled URL becomes due again, or None for
    no re-crawl.
    time_provider: The only source of "now" for any write.
    logger: The injected logger; a visited page and its links are INFO,
    the crawl's own output (goal.md:1).
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
        idle_sleep_seconds: float = 1.0,
        reschedule_delay: timedelta = timedelta(minutes=1),
        re_crawl_interval: timedelta | None = None,
        time_provider: TimeProviderFactory,
        logger: logging.Logger,
    ) -> None:
        """Hold the ports and the schedule this worker was configured with.

        Args:
        repository: The crawl state store.
        reader: The queue's read side.
        fetcher: Retrieves one page body.
        link_extractor: Parses a body into same-host links.
        politeness_policy: Reports the wait before the next call.
        queuer: Queues the discovered URLs.
        producer: Parks the messages of URLs that could not be crawled.
        batch_size: The most messages one peek returns.
        idle_sleep_seconds: The wait after a peek returned nothing.
        reschedule_delay: The delay applied after a failed URL.
        re_crawl_interval: The re-crawl interval, or None for no re-crawl.
        time_provider: The only source of "now".
        logger: The injected logger.
        """
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
        self._time_provider = time_provider
        self._logger = logger

    async def run(self) -> None:
        """Read batches until cancelled, recording each one.

        `peek` is unguarded: the shipped in-memory reader cannot fail, and a
        guard for an extension nobody asked for is not added.

        Raises:
        asyncio.CancelledError: When cancelled, which is how the orchestrator
        stops the crawl.
        """
        self._logger.debug("worker reading the crawl queue")
        while True:
            batch = await self._reader.peek(self._batch_size)
            if not batch:
                # peek is CPU-only, so this sleep is the only yield on an empty
                # queue; without it the poller would starve.
                await asyncio.sleep(self._idle_sleep_seconds)
                continue
            await self._process_batch(batch)

    async def _process_batch(self, batch: list[BaseMessage]) -> None:
        """Crawl one batch concurrently, then record, queue, park, and commit it.

        One `now` is read before the task group opens and shared by every write,
        and the whole batch is marked started in one call before it opens.

        The calls after the group are ordered and each happens once.
        `complete_crawl` must come first, because `enqueue_urls` claims the rows
        it inserted and `commit` acknowledges work that is only finished once
        the outcome is durable.

        Raises:
        Exception: Nothing is raised
        """
        now = self._time_provider.now()
        await self._mark_started(batch, now)
        async with asyncio.TaskGroup() as group:
            crawls = [
                group.create_task(self._crawl_one(item.url, now)) for item in batch
            ]
        # Collected here so the tasks share no mutable state and batch order is
        # kept.
        outcomes: list[tuple[CustomURL, datetime | None]] = []
        discovered: list[CustomURL] = []
        failed: list[BaseMessage] = []
        for item, crawl in zip(batch, crawls):
            outcome, links, is_success = crawl.result()
            outcomes.append(outcome)
            discovered.extend(links)
            if not is_success:
                failed.append(item)
        try:
            await self._repository.complete_crawl(outcomes, discovered, now)
        except Exception as error:
            # The outcome was never recorded, so the rows stay started_crawl for
            # job_timeout to reclaim. Committing the messages is what stops the
            # queue handing this batch straight back, so every message in it is
            # deadlettered as best effort: the URLs are not lost, job_timeout
            # returns them, but the worker must not fetch them again now.
            self._logger.error(
                "recording the outcome of %d url(s) failed, so the batch is "
                "deadlettered and committed and its rows stay started_crawl for "
                "the job_timeout branch to reclaim: %s",
                len(outcomes),
                error,
            )
            await self._deadletter(batch)
            await self._reader.commit(batch)
            return
        # The rows complete_crawl inserted are already durable, so the next poll
        # claims the ones this feed misses. Losing it costs no work, which is
        # why a final failure is only a warning.
        await self._try_queue_call(
            lambda: self._queuer.enqueue_urls(discovered),
            "queueing %d discovered url(s) failed, so the next poll claims them "
            "from the store instead",
            len(discovered),
            level=logging.WARNING,
        )
        # Best effort: parking is a fallback, so a failure to park is logged and
        # the rest are still parked. No retry, since the shipped producer cannot
        # fail and a networked one reports a full queue as False.
        await self._deadletter(failed)
        # No retry policy here: commit is head-based, so re-issuing it would
        # remove more than the batch owns. The port documents that a networked
        # reader may raise, so the failure is caught and logged.
        await self._try_queue_call(
            lambda: self._reader.commit(batch),
            "committing %d message(s) failed, so they stay on the queue and are "
            "crawled again",
            len(batch),
        )

    async def _try_queue_call(
        self, call, template: str, count: int, level: int = logging.ERROR
    ) -> None:
        """Run one queue call, then log any failure at the given level.

        No retry policy: the implementation retries its own I/O internally, so
        this only catches what survives that.

        Args:
        call: The coroutine function to run.
        template: The failure message, with one %s for the error.
        count: How many URLs the call covered, for the message.
        level: The level to log a final failure at, ERROR by default.

        Raises:
        Exception: Nothing is raised
        """
        try:
            await call()
        except Exception as error:
            self._logger.log(level, template + ": %s", count, error)

    async def _mark_started(self, batch: list[BaseMessage], now: datetime) -> None:
        """Mark the whole batch started in one store call, and keep going.

        A failure leaves the rows claimable, which the `job_timeout` and
        `queue_timeout` branches then reconcile.

        Args:
        batch: The messages this iteration owns, whose URLs are all marked.
        now: The batch's single instant, the attempt time.

        Raises:
        Exception: Nothing is raised
        """
        try:
            await self._repository.mark_started([item.url for item in batch], now)
        except Exception as error:
            self._logger.error(
                "marking %d url(s) started at %s failed, so their rows stay "
                "claimable and a later claim re-queues them: %s",
                len(batch),
                now.isoformat(),
                error,
            )

    async def _deadletter(self, failed: list[BaseMessage]) -> None:
        """Park the batch's failed messages so no message is retried for ever.

        One message per call, the port's shape. A message that cannot be parked
        is logged and the rest are still parked; the caller commits either way.

        Args:
        failed: The messages whose URL did not crawl, possibly empty.

        Raises:
        Exception: Nothing is raised
        """
        for message in failed:
            try:
                parked = await self._producer.enqueue_to_deadletter(message)
            except Exception as error:
                self._logger.error(
                    "parking the failed message for %s failed, so it is dropped "
                    "with its batch committed: %s",
                    message.url.get_url(),
                    error,
                )
                continue
            if not parked:
                self._logger.error(
                    "the deadletter queue is full, so the failed message for %s "
                    "is dropped with its batch committed",
                    message.url.get_url(),
                )

    async def _crawl_one(self, url: CustomURL, now: datetime) -> _CrawlResult:
        """Crawl one URL, returning its outcome beside the URLs it revealed.

        Every step is inside the one handler, because the batch runs in a task
        group and a raise would take the whole batch down. Any failure is one
        failed URL: due again in `reschedule_delay`, and deadlettered by the
        caller. A politeness wait is never slept, only deferred, so one slow URL
        cannot stall the batch.

        Args:
        url: The URL this task owns, from one message's payload.
        now: The batch's single instant, shared by every write of this batch.

        Returns:
        _CrawlResult: The outcome to record, None scheduling no re-crawl; the
        same-host links the body revealed; and whether the URL was crawled. A
        deferral and a success yield True, a failure yields no links and False,
        which is what makes the caller deadletter the message.

        Raises:
        Exception: Nothing is raised
        """
        try:
            wait_ms = self._politeness_policy.before_fetch(url)
            if wait_ms > 0:
                return (url, now + timedelta(milliseconds=wait_ms)), [], True
            is_success = False
            try:
                html = await self._fetcher.fetch(url)
                is_success = True
            except Exception as error:
                # A handled failure, not an abort: the row becomes due again and
                # the caller deadletters the message.
                self._logger.error(
                    "fetching %s failed once its retries were spent, so it is due "
                    "again in %s: %s",
                    url.get_url(),
                    self._reschedule_delay,
                    error,
                )
            finally:
                # One outcome per attempt, given back even when it failed, so a
                # learning policy can back off from it.
                self._politeness_policy.record_fetch(
                    now, url, BaseResult(is_success=is_success)
                )
            if not is_success:
                return (url, now + self._reschedule_delay), [], False
            links = self._link_extractor.extract(html, url)
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
            return (url, next_crawl_time), links, True
        except Exception as error:
            self._logger.error(
                "crawling %s failed, so it is due again in %s and its message is "
                "deadlettered: %s",
                url.get_url(),
                self._reschedule_delay,
                error,
            )
            return (url, now + self._reschedule_delay), [], False
