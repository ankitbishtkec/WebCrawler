"""The crawl worker: the consumer half of the crawl loop (goal.md).

`goal.md` describes the worker as a queue reader that marks one URL as
started, fetches it subject to a politeness policy, parses the body, and only
then writes the outcome, queues the URLs it discovered, and commits the
messages it consumed. Every collaborator is a port/ interface/ abstract class, 
so this module names no concrete class: the same loop runs against SQLite and 
HTTP in production and against in-memory doubles in the tests.


`CrawlerWorker` has no project base class: it is a consumer loop composed from
ports, and nothing is shared with it by inheritance ( goal.md).

No exception escapes the loop. A per-URL failure is one failed URL, and a
failed batch I/O call is one logged step, so neither a parse error nor a store
outage can kill the worker and stop the crawl.
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
from webcrawler.ports.retry_policy import RetryPolicy
from webcrawler.ports.time_provider import TimeProviderFactory
from webcrawler.ports.topic_producer import TopicProducer
from webcrawler.ports.topic_reader import TopicReader
from webcrawler.ports.url_state_repository import URLStateRepository
from webcrawler.ports.web_page_fetcher import WebPageFetcher

# One URL's recorded outcome, a (url, next_crawl_time) pair, beside the URLs
# its body revealed and whether the URL was crawled: a False third element is
# what makes the caller deadletter the URL's message.
_CrawlResult = tuple[tuple[CustomURL, datetime | None], list[CustomURL], bool]

class CrawlerWorker:
    """Consumes the crawl queue one batch at a time, forever.

    The loop is unbounded: `run` returns only when the task is cancelled,
    because a crawl ends when the operator interrupts it, not when the queue
    happens to be empty.

    One instant is read per batch and every write in that batch shares it, so a
    batch cannot record two instants that disagree and the clock is consulted
    once however many URLs the batch holds.

    Args:
    repository: The crawl state store, the only way a status is written.
    reader: The queue's read side, the only source of work.
    fetcher: Retrieves one page body, applying the retry policy per call.
    link_extractor: Parses a body into the same-host URLs to crawl next.
    politeness_policy: Reports how many milliseconds to wait before the next
    call; it never sleeps, because only this worker knows its threshold.
    queuer: Queues the discovered URLs immediately, once the rows they
    belong to have been written.
    producer: Parks a message whose URL could not be crawled, so the batch is
    committed rather than retried for ever.
    retry_policy: The one policy instance handed to every `fetch` call, so
    the worker's attempts retry with the process-wide settings
    (goal.md:17).
    batch_size: How many messages one `peek` may return, and so the most
    URLs a single task group can hold.
    idle_sleep_seconds: How long to wait after a `peek` returned nothing.
    reschedule_delay: How far ahead a URL whose fetch failed after its
    retries is scheduled again.
    sleep_threshold_ms: The largest politeness wait this worker sleeps
    through. A longer one is deferred to the URL's own schedule, since
    sleeping it would stall the batch and requeue that URL forever.
    re_crawl_interval: The interval at which a successfully crawled URL
    becomes due again, or None to schedule no re-crawl at all.
    time_provider: The only source of "now" for any write.
    logger: The injected logger. A visited page and its found links are INFO,
    because they are the crawl's own output (`goal.md:1`).
    """

    def __init__(
        self,
        repository: URLStateRepository,
        reader: TopicReader,
        fetcher: WebPageFetcher,
        link_extractor: LinkExtractor,
        politeness_policy: PolitenessPolicy,
        queuer: CrawlQueuer,
        retry_policy: RetryPolicy,
        *,
        producer: TopicProducer,
        batch_size: int,
        idle_sleep_seconds: float = 1.0,
        reschedule_delay: timedelta = timedelta(minutes=1),
        sleep_threshold_ms: int = 2_000,
        re_crawl_interval: timedelta | None = None,
        time_provider: TimeProviderFactory,
        logger: logging.Logger) -> None:
        """Hold the ports and the schedule this worker was configured with.

        Nothing is validated and nothing is initialized here except for the assignment

        Args:
        repository: The crawl state store.
        reader: The queue's read side, the only source of work.
        fetcher: Retrieves one page body under a supplied policy.
        link_extractor: Parses a body into same-host links.
        politeness_policy: Reports the wait before the next call.
        queuer: Queues the discovered URLs.
        retry_policy: The instance passed to every `fetch` call.
        producer: Parks the messages of the URLs that could not be crawled.
        batch_size: The largest number of messages one peek may return.
        idle_sleep_seconds: The wait after a peek returned nothing.
        reschedule_delay: The delay applied after a failed fetch.
        sleep_threshold_ms: The largest politeness wait that is slept
        through; a longer one is deferred on the URL's schedule.
        re_crawl_interval: The re-crawl interval for a success, or None for
        no re-crawl.
        time_provider: The only source of "now".
        logger: The injected logger.
        """
        self._repository = repository
        self._reader = reader
        self._fetcher = fetcher
        self._link_extractor = link_extractor
        self._politeness_policy = politeness_policy
        self._queuer = queuer
        self._retry_policy = retry_policy
        self._producer = producer
        self._batch_size = batch_size
        self._idle_sleep_seconds = idle_sleep_seconds
        self._reschedule_delay = reschedule_delay
        self._sleep_threshold_ms = sleep_threshold_ms
        self._re_crawl_interval = re_crawl_interval
        self._time_provider = time_provider
        self._logger = logger

    async def run(self) -> None:
        """Read batches until the task is cancelled, recording each one.

        The reader is wired by its constructor, so the loop starts peeking
        straight away; there is no connect step and no partition assignment to
        hold.

        The peek is the one call this loop makes outside a handler, because a
        failed read is nothing to do with the batch behind it: it is logged and
        the next iteration tries again, so the loop never ends on its own.

        Raises:
        asyncio.CancelledError: When the task is cancelled, which is how
        the orchestrator stops the crawl.
        """
        self._logger.debug("worker reading the crawl queue")
        while True:
            try:
                batch = await self._reader.peek(self._batch_size)
            except Exception as error:
                self._logger.error(
                    "reading the crawl queue failed, so this iteration is "
                    "skipped and the next one tries again: %s",
                    error,
                )
                batch = []
            if not batch:
                # peek is CPU-only, so awaiting it does not yield; this sleep is
                # the only yield on an empty queue, or the poller would starve.
                await asyncio.sleep(self._idle_sleep_seconds)
                continue
            await self._process_batch(batch)

    async def _process_batch(self, batch: list[BaseMessage]) -> None:
        """Crawl one batch concurrently, then record, queue, park, and commit it.

        The batch's single instant is read before the task group opens, so the
        per-URL work and the completion write share one `now` and the clock is
        consulted once per batch rather than once per URL. The whole batch is
        marked started in one call before the group opens, because the store
        updates many rows in one statement.

        Every call after the group is ordered and each happens once.
        `complete_crawl` must come first because the two following calls both
        depend on its rows existing: `enqueue_urls` claims the discovered rows
        and would claim nothing without the insert, and `commit` acknowledges
        messages whose work is only safely finished once the outcome is durable.

        A `complete_crawl` failure is handled rather than propagated, because the
        DB write that would have recorded the outcome is the write that failed:
        there is nothing to record anywhere else. The batch is abandoned
        uncommitted, so its rows stay `started_crawl` and the `job_timeout`
        branch reclaims them once this worker moves on. Every other failure is
        handled per call, so the loop continues with the next peeked batch
        rather than stopping the worker.

        Args:
        batch: The messages this iteration owns, which are the messages the
        final `commit` acknowledges.

        Raises:
        Exception: Nothing is raised
        """
        now = self._time_provider.now()
        await self._mark_started(batch, now)
        async with asyncio.TaskGroup() as group:
            crawls = [
                group.create_task(self._crawl_one(item.url, now)) for item in batch
            ]
        # Reading the results here keeps both lists owned by this coroutine, so
        # the tasks never share mutable state and the batch's order is kept.
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
            # The rows stay started_crawl for the job_timeout branch to reclaim;
            # committing would acknowledge work that was never recorded.
            self._logger.error(
                "recording the outcome of %d url(s) failed, so the batch is left "
                "uncommitted and its rows stay started_crawl for the job_timeout "
                "branch to reclaim: %s",
                len(outcomes),
                error,
            )
            return
        # enqueue_urls claims the rows complete_crawl inserted, so it must follow
        # that transaction or the claim finds nothing to move.
        try:
            await self._queuer.enqueue_urls(discovered)
        except Exception as error:
            # The discovered rows are durable, so the next poll claims them from
            # the store; losing this feed costs no work.
            self._logger.warning(
                "queueing %d discovered url(s) failed, so the next poll claims "
                "them from the store instead: %s",
                len(discovered),
                error,
            )
        await self._deadletter(failed)
        try:
            await self._reader.commit(batch)
        except Exception as error:
            # The messages are still on the queue, so they are crawled again.
            self._logger.error(
                "committing %d message(s) failed, so they stay on the queue and "
                "are crawled again: %s",
                len(batch),
                error,
            )

    async def _mark_started(self, batch: list[BaseMessage], now: datetime) -> None:
        """Mark the whole batch started in one store call, and keep going.

        The write is the worker's own attempt time, not an outcome, so a row
        stays claimable while the attempt runs. A failure therefore leaves the
        rows claimable and the crawl still runs, which the `job_timeout` and
        `queue_timeout` branches then reconcile.

        Args:
        batch: The messages this iteration owns, whose URLs are all marked.
        now: The batch's single instant, the attempt time.

        Raises:
        Exception: Nothing is raised
        """
        try:
            await self._repository.mark_started(
                [item.url for item in batch], now)
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

        One message per call, because that is the port's shape. A message that
        cannot be parked is logged and the rest are still parked: the caller
        commits the batch either way, so a failed crawl is never seen again.

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
                    "parking the failed message for %s failed, so it is "
                    "dropped with its batch committed: %s",
                    message.url.get_url(),
                    error,
                )
                continue
            if not parked:
                self._logger.error(
                    "the deadletter queue is full, so the failed message for "
                    "%s is dropped with its batch committed",
                    message.url.get_url(),
                )

    async def _crawl_one(self, url: CustomURL, now: datetime) -> _CrawlResult:
        """Crawl one URL and return its outcome beside the URLs it revealed.

        Every step of one URL is inside the one handler, because the batch runs
        in a task group and an exception would take the whole batch down with
        it. A failure of any kind — a fetch, a parse, an extract, a store write,
        a programming error — is one failed URL: it becomes due again in
        `reschedule_delay` and its message is deadlettered by the caller.

        Args:
        url: The URL this task owns, taken from one message's payload.
        now: The batch's single instant, shared by every mark, outcome, and
        the completion write of this batch.

        Returns:
        _CrawlResult: The `(url, next_crawl_time)` pair to record, where
        None schedules no re-crawl; the same-host URLs the body revealed;
        and whether the URL was crawled. A success and a politeness skip yield
        True, while a failed fetch and an unexpected failure yield no links and
        False, which is what makes the caller deadletter the message.

        Raises:
        Exception: Nothing is raised
        """
        try:
            wait_ms = self._politeness_policy.before_fetch(url)
            if wait_ms > self._sleep_threshold_ms:
                return (
                    url,
                    now + timedelta(milliseconds=wait_ms)), [], True
            if wait_ms > 0:
                await asyncio.sleep(wait_ms / 1000)
            is_success = False
            try:
                html = await self._fetcher.fetch(url, self._retry_policy)
                is_success = True
            except Exception as error:
                # A handled failure, not an abort: the row becomes due again in
                # reschedule_delay and the caller deadletters the message.
                self._logger.error(
                    "fetching %s failed once its retries were spent, so it is due "
                    "again in %s: %s",
                    url.get_url(),
                    self._reschedule_delay,
                    error,
                )
            finally:
                # One outcome per attempt, given back even when the attempt
                # failed, so a learning policy can back off from it.
                self._politeness_policy.record_fetch(
                    now, url, BaseResult(is_success=is_success))
            if not is_success:
                return (url, now + self._reschedule_delay), [], False
            links = self._link_extractor.extract(html, url)
            self._logger.info(
                "visited %s, found %d link(s): %s",
                url.get_url(),
                len(links),
                [link.get_url() for link in links])
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
                "crawling %s failed, so it is due again in %s and its message "
                "is deadlettered: %s",
                url.get_url(),
                self._reschedule_delay,
                error,
            )
            return (url, now + self._reschedule_delay), [], False
