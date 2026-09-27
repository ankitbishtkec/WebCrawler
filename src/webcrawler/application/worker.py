"""The crawl worker: the consumer half of the crawl loop (goal.md:130-150).

`goal.md:130-150` describes the worker as a queue reader that marks one URL as
started, fetches it subject to a politeness policy, parses the body, and only
then writes the outcome, queues the URLs it discovered, and commits the
messages it consumed. Every collaborator is a port, so this module names no
concrete class: the same loop runs against SQLite and HTTP in production and
against in-memory doubles in the tests.

Three structural choices carry the loop, and each one is a decision rather than
an obvious line of code:

- One batch per iteration and exactly one `commit` per batch. The reader's
`commit` is head-based and non-idempotent (`goal.md:127`), so a second
commit of the same batch would discard messages nobody processed.
- All the per-URL work of a batch runs inside one `asyncio.TaskGroup`
(`goal.md:139`), so every URL is in flight at once and one uncaught failure
aborts the whole batch instead of leaving it half recorded.
- The batch's outcome and discovery lists are filled from the finished tasks
after the group closes, never from inside it, so the concurrent tasks never
share mutable state and the two lists keep the batch's order.

`CrawlerWorker` has no project base class: it is a consumer loop composed from
ports, and nothing is shared with it by inheritance ( goal.md:8).
"""

import asyncio
import logging
from datetime import datetime, timedelta

from webcrawler.domain.custom_url import CustomURL
from webcrawler.domain.messages import BaseMessage
from webcrawler.ports.crawl_queuer import CrawlQueuer
from webcrawler.ports.link_extractor import LinkExtractor
from webcrawler.ports.politeness_policy import PolitenessPolicy
from webcrawler.ports.retry_policy import RetryPolicy
from webcrawler.ports.time_provider import TimeProviderFactory
from webcrawler.ports.topic_reader import TopicReader
from webcrawler.ports.url_state_repository import URLStateRepository
from webcrawler.ports.web_page_fetcher import WebPageFetcher

# One URL's recorded outcome, a (url, next_crawl_time) pair, beside the URLs
# its body revealed.
_CrawlResult = tuple[tuple[CustomURL, datetime | None], list[CustomURL]]

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
    retry_policy: The one policy instance handed to every `fetch` call, so
    the worker's attempts retry with the process-wide settings
    (goal.md:17).
    batch_size: How many messages one `peek` may return, and so the most
    URLs a single task group can hold.
    idle_sleep_seconds: How long to wait after a `peek` returned nothing,
    which is the only backpressure the loop has against a real clock.
    reschedule_delay: How far ahead a URL whose fetch failed after its
    retries is scheduled again.
    sleep_threshold_ms: The largest politeness wait this worker sleeps
    through. A longer one is deferred to the URL's own schedule, since
    sleeping it would stall the batch and requeue that URL forever.
    re_crawl_interval: The interval at which a successfully crawled URL
    becomes due again, or None to schedule no re-crawl at all.
    time_provider: The only source of "now" for any write.
    logger: The injected logger. The assignment and every visited page are
    INFO, because they are the crawl's own output (`goal.md:1`).
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
        batch_size: int,
        idle_sleep_seconds: float = 1.0,
        reschedule_delay: timedelta = timedelta(minutes=1),
        sleep_threshold_ms: int = 2_000,
        re_crawl_interval: timedelta | None = None,
        time_provider: TimeProviderFactory,
        logger: logging.Logger) -> None:
        """Hold the ports and the schedule this worker was configured with.

        Nothing is validated and nothing is started: the configuration is the
        composition root's decision, and an unusable value
        surfaces as a scheduled time nobody can meet rather than as a
        construction error this class would have to second-guess.

        Args:
        repository: The crawl state store.
        reader: The queue's read side, the only source of work.
        fetcher: Retrieves one page body under a supplied policy.
        link_extractor: Parses a body into same-host links.
        politeness_policy: Reports the wait before the next call.
        queuer: Queues the discovered URLs.
        retry_policy: The instance passed to every `fetch` call.
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
        hold. A peek that returns nothing waits `idle_sleep_seconds` rather than
        yielding to the loop in a spin, so an idle crawl costs one timer instead
        of the whole event loop's attention.

        Raises:
        Exception: Anything raised by the reader, the per-URL collaborators,
        or the batch's own bookkeeping, propagated unchanged. An
        uncaught per-URL failure is deliberately not swallowed: it
        aborts the batch so it is re-read rather than half recorded.
        asyncio.CancelledError: When the task is cancelled, which is how
        the orchestrator stops the crawl.
        """
        self._logger.info("worker reading the crawl queue")
        while True:
            batch = await self._reader.peek(self._batch_size())
            if not batch:
                # peek is CPU-only, so awaiting it does not yield; this sleep is
                # the only yield on an empty queue, or the poller would starve.
                await asyncio.sleep(self._idle_sleep_seconds())
                continue
                await self._process_batch(batch)

                async def _process_batch(self, batch: list[BaseMessage]) -> None:
                    """Crawl one batch concurrently, then record, queue, and commit it once.

                    The batch's single instant is read before the task group opens, so the
                    per-URL work and the completion write share one `now` and the clock is
                    consulted once per batch rather than once per URL.

                    The three calls after the task group are ordered and each happens once.
                    `complete_crawl` must come first because the two following calls both
                    depend on its rows existing: `enqueue_urls` claims the discovered rows
                    and would claim nothing without the insert, and `commit` acknowledges
                    messages whose work is only safely finished once the outcome is durable.

                    A `complete_crawl` failure is handled rather than propagated, because the
                    DB write that would have recorded the outcome is the write that failed:
                    there is nothing to record anywhere else. The batch is abandoned
                    uncommitted, so its rows stay `started_crawl` and the `job_timeout`
                    branch reclaims them once this worker moves on. The
                    failure is per batch, so the loop continues with the next peeked batch
                    rather than stopping the worker.

                    Args:
                    batch: The messages this iteration owns, which are the messages the
                    final `commit` acknowledges.

                    Raises:
                    ExceptionGroup: Whatever the per-URL tasks raised, as one group, so
                    a single unhandled failure aborts the whole batch. Nothing is
                    recorded, queued, or committed in that case.
                    Exception: Whatever `enqueue_urls` or `commit` raises, propagated
                    after the outcomes were already written, so the batch is
                    re-read and its outcomes recorded again idempotently.
                    """
                    now = self._time_provider.now
                    async with asyncio.TaskGroup() as group:
                        crawls = [
                            group.create_task(self._crawl_one(item.url, now)) for item in batch
                            ]
                        # Reading the results here keeps both lists owned by this coroutine, so
                        # the tasks never share mutable state and the batch's order is kept.
                        outcomes: list[tuple[CustomURL, datetime | None]] = []
                        discovered: list[CustomURL] = []
                        for crawl in crawls:
                            outcome, links = crawl.result
                            outcomes.append(outcome)
                            discovered.extend(links)
                            try:
                                await self._repository.complete_crawl(outcomes, discovered, now)
                            except Exception as error:
                                # The rows stay started_crawl for the job_timeout branch to
                                # reclaim; committing would acknowledge unrecorded work.
                                self._logger.error(
                                    "recording the outcome of %d url(s) failed, so the batch is left "
                                    "uncommitted and its rows stay started_crawl for the job_timeout "
                                    "branch to reclaim: %s",
                                    len(outcomes),
                                    error)
                                return
                                # enqueue_urls claims the rows complete_crawl inserted, so it must
                                # follow that transaction or the claim finds nothing to move.
                                await self._queuer.enqueue_urls(discovered)
                                await self._reader.commit(batch)

                                async def _crawl_one(self, url: CustomURL, now: datetime) -> _CrawlResult:
                                    """Crawl one URL and return its outcome beside the URLs it revealed.

                                    The order is the plan's: mark started, ask the politeness policy, fetch,
                                    store, extract, report. Marking first is what makes the row reclaimable
                                    by the `job_timeout` branch if this worker dies before the outcome is
                                    written (`goal.md:135-137`).

                                    The batch's single instant is passed in rather than read here, so every
                                    schedule this task records is the instant the batch began at and a
                                    batch of concurrent tasks can never disagree about when it ran.

                                    The politeness decision is resolved here, per URL, because the policy
                                    reports a wait rather than performing one: a wait at or below the
                                    threshold is slept through and the crawl proceeds, while a longer wait
                                    is recorded as the URL's own `next_crawl_time` and the URL is skipped. Skipping is what keeps a wait above the threshold from
                                    stalling the whole batch and requeueing the same URL indefinitely.

                                    Only a failed fetch is handled here. An extractor failure is left to
                                    propagate, because an outcome recorded after a lost body would claim
                                    a crawl that did not finish.

                                    Args:
                                    url: The URL this task owns, taken from one message's payload.
                                    now: The batch's single instant, shared by every mark, outcome, and
                                    the completion write of this batch.

                                    Returns:
                                    _CrawlResult: The `(url, next_crawl_time)` pair to record, where
                                    None schedules no re-crawl, and the same-host URLs the body
                                    revealed. A success yields the links, while a failed fetch and
                                    a politeness skip yield none.

                                    Raises:
                                    Exception: Whatever `mark_started`, `fetch`, `save`, or `extract`
                                    raises other than a handled fetch failure; the enclosing task
                                    group turns it into an abort of the whole batch.
                                    """
                                    await self._repository.mark_started(url, now)
                                    wait_ms = self._politeness_policy.before_fetch
                                    if wait_ms > self._sleep_threshold_ms:
                                        return (
                                            url,
                                            now + timedelta(milliseconds=wait_ms)), []
                                        if wait_ms > 0:
                                            await asyncio.sleep(wait_ms / 1000)
                                            try:
                                                html = await self._fetcher.fetch(url, self._retry_policy)
                                            except Exception as error:
                                                self._logger.info(
                                                    "fetching %s failed once its retries were spent, so it is due "
                                                    "again in %s: %s",
                                                    url.get_url,
                                                    self._reschedule_delay,
                                                    error)
                                                return (url, now + self._reschedule_delay), []
                                                links = self._link_extractor.extract(html, url)
                                                self._logger.info(
                                                    "visited %s, found %d link(s): %s",
                                                    url.get_url,
                                                    len(links),
                                                    [link.get_url for link in links])
                                                # Compared against None, not truthiness, so a configured zero still
                                                # schedules a re-crawl.
                                                next_crawl_time = (
                                                    None
                                                    if self._re_crawl_interval is None
                                                    else now + self._re_crawl_interval
                                                    )
                                                return (url, next_crawl_time), links
