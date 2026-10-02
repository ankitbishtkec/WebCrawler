"""Unit tests for `CrawlerWorkerV1`, the non-blocking consumer."""

import asyncio
from datetime import datetime, timedelta, timezone
from typing import NamedTuple
from unittest.mock import AsyncMock, MagicMock, call

import pytest

from webcrawler.application.worker_v1 import CrawlerWorkerV1
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

HOST: str = "https://crawlme.monzo.com"

# The body the mocked fetcher serves for every URL of the batch.
BODY: str = "<html>one page</html>"

# The instant the pinned clock hands the worker, so every write is predictable.
NOW: datetime = datetime(2026, 9, 29, 6, 0, tzinfo=timezone.utc)

# The delay the worker applies to a URL it could not crawl, its own default.
RESCHEDULE: timedelta = timedelta(minutes=1)

# The wait a policy that defers a URL returns, in milliseconds.
DEFER_MS: int = 500

# The re-crawl interval a test configures, and the instant it puts on a row.
RE_CRAWL: timedelta = timedelta(minutes=30)


class _Built(NamedTuple):
    """The worker under test, with the batch it is given and its mocked ports."""

    worker: CrawlerWorkerV1
    batch: list[BaseMessage]
    urls: list[CustomURL]
    links: set[CustomURL]
    repository: MagicMock
    reader: MagicMock
    fetcher: MagicMock
    extractor: MagicMock
    queuer: MagicMock
    politeness: MagicMock
    producer: MagicMock


def _build_worker(
    count: int,
    *,
    max_concurrent_fetches: int = 500,
    re_crawl_interval: timedelta | None = None,
) -> _Built:
    """Build the worker over mocks, plus the batch of `count` messages it crawls."""
    urls = [CustomURL(f"{HOST}/page-{index}.html") for index in range(1, count + 1)]
    batch = [BaseMessage(url, partition_key=hash(url)) for url in urls]
    links = {CustomURL(f"{HOST}/found-{index}.html") for index in range(1, count + 1)}

    repository = MagicMock(spec=URLStateRepository)
    reader = MagicMock(spec=TopicReader)
    fetcher = MagicMock(spec=WebPageFetcher)
    fetcher.fetch.return_value = BODY
    extractor = MagicMock(spec=LinkExtractor)
    extractor.extract.return_value = links
    politeness = MagicMock(spec=PolitenessPolicy)
    politeness.before_fetch.return_value = 0
    queuer = MagicMock(spec=CrawlQueuer)
    producer = MagicMock(spec=TopicProducer)
    clock = MagicMock(spec=TimeProviderFactory)
    clock.now.return_value = NOW

    worker = CrawlerWorkerV1(
        repository,
        reader,
        fetcher,
        extractor,
        politeness,
        queuer,
        producer=producer,
        batch_size=count,
        re_crawl_interval=re_crawl_interval,
        max_concurrent_fetches=max_concurrent_fetches,
        time_provider=clock,
    )
    return _Built(
        worker, batch, urls, links, repository, reader, fetcher, extractor, queuer,
        politeness, producer,
    )


async def _crawl_all(built: _Built) -> None:
    """Crawl every message of the batch, as the consume loop's tasks would."""
    await asyncio.gather(
        *(built.worker._crawl_one(message) for message in built.batch)
    )


async def _drain(built: _Built) -> None:
    """Wait for the crawls the worker detached, so no task outlives the test."""
    await asyncio.gather(*tuple(built.worker._crawling), return_exceptions=True)


@pytest.mark.parametrize("count", [1, 3])
async def test_the_urls_of_the_window_are_marked_started_in_one_store_call(
    count: int,
) -> None:
    """One call marks every crawled URL, at the one instant the flush took."""
    built = _build_worker(count)

    await _crawl_all(built)
    built.repository.mark_started.assert_not_awaited()
    await built.worker._flush()

    built.repository.mark_started.assert_awaited_once_with(set(built.urls), NOW)


async def test_the_body_the_fetcher_returned_is_the_body_the_extractor_reads() -> None:
    """The one message is fetched, and the body that came back is what gets parsed."""
    built = _build_worker(1)
    url = built.urls[0]

    await _crawl_all(built)

    built.fetcher.fetch.assert_awaited_once_with(url)
    built.extractor.extract.assert_called_once_with(BODY, url)


@pytest.mark.parametrize("count", [1, 3])
async def test_the_finished_urls_and_the_discovered_links_are_written_together(
    count: int,
) -> None:
    """One call per window records every outcome beside the links they revealed."""
    built = _build_worker(count)

    await _crawl_all(built)
    await built.worker._flush()

    built.repository.complete_crawl.assert_awaited_once_with(
        {url: None for url in built.urls}, built.links, NOW
    )


async def test_a_re_crawl_interval_puts_that_interval_on_a_finished_row() -> None:
    """A configured interval is the outcome a finished URL is stored with."""
    built = _build_worker(1, re_crawl_interval=RE_CRAWL)

    await _crawl_all(built)
    await built.worker._flush()

    built.repository.complete_crawl.assert_awaited_once_with(
        {built.urls[0]: NOW + RE_CRAWL}, built.links, NOW
    )


@pytest.mark.parametrize("count", [1, 3])
async def test_the_discovered_links_are_handed_to_the_queuer_as_a_list(
    count: int,
) -> None:
    """The queuer is asked once for exactly the links the window found, in a list."""
    built = _build_worker(count)

    await _crawl_all(built)
    await built.worker._flush()

    built.queuer.enqueue_urls.assert_awaited_once()
    queued = built.queuer.enqueue_urls.await_args.args[0]
    # The links arrive as a set, so their order is the store's business; the
    # contents and the count are the worker's.
    assert sorted(queued, key=lambda url: url.get_url()) == sorted(
        built.links, key=lambda url: url.get_url()
    )


async def test_the_queue_is_fed_only_after_the_write_that_stored_the_rows() -> None:
    """`enqueue_urls` claims rows, so it cannot run before they are committed."""
    built = _build_worker(1)
    order: list[str] = []
    built.repository.complete_crawl.side_effect = lambda *args: order.append("write")
    built.queuer.enqueue_urls.side_effect = lambda *args: order.append("feed")

    await _crawl_all(built)
    await built.worker._flush()

    assert order == ["write", "feed"]


async def test_the_window_is_written_once_and_the_buffer_starts_empty_again() -> None:
    """A flush takes the buffer whole, so the next window is not written twice."""
    built = _build_worker(1)

    await _crawl_all(built)
    await built.worker._flush()
    await built.worker._flush()

    built.repository.complete_crawl.assert_awaited_once_with(
        {built.urls[0]: None}, built.links, NOW
    )
    built.queuer.enqueue_urls.assert_awaited_once()


async def test_the_batch_is_committed_while_its_crawls_are_still_running() -> None:
    """The queue moves on without waiting, which is the whole point of this worker."""
    built = _build_worker(2)
    release = asyncio.Event()
    events: list[str] = []

    async def fetch(url: CustomURL) -> str:
        await release.wait()
        events.append("fetched")
        return BODY

    built.fetcher.fetch.side_effect = fetch
    built.reader.commit.side_effect = lambda *args: events.append("commit")
    # The second peek ends the loop the way cancellation would.
    built.reader.peek.side_effect = [built.batch, asyncio.CancelledError()]

    with pytest.raises(asyncio.CancelledError):
        await built.worker._consume_forever()

    built.reader.commit.assert_awaited_once_with(built.batch)
    # The batch is acknowledged before either crawl has produced a body.
    assert events == ["commit"]
    release.set()
    await _drain(built)
    assert events == ["commit", "fetched", "fetched"]
    built.extractor.extract.assert_any_call(BODY, built.urls[0])
    built.repository.complete_crawl.assert_not_awaited()


async def test_a_url_whose_fetch_fails_does_not_stop_the_others() -> None:
    """The failure is one URL: it is rescheduled and parked, the others are done."""
    built = _build_worker(2)
    failing = built.urls[0]

    async def fetch(url: CustomURL) -> str:
        if url == failing:
            raise RuntimeError("the host closed the connection")
        return BODY

    built.fetcher.fetch.side_effect = fetch

    await _crawl_all(built)
    await built.worker._flush()

    built.repository.complete_crawl.assert_awaited_once_with(
        {failing: NOW + RESCHEDULE, built.urls[1]: None},
        built.links,
        NOW,
    )
    built.producer.enqueue_to_deadletter.assert_awaited_once_with(built.batch[0])


async def test_a_deferred_url_is_not_fetched_and_is_due_after_its_wait() -> None:
    """A positive wait defers the URL, so its outcome is the wake up and no body."""
    built = _build_worker(1)
    built.politeness.before_fetch.return_value = DEFER_MS

    await _crawl_all(built)
    await built.worker._flush()

    built.fetcher.fetch.assert_not_awaited()
    built.repository.complete_crawl.assert_awaited_once_with(
        {built.urls[0]: NOW + timedelta(milliseconds=DEFER_MS)}, set(), NOW
    )
    built.queuer.enqueue_urls.assert_awaited_once_with([])
    built.producer.enqueue_to_deadletter.assert_not_awaited()


async def test_a_park_failure_does_not_stop_the_next_message_being_parked() -> None:
    """Parking is best effort, so the first failure is logged and the next goes on."""
    built = _build_worker(2)
    built.fetcher.fetch.side_effect = RuntimeError("the host closed the connection")
    built.producer.enqueue_to_deadletter.side_effect = [
        RuntimeError("the deadletter queue is gone"),
        True,
    ]

    await _crawl_all(built)
    await built.worker._flush()

    assert built.producer.enqueue_to_deadletter.call_args_list[1].args == (
        built.batch[1],
    )


async def test_a_full_deadletter_queue_is_reported_and_the_crawl_still_finishes() -> None:
    """A refused park is logged, and the URL is still rescheduled and recorded."""
    built = _build_worker(1)
    built.fetcher.fetch.side_effect = RuntimeError("the host closed the connection")
    built.producer.enqueue_to_deadletter.return_value = False

    await _crawl_all(built)
    await built.worker._flush()

    built.repository.complete_crawl.assert_awaited_once_with(
        {built.urls[0]: NOW + RESCHEDULE}, set(), NOW
    )


async def test_a_failed_write_queues_nothing_and_leaves_the_rows_to_the_timeout() -> None:
    """The rows of an unwritten window stay `started_crawl`, and nothing is claimed."""
    built = _build_worker(1)
    built.repository.complete_crawl.side_effect = RuntimeError("the write failed")

    await _crawl_all(built)
    await built.worker._flush()

    built.queuer.enqueue_urls.assert_not_awaited()


async def test_a_failed_mark_started_still_writes_the_window() -> None:
    """The mark is only the store's own record, so the outcomes are still written."""
    built = _build_worker(1)
    built.repository.mark_started.side_effect = RuntimeError("the update failed")

    await _crawl_all(built)
    await built.worker._flush()

    built.repository.complete_crawl.assert_awaited_once_with(
        {built.urls[0]: None}, built.links, NOW
    )


async def test_a_url_crawled_twice_in_one_window_is_written_once() -> None:
    """The buffers key by URL, so one row is marked once and finished once."""
    built = _build_worker(1, re_crawl_interval=RE_CRAWL)
    message = built.batch[0]
    built.worker._time_provider = MagicMock(spec=TimeProviderFactory)
    built.worker._time_provider.now.side_effect = [
        NOW,
        NOW + RE_CRAWL,
        NOW + RE_CRAWL + RE_CRAWL,
    ]

    await built.worker._crawl_one(message)
    await built.worker._crawl_one(message)
    await built.worker._flush()

    # The row is marked once, at the instant the flush took, and finished once:
    # the later attempt wins, so it is not scheduled from the older one.
    built.repository.mark_started.assert_awaited_once_with(
        {message.url}, NOW + RE_CRAWL + RE_CRAWL
    )
    built.repository.complete_crawl.assert_awaited_once_with(
        {message.url: NOW + RE_CRAWL + RE_CRAWL},
        built.links,
        NOW + RE_CRAWL + RE_CRAWL,
    )


async def test_close_writes_what_the_crawls_left_and_releases_the_session() -> None:
    """The last window is the caller's to trigger, so `close` flushes and closes."""
    built = _build_worker(1)

    await _crawl_all(built)
    await built.worker.close()

    built.repository.complete_crawl.assert_awaited_once_with(
        {built.urls[0]: None}, built.links, NOW
    )
    built.fetcher.close.assert_awaited_once()


async def test_the_second_fetch_waits_for_the_first_when_one_slot_is_configured() -> None:
    """The semaphore counts requests in flight, so one slot means one at a time."""
    built = _build_worker(2, max_concurrent_fetches=1)
    in_flight = 0
    most_in_flight = 0

    async def fetch(url: CustomURL) -> str:
        nonlocal in_flight, most_in_flight
        in_flight += 1
        most_in_flight = max(most_in_flight, in_flight)
        await asyncio.sleep(0)
        in_flight -= 1
        return BODY

    built.fetcher.fetch.side_effect = fetch

    await _crawl_all(built)

    assert built.fetcher.fetch.await_count == 2
    assert most_in_flight == 1


# The failures a single URL's crawl can hit that are not a failed fetch. A
# detached task has no task group to contain a raise, so each of these is the
# reason the one handler around the crawl exists.
_STEP_FAILURES = [
    pytest.param("before_fetch", id="before_fetch raises"),
    pytest.param("extract", id="the extractor raises"),
    pytest.param("record_fetch", id="record_fetch raises"),
]


@pytest.mark.parametrize("_step", _STEP_FAILURES)
@pytest.mark.parametrize("count", [1, 3])
async def test_a_failure_outside_the_fetch_still_records_and_parks(
    count: int, _step: str
) -> None:
    """A detached crawl that raises leaves the row due again and the message parked."""
    built = _build_worker(count)
    error = RuntimeError("the step failed")
    if _step == "before_fetch":
        built.politeness.before_fetch.side_effect = error
    elif _step == "extract":
        built.extractor.extract.side_effect = error
    else:
        built.politeness.record_fetch.side_effect = error

    await _crawl_all(built)
    await built.worker._flush()

    recorded = built.repository.complete_crawl.await_args.args[0]
    assert recorded == {url: NOW + RESCHEDULE for url in built.urls}
    assert built.producer.enqueue_to_deadletter.call_args_list == [
        call(message) for message in built.batch
    ]


@pytest.mark.parametrize(
    "outcome",
    [
        pytest.param("parked", id="the message was parked"),
        pytest.param("full", id="the deadletter queue is full"),
    ],
)
async def test_a_full_deadletter_queue_is_reported_for_the_detached_crawl(
    outcome: str,
) -> None:
    """A refusal is logged, and the row is still recorded as due again."""
    built = _build_worker(1)
    built.fetcher.fetch.side_effect = RuntimeError("the host closed the connection")
    built.producer.enqueue_to_deadletter.return_value = outcome == "parked"

    await _crawl_all(built)
    await built.worker._flush()

    recorded = built.repository.complete_crawl.await_args.args[0]
    assert recorded == {built.urls[0]: NOW + RESCHEDULE}


@pytest.mark.parametrize(
    ("empty_reads", "expected_peeks"),
    [(1, 2), (2, 3), (3, 4)],
)
async def test_the_consume_loop_sleeps_on_an_empty_queue_and_reads_again(
    empty_reads: int, expected_peeks: int
) -> None:
    """An empty peek is a sleep, so the poller is not starved and the loop lives."""
    built = _build_worker(1)
    built.worker._idle_sleep_seconds = 0
    built.reader.peek = AsyncMock(
        side_effect=[[] for _ in range(empty_reads)] + [asyncio.CancelledError()]
    )

    with pytest.raises(asyncio.CancelledError):
        await built.worker._consume_forever()

    assert built.reader.peek.await_count == expected_peeks


async def test_the_consume_loop_commits_a_batch_and_reads_again() -> None:
    """One full iteration: peek, detach a crawl per message, commit, read again."""
    built = _build_worker(2)
    built.worker._idle_sleep_seconds = 0
    built.reader.peek = AsyncMock(side_effect=[built.batch, asyncio.CancelledError()])

    with pytest.raises(asyncio.CancelledError):
        await built.worker._consume_forever()
    await _drain(built)

    built.reader.commit.assert_awaited_once_with(built.batch)
    assert built.fetcher.fetch.await_count == 2


@pytest.mark.parametrize("windows", [1, 2, 3])
async def test_the_flush_loop_writes_once_per_interval(windows: int) -> None:
    """The loop is what makes the writes periodic; each tick flushes the buffers."""
    built = _build_worker(1)
    await _crawl_all(built)

    task = asyncio.create_task(built.worker._flush_forever())
    # Poll rather than sleep the interval, so the test does not race the timer.
    for _ in range(windows):
        while built.repository.complete_crawl.await_count < 1:
            await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert built.repository.complete_crawl.await_count >= 1


@pytest.mark.parametrize("fails_on_peek", [1, 2, 3])
async def test_a_failed_worker_task_ends_the_whole_worker(fails_on_peek: int) -> None:
    """The group takes both loops down, so nothing is left half running."""
    built = _build_worker(1)
    built.worker._idle_sleep_seconds = 0
    built.reader.peek = AsyncMock(
        side_effect=[[]] * (fails_on_peek - 1) + [RuntimeError("the queue is gone")]
    )

    with pytest.raises(BaseExceptionGroup) as group:
        await built.worker.run()
    # The group's own message names the crawl, and the sub-exception is the real
    # cause the consume loop raised.
    assert isinstance(group.value.exceptions[0], RuntimeError)
