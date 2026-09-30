"""Unit tests for `CrawlerWorker`, the consumer half of the crawl loop.

Every collaborator is a mock, so each test only asks which call the worker made
and with what. `run` never returns, so the happy path is driven through the one
batch method `run` itself calls.
"""

from datetime import datetime, timedelta, timezone
from typing import NamedTuple
from unittest.mock import MagicMock, call

import pytest

from webcrawler.application.worker import CrawlerWorker
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


class _Built(NamedTuple):
    """The worker under test, with the batch it is given and its mocked ports.

    Args:
    worker: The worker, built over a mock on every constructor argument.
    batch: The messages the test hands to the batch method.
    urls: The batch's URLs, in the order the batch names them.
    links: The links the mocked extractor finds on every page of the batch.
    repository: The mocked crawl state store.
    reader: The mocked queue read side.
    fetcher: The mocked page fetcher.
    extractor: The mocked link extractor.
    queuer: The mocked queuer the discovered URLs are handed to.
    politeness: The mocked policy, whose wait is 0 so every URL is fetched.
    producer: The mocked producer, which parks the messages that failed.
    """

    worker: CrawlerWorker
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


def _build_worker(count: int) -> _Built:
    """Build the worker over mocks, plus the batch of `count` messages it crawls.

    Args:
    count: How many messages the batch holds.

    Returns:
    _Built: The worker, the batch to hand it, and the mocks to assert on.
    """
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

    worker = CrawlerWorker(
        repository,
        reader,
        fetcher,
        extractor,
        politeness,
        queuer,
        producer=producer,
        batch_size=count,
        time_provider=clock,
    )
    return _Built(
        worker, batch, urls, links, repository, reader, fetcher, extractor, queuer,
        politeness, producer,
    )


@pytest.mark.parametrize("count", [1, 3])
async def test_each_message_is_marked_started_at_the_clocks_instant(
    count: int,
) -> None:
    """One store call marks every URL of the batch, at the instant the clock gave.

    Args:
    count: How many messages the batch holds.

    Returns:
    None
    """
    built = _build_worker(count)

    await built.worker._process_batch(built.batch)

    built.repository.mark_started.assert_awaited_once_with(set(built.urls), NOW)


async def test_the_body_the_fetcher_returned_is_the_body_the_extractor_reads() -> None:
    """The one message is fetched, and the body that came back is what gets parsed.

    Returns:
    None
    """
    built = _build_worker(1)
    url = built.urls[0]

    await built.worker._process_batch(built.batch)

    built.fetcher.fetch.assert_awaited_once_with(url)
    built.extractor.extract.assert_called_once_with(BODY, url)


@pytest.mark.parametrize("count", [1, 3])
async def test_the_finished_urls_and_the_discovered_links_are_recorded_together(
    count: int,
) -> None:
    """One store call records every outcome beside the links they revealed.

    Args:
    count: How many messages the batch holds.

    Returns:
    None
    """
    built = _build_worker(count)

    await built.worker._process_batch(built.batch)

    built.repository.complete_crawl.assert_awaited_once_with(
        {url: None for url in built.urls}, built.links, NOW
    )


@pytest.mark.parametrize("count", [1, 3])
async def test_the_discovered_links_are_handed_to_the_queuer_as_a_list(
    count: int,
) -> None:
    """The queuer is asked once for exactly the links the batch found, in a list.

    Args:
    count: How many messages the batch holds.

    Returns:
    None
    """
    built = _build_worker(count)

    await built.worker._process_batch(built.batch)

    built.queuer.enqueue_urls.assert_awaited_once()
    queued = built.queuer.enqueue_urls.await_args.args[0]
    # The links arrive as a set, so their order is the store's business; the
    # contents and the count are the worker's.
    assert sorted(queued, key=lambda url: url.get_url()) == sorted(
        built.links, key=lambda url: url.get_url()
    )


@pytest.mark.parametrize("count", [1, 3])
async def test_the_batch_is_committed_once_it_is_finished(count: int) -> None:
    """The reader is handed the very messages of the batch, so the queue moves on.

    Args:
    count: How many messages the batch holds.

    Returns:
    None
    """
    built = _build_worker(count)

    await built.worker._process_batch(built.batch)

    built.reader.commit.assert_awaited_once_with(built.batch)


@pytest.mark.parametrize("count", [2, 3])
async def test_one_url_whose_fetch_fails_does_not_stop_the_rest_of_the_batch(
    count: int,
) -> None:
    """The failure is one URL: it is rescheduled and parked, the others are done.

    Args:
    count: How many messages the batch holds, only the first of which fails.

    Returns:
    None
    """
    built = _build_worker(count)
    failing = built.urls[0]

    async def fetch(url: CustomURL) -> str:
        if url == failing:
            raise RuntimeError("the host closed the connection")
        return BODY

    built.fetcher.fetch.side_effect = fetch

    await built.worker._process_batch(built.batch)

    built.fetcher.fetch.assert_any_call(failing)
    for url in built.urls[1:]:
        built.extractor.extract.assert_any_call(BODY, url)
    built.repository.complete_crawl.assert_awaited_once_with(
        {failing: NOW + RESCHEDULE, **{url: None for url in built.urls[1:]}},
        built.links,
        NOW,
    )
    built.producer.enqueue_to_deadletter.assert_awaited_once_with(built.batch[0])
    built.reader.commit.assert_awaited_once_with(built.batch)


async def test_a_complete_crawl_failure_still_commits_the_batch() -> None:
    """An unrecorded outcome parks the batch and commits it, queueing no links.

    Returns:
    None
    """
    built = _build_worker(3)
    built.repository.complete_crawl.side_effect = RuntimeError("the write failed")

    await built.worker._process_batch(built.batch)

    built.queuer.enqueue_urls.assert_not_awaited()
    assert built.producer.enqueue_to_deadletter.call_args_list == [
        call(message) for message in built.batch
    ]
    built.reader.commit.assert_awaited_once_with(built.batch)


async def test_a_park_failure_does_not_stop_the_next_message_being_parked() -> None:
    """Parking is best effort, so the first failure is logged and the loop goes on.

    Returns:
    None
    """
    built = _build_worker(2)
    built.fetcher.fetch.side_effect = RuntimeError("the host closed the connection")
    built.producer.enqueue_to_deadletter.side_effect = [
        RuntimeError("the deadletter queue is gone"),
        True,
    ]

    await built.worker._process_batch(built.batch)

    assert built.producer.enqueue_to_deadletter.call_args_list == [
        call(built.batch[0]),
        call(built.batch[1]),
    ]
    built.reader.commit.assert_awaited_once_with(built.batch)


async def test_a_mark_started_failure_still_crawls_and_commits_the_batch() -> None:
    """The batch is crawled anyway, since the mark is only the store's own record.

    Returns:
    None
    """
    built = _build_worker(1)
    built.repository.mark_started.side_effect = RuntimeError("the update failed")

    await built.worker._process_batch(built.batch)

    built.repository.complete_crawl.assert_awaited_once_with(
        {built.urls[0]: None}, built.links, NOW
    )
    built.reader.commit.assert_awaited_once_with(built.batch)


async def test_a_deferred_url_is_not_fetched_and_is_due_after_its_wait() -> None:
    """A positive wait defers the URL, so its outcome is the wake up and no body.

    Returns:
    None
    """
    built = _build_worker(1)
    built.politeness.before_fetch.return_value = DEFER_MS

    await built.worker._process_batch(built.batch)

    built.fetcher.fetch.assert_not_awaited()
    built.repository.complete_crawl.assert_awaited_once_with(
        {built.urls[0]: NOW + timedelta(milliseconds=DEFER_MS)}, set(), NOW
    )
    built.queuer.enqueue_urls.assert_awaited_once_with([])
    built.producer.enqueue_to_deadletter.assert_not_awaited()
    built.reader.commit.assert_awaited_once_with(built.batch)

