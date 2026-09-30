"""Unit tests for `CrawlerWorker`, the consumer half of the crawl loop.

Every collaborator is a mock, so each test only asks which call the worker made
and with what. `run` never returns, so the happy path is driven through the one
batch method `run` itself calls.
"""

from datetime import datetime, timezone
from typing import NamedTuple
from unittest.mock import MagicMock

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
        worker, batch, urls, links, repository, reader, fetcher, extractor, queuer
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
