"""Shared fakes and builders for the test suite.

Every collaborator is a real object wherever the real object is cheap, so a test
exercises the production path; only the network and the clock are faked.
"""

import contextlib
import sqlite3
from collections.abc import Iterable, Mapping, Sequence
from datetime import datetime, timedelta, timezone
from typing import NamedTuple

from webcrawler.application.url_poller import URLPoller
from webcrawler.application.worker import CrawlerWorker
from webcrawler.domain.base_result import BaseResult
from webcrawler.domain.custom_url import CustomURL
from webcrawler.domain.messages import BaseMessage
from webcrawler.domain.retry_settings import RetrySettings
from webcrawler.infrastructure.db.sqlite_url_state_repository import (
    SQLiteURLStateRepository)
from webcrawler.infrastructure.queue.in_memory_single_topic_single_partition_queue import (
    InMemorySingleTopicSinglePartitionQueue)
from webcrawler.infrastructure.queue.in_memory_topic_producer import (
    InMemoryTopicProducer)
from webcrawler.infrastructure.queue.in_memory_topic_reader import (
    InMemoryTopicReader)
from webcrawler.infrastructure.retry.exponential_backoff_retry_policy import (
    ExponentialBackoffRetryPolicy)
from webcrawler.infrastructure.time.system_time_provider import (
    SystemTimeProvider)
from webcrawler.ports.link_extractor import LinkExtractor
from webcrawler.ports.politeness_policy import PolitenessPolicy
from webcrawler.ports.web_page_fetcher import WebPageFetcher

# The one instant the suite pins, timezone-aware UTC.
NOW: datetime = datetime(2026, 9, 29, 6, 0, tzinfo=timezone.utc)

TOPIC: str = "crawl"
GROUP: str = "crawler"

# The one host every crawl test crawls; the extractor keeps its own nested page.
HOST: str = "https://crawlme.monzo.com"


class _Timeouts(NamedTuple):
    """The two staleness timeouts the store's predicates are given."""

    job: timedelta
    queue: timedelta


# Exported so the numbers the timeout branches are tested against live here once.
TIMEOUTS: _Timeouts = _Timeouts(
    job=timedelta(minutes=1), queue=timedelta(seconds=30)
)

# Retries are exercised for their loop, not for the wall clock, so the backoff
# is a millisecond and the budget is two attempts.
FAST_RETRY: RetrySettings = RetrySettings(
    max_attempts=2,
    base_delay_seconds=0.001,
    max_delay_seconds=0.001,
    jitter_seconds=0.0,
    timeout_seconds=5.0,
)


def make_repository(path: str) -> SQLiteURLStateRepository:
    """Build the real store over a file, without creating its schema.

    Args:
    path: The SQLite file, or `":memory:"`. Its parent directory must exist.

    Returns:
    SQLiteURLStateRepository: The store, with a real retry policy and the real
    system clock. The caller awaits `initialize` when the schema is wanted.
    """
    return SQLiteURLStateRepository(
        path,
        ExponentialBackoffRetryPolicy(FAST_RETRY),
        SystemTimeProvider(),
    )


def make_queue(
    max_size: int = 64, max_deadletter_size: int = 8
) -> InMemorySingleTopicSinglePartitionQueue:
    """Build the real bounded queue with small capacities a test can fill.

    Args:
    max_size: The crawl queue's capacity, in messages.
    max_deadletter_size: The deadletter queue's capacity, in messages.

    Returns:
    InMemorySingleTopicSinglePartitionQueue: One queue, to be shared by the
    producer and the reader a test builds over it.
    """
    return InMemorySingleTopicSinglePartitionQueue(
        max_size=max_size, max_deadletter_size=max_deadletter_size
    )


def make_producer(
    queue: InMemorySingleTopicSinglePartitionQueue,
) -> InMemoryTopicProducer:
    """Build the real write side over the given shared queue.

    Args:
    queue: The shared queue, which must be the reader's queue.

    Returns:
    InMemoryTopicProducer: A producer bound to `TOPIC`.
    """
    return InMemoryTopicProducer(TOPIC, queue)


def make_reader(
    queue: InMemorySingleTopicSinglePartitionQueue,
) -> InMemoryTopicReader:
    """Build the real read side over the given shared queue.

    Args:
    queue: The shared queue, which must be the producer's queue.

    Returns:
    InMemoryTopicReader: A reader bound to `TOPIC` and `GROUP`.
    """
    return InMemoryTopicReader(TOPIC, GROUP, queue)


def make_poller(
    repository: SQLiteURLStateRepository,
    producer: InMemoryTopicProducer,
    *,
    queue_timeout: timedelta = TIMEOUTS.queue,
    periodic_fetch_seconds: float = 1.0,
) -> URLPoller:
    """Build the real poller over the real store and the real producer.

    Args:
    repository: The initialized store the poller claims from.
    producer: The write side of the queue the poller feeds.
    queue_timeout: The `queued` staleness window its claims are given.
    periodic_fetch_seconds: The interval between two polls of `run`.

    Returns:
    URLPoller: The poller, holding the production job timeout and clock.
    """
    return URLPoller(
        repository,
        producer,
        periodic_fetch_seconds=periodic_fetch_seconds,
        job_timeout=TIMEOUTS.job,
        queue_timeout=queue_timeout,
        time_provider=SystemTimeProvider(),
    )


class FakeFetcher(WebPageFetcher):
    """Serves configured bodies, records every URL, and raises on a blank entry.

    A URL in neither mapping raises `KeyError`, so a fetch the test never set up
    is loud rather than silently empty.

    Args:
    bodies: Body per URL text, keyed by the canonical URL text.
    failures: The exception `fetch` raises per URL text, keyed the same way.
    """

    def __init__(
        self,
        bodies: Mapping[str, str] | None = None,
        failures: Mapping[str, BaseException] | None = None,
    ) -> None:
        """Copy both mappings and start with nothing requested or closed.

        Args:
        bodies: Body per URL text, or None for a fetcher that serves nothing.
        failures: Exception per URL text, or None for a fetcher that never fails.
        """
        self._bodies = dict(bodies or {})
        self._failures = dict(failures or {})
        self.requested: list[str] = []
        self.closed: int = 0

    async def fetch(self, url: CustomURL) -> str:
        """Return the body configured for one URL.

        Args:
        url: The URL to serve, looked up by its canonical text.

        Returns:
        str: The configured body.

        Raises:
        KeyError: If the URL is in neither mapping, so an unexpected fetch
        fails the test instead of returning an empty page.
        Exception: The exception configured for the URL, when it has one.
        """
        key = url.get_url()
        self.requested.append(key)
        if key in self._failures:
            raise self._failures[key]
        return self._bodies[key]

    async def close(self) -> None:
        """Count the call and release nothing, since no session was opened.

        Returns:
        None
        """
        self.closed += 1


class FakeLinkExtractor(LinkExtractor):
    """Returns configured links per URL, and a parse failure per flagged URL.

    Args:
    links_by_url: Links per URL text, keyed by the canonical URL text.
    raises_on: URL texts whose extraction must raise, to simulate a body the
    parser cannot read.
    """

    def __init__(
        self,
        links_by_url: Mapping[str, list[CustomURL]] | None = None,
        raises_on: set[str] | None = None,
    ) -> None:
        """Copy both arguments into this extractor.

        Args:
        links_by_url: Links per URL text, or None for an extractor that finds
        nothing anywhere.
        raises_on: URL texts to fail on, or None to fail on none.
        """
        self._links_by_url = {key: list(links) for key, links in (links_by_url or {}).items()}
        self._raises_on = set(raises_on or ())

    def extract(self, html: str, base_url: CustomURL) -> set[CustomURL]:
        """Return the links configured for the URL the body came from.

        Args:
        html: The page body, ignored: the answer is keyed by the URL alone.
        base_url: The URL the body came from, looked up by its canonical text.

        Returns:
        set[CustomURL]: The configured links, unique and unordered, or an empty
        set when the URL is not configured.

        Raises:
        ValueError: If the URL is in `raises_on`, simulating a body the parser
        cannot read.
        """
        key = base_url.get_url()
        if key in self._raises_on:
            raise ValueError(f"link extraction for {key} is configured to fail")
        return set(self._links_by_url.get(key, []))


class CountingPolitenessPolicy(PolitenessPolicy):
    """Reports a fixed wait and records every outcome it was handed.

    Args:
    wait_ms: The milliseconds `before_fetch` always reports; `0` means fetch now.
    """

    def __init__(self, wait_ms: int = 0) -> None:
        """Record the wait to report and start with no outcomes recorded.

        Args:
        wait_ms: The milliseconds `before_fetch` always reports.
        """
        self._wait_ms = wait_ms
        self.recorded: list[tuple[datetime, CustomURL, BaseResult]] = []

    async def before_fetch(self, url: CustomURL) -> int:
        """Report the configured wait, which is never actually slept.

        Args:
        url: The URL about to be fetched, ignored.

        Returns:
        int: The configured wait in milliseconds.
        """
        return self._wait_ms

    async def record_fetch(
        self, now: datetime, url: CustomURL, result: BaseResult
    ) -> None:
        """Append one completed attempt, so a test can assert on it.

        Args:
        now: When the attempt finished.
        url: The URL that was fetched.
        result: How that attempt ended.

        Returns:
        None
        """
        self.recorded.append((now, url, result))


def make_worker(
    repository: SQLiteURLStateRepository,
    *,
    queue: InMemorySingleTopicSinglePartitionQueue | None = None,
    reader: InMemoryTopicReader | None = None,
    producer: InMemoryTopicProducer | None = None,
    fetcher: FakeFetcher | None = None,
    link_extractor: FakeLinkExtractor | None = None,
    politeness_policy: CountingPolitenessPolicy | None = None,
    batch_size: int = 4,
    reschedule_delay: timedelta = timedelta(seconds=30),
) -> CrawlerWorker:
    """Build the real worker over the real store and queue, with the fakes given.

    Every override defaults to a fresh real or fake object built here, so a test
    varies one collaborator and leaves the rest on the production path.

    Args:
    repository: The real store, already initialized by the caller.
    queue: The shared queue, or None to build one with `make_queue`.
    reader: The read side, or None to build one over the resolved queue.
    producer: The write side, or None to build one over the resolved queue.
    fetcher: The fetch fake, or None for a `FakeFetcher` serving nothing.
    link_extractor: The extract fake, or None for a `FakeLinkExtractor` that
    finds nothing.
    politeness_policy: The politeness fake, or None for a
    `CountingPolitenessPolicy` that never waits.
    batch_size: The most messages one peek returns.
    reschedule_delay: The delay recorded after a URL that did not crawl.

    Returns:
    CrawlerWorker: The worker, holding a real `URLPoller` as its queuer so the
    discovered URLs are claimed and fed through the store like in production.
    """
    resolved_queue = queue if queue is not None else make_queue()
    resolved_reader = (
        reader if reader is not None else make_reader(resolved_queue)
    )
    resolved_producer = (
        producer if producer is not None else make_producer(resolved_queue)
    )
    queuer = URLPoller(
        repository,
        resolved_producer,
        job_timeout=TIMEOUTS.job,
        queue_timeout=TIMEOUTS.queue,
        time_provider=SystemTimeProvider(),
    )
    return CrawlerWorker(
        repository,
        resolved_reader,
        fetcher if fetcher is not None else FakeFetcher(),
        link_extractor if link_extractor is not None else FakeLinkExtractor(),
        (
            politeness_policy
            if politeness_policy is not None
            else CountingPolitenessPolicy()
        ),
        queuer,
        producer=resolved_producer,
        batch_size=batch_size,
        reschedule_delay=reschedule_delay,
        time_provider=SystemTimeProvider(),
    )


async def seed_rows(
    repository: SQLiteURLStateRepository,
    urls: Iterable[str],
) -> None:
    """Insert one `not_crawled` row per URL through the real `create_urls`.

    Args:
    repository: The real store, already initialized.
    urls: The URL texts to make known, each of which must be a valid crawlable URL.

    Returns:
    None

    Raises:
    InvalidURLError: If a URL text is not an absolute http(s) URL.
    sqlite3.Error: If the rows cannot be committed.
    """
    await repository.create_urls({CustomURL(url) for url in urls})


def now() -> datetime:
    """Return the real current instant, which is the only clock in the store.

    Returns:
    datetime: The current timezone-aware UTC instant.
    """
    return datetime.now(timezone.utc)


def page_urls(count: int) -> list[str]:
    """Return the URL texts of pages 1 through `count`, in order.

    Args:
    count: How many pages to name.

    Returns:
    list[str]: The URL texts, in generation order.
    """
    return [f"{HOST}/page-{index}.html" for index in range(1, count + 1)]


def texts(messages: Sequence[BaseMessage]) -> list[str]:
    """Return one canonical URL text per message, in the order given.

    Args:
    messages: The messages a peek or a commit was handed.

    Returns:
    list[str]: The URL texts, so a test asserts both order and content.
    """
    return [message.url.get_url() for message in messages]


def queued_texts(queue: InMemorySingleTopicSinglePartitionQueue, limit: int = 50) -> list[str]:
    """Return one canonical URL text per message waiting on the queue.

    Args:
    queue: The shared queue the poller fed.
    limit: How many messages the non-destructive peek asks for.

    Returns:
    list[str]: The queued URL texts, in queue order.
    """
    return texts(queue.peek(limit))


def rows_by_state(db_path: str) -> dict[str, int]:
    """Count the stored rows per state with the stdlib driver, not the store.

    Args:
    db_path: The SQLite file, opened read-only by path.

    Returns:
    dict[str, int]: One count per state present, keyed by the stored state text.

    Raises:
    sqlite3.Error: If the file cannot be opened or is not a SQLite database.
    """
    with contextlib.closing(sqlite3.connect(db_path)) as connection:
        rows = connection.execute(
            "SELECT state, COUNT(*) FROM urls GROUP BY state"
        ).fetchall()
    return {state: count for state, count in rows}
