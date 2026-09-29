"""Tests for `CrawlerWorker`, the consumer half of the crawl loop.

The worker's `run` never returns, so a test drives it and cancels it as soon as
the queue drains; the store's clock is real, so a state and a count are asserted
and a timestamp only where the timestamp is the behaviour.
"""

import asyncio
import contextlib
import sqlite3
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from tests.support import (
    HOST,
    TIMEOUTS,
    CountingPolitenessPolicy,
    FakeFetcher,
    FakeLinkExtractor,
    make_producer,
    make_queue,
    make_reader,
    make_repository,
    make_worker,
    now,
    rows_by_state,
    seed_rows,
)
from webcrawler.application.url_poller import URLPoller
from webcrawler.application.worker import CrawlerWorker
from webcrawler.domain.crawl_state import CrawlState
from webcrawler.domain.custom_url import CustomURL
from webcrawler.infrastructure.db.sqlite_url_state_repository import (
    SQLiteURLStateRepository)
from webcrawler.infrastructure.queue.in_memory_single_topic_single_partition_queue import (
    InMemorySingleTopicSinglePartitionQueue)
from webcrawler.infrastructure.queue.in_memory_topic_producer import (
    InMemoryTopicProducer)
from webcrawler.infrastructure.time.system_time_provider import SystemTimeProvider

SEED: str = f"{HOST}/index.html"
LINK_A: str = f"{HOST}/a.html"
LINK_B: str = f"{HOST}/b.html"
BROKEN: str = f"{HOST}/broken.html"
HEALTHY: str = f"{HOST}/healthy.html"

BODY: str = "<html>a page</html>"

# The wait `CountingPolitenessPolicy` reports, long enough that no row is due.
DEFERRED_MS: int = 5000

# How far ahead a deferred row's `next_crawl_time` must sit. The stored text is
# truncated to a whole second, so the bound sits well inside the requested wait:
# the worker's clock and this test's clock can differ by a few milliseconds.
DEFERRED_BY: timedelta = timedelta(seconds=2)

# Short enough that a batch is not still settling, long enough not to spin.
POLL_SECONDS: float = 0.005

# How long a drive keeps yielding before it cancels the loop anyway.
DEADLINE_SECONDS: float = 2.0


def _parked(queue: InMemorySingleTopicSinglePartitionQueue) -> int:
    """Count the parked messages, since the deadletter queue has no reader.

    The deque's length is the deadletter behaviour itself: how many messages
    were parked. Nothing else about the queue is read here.

    Args:
    queue: The shared queue the worker parks its failed messages in.

    Returns:
    int: How many messages are parked, which is the deadletter behaviour itself.
    """
    return len(queue._deadletters)


def _next_crawl_time(database: str) -> datetime | None:
    """Read the one row's `next_crawl_time` as a timezone-aware instant.

    Args:
    database: This test's SQLite file, read with the stdlib driver.

    Returns:
    datetime | None: The stored instant, or None where the column is NULL.
    """
    with contextlib.closing(sqlite3.connect(database)) as connection:
        row = connection.execute("SELECT next_crawl_time FROM urls").fetchone()
    if row is None or row[0] is None:
        return None
    return datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S").replace(
        tzinfo=timezone.utc
    )


async def queue_rows(
    repository: SQLiteURLStateRepository,
    producer: InMemoryTopicProducer,
    urls: list[str],
) -> None:
    """Claim the given rows and feed the queue, so a batch waits for the worker.

    Args:
    repository: The initialized store holding the rows.
    producer: The write side of the queue the worker reads.
    urls: The URL texts to claim, each of which must already be a row.

    Returns:
    None
    """
    poller = URLPoller(
        repository,
        producer,
        job_timeout=TIMEOUTS.job,
        queue_timeout=TIMEOUTS.queue,
        time_provider=SystemTimeProvider(),
    )
    await poller.enqueue_urls([CustomURL(url) for url in urls])


async def drive(worker: CrawlerWorker, until: Callable[[], bool]) -> None:
    """Run the worker's own loop until the condition holds, then cancel it.

    Args:
    worker: The worker whose `run` is driven.
    until: The condition that ends the drive, checked between yields.

    Returns:
    None
    """
    task = asyncio.create_task(worker.run())
    deadline = asyncio.get_running_loop().time() + DEADLINE_SECONDS
    try:
        while not until() and asyncio.get_running_loop().time() < deadline:
            await asyncio.sleep(POLL_SECONDS)
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


async def test_a_successful_fetch_finishes_the_row_and_commits_the_batch(
    tmp_path: Path,
) -> None:
    """The outcome is durable and the message is gone, so the queue moves on.

    Args:
    tmp_path: This test's private directory, holding its own database file.

    Returns:
    None
    """
    database = str(tmp_path / "crawl.db")
    repository = make_repository(database)
    await repository.initialize()
    queue = make_queue()
    producer = make_producer(queue)
    fetcher = FakeFetcher(bodies={SEED: BODY})
    worker = make_worker(
        repository,
        queue=queue,
        reader=make_reader(queue),
        producer=producer,
        fetcher=fetcher,
    )
    try:
        await seed_rows(repository, [SEED])
        await queue_rows(repository, producer, [SEED])

        await drive(worker, until=lambda: not queue.peek(1))

        assert fetcher.requested == [SEED]
        assert rows_by_state(database) == {CrawlState.FINISHED_CRAWL.value: 1}
        assert queue.peek(1) == []
    finally:
        await repository.close()


async def test_discovered_links_are_inserted_and_crawled(tmp_path: Path) -> None:
    """A page's links become rows, are queued by the poller, and are crawled.

    Args:
    tmp_path: This test's private directory, holding its own database file.

    Returns:
    None
    """
    database = str(tmp_path / "crawl.db")
    repository = make_repository(database)
    await repository.initialize()
    queue = make_queue()
    producer = make_producer(queue)
    fetcher = FakeFetcher(bodies={SEED: BODY, LINK_A: BODY, LINK_B: BODY})
    extractor = FakeLinkExtractor(
        links_by_url={SEED: [CustomURL(LINK_A), CustomURL(LINK_B)]}
    )
    worker = make_worker(
        repository,
        queue=queue,
        reader=make_reader(queue),
        producer=producer,
        fetcher=fetcher,
        link_extractor=extractor,
    )
    try:
        await seed_rows(repository, [SEED])
        assert rows_by_state(database) == {CrawlState.NOT_CRAWLED.value: 1}
        await queue_rows(repository, producer, [SEED])

        await drive(worker, until=lambda: not queue.peek(1))

        assert sorted(fetcher.requested) == sorted([SEED, LINK_A, LINK_B])
        assert rows_by_state(database) == {CrawlState.FINISHED_CRAWL.value: 3}
    finally:
        await repository.close()


async def test_a_failed_fetch_parks_the_message_and_keeps_the_row(
    tmp_path: Path,
) -> None:
    """A URL that cannot be fetched is parked, and its row is still tracked.

    Args:
    tmp_path: This test's private directory, holding its own database file.

    Returns:
    None
    """
    database = str(tmp_path / "crawl.db")
    repository = make_repository(database)
    await repository.initialize()
    queue = make_queue()
    producer = make_producer(queue)
    fetcher = FakeFetcher(failures={SEED: RuntimeError("the host refused")})
    worker = make_worker(
        repository,
        queue=queue,
        reader=make_reader(queue),
        producer=producer,
        fetcher=fetcher,
    )
    try:
        await seed_rows(repository, [SEED])
        await queue_rows(repository, producer, [SEED])

        await drive(worker, until=lambda: not queue.peek(1))

        assert _parked(queue) == 1
        assert rows_by_state(database) == {CrawlState.FINISHED_CRAWL.value: 1}
        assert queue.peek(1) == []
    finally:
        await repository.close()


async def test_an_unexpected_failure_is_parked_and_the_loop_survives(
    tmp_path: Path,
) -> None:
    """A body the parser cannot read is one failed URL, not a dead worker.

    Args:
    tmp_path: This test's private directory, holding its own database file.

    Returns:
    None
    """
    database = str(tmp_path / "crawl.db")
    repository = make_repository(database)
    await repository.initialize()
    queue = make_queue()
    producer = make_producer(queue)
    fetcher = FakeFetcher(bodies={BROKEN: BODY, HEALTHY: BODY})
    extractor = FakeLinkExtractor(raises_on={BROKEN})
    worker = make_worker(
        repository,
        queue=queue,
        reader=make_reader(queue),
        producer=producer,
        fetcher=fetcher,
        link_extractor=extractor,
    )
    try:
        await seed_rows(repository, [BROKEN, HEALTHY])
        await queue_rows(repository, producer, [BROKEN])
        # First phase: the unreadable page is the only message, so one is parked.
        await drive(worker, until=lambda: not queue.peek(1))
        assert _parked(queue) == 1

        # Second phase: a good page is crawled by the same worker, which is the
        # point -- the loop outlived the failure.
        await queue_rows(repository, producer, [HEALTHY])
        await drive(worker, until=lambda: not queue.peek(1))

        assert sorted(fetcher.requested) == sorted([BROKEN, HEALTHY])
        assert rows_by_state(database) == {CrawlState.FINISHED_CRAWL.value: 2}
    finally:
        await repository.close()


@pytest.mark.parametrize(
    ("reschedule_delay", "crawlable"),
    [(timedelta(milliseconds=1), True), (TIMEOUTS.job, False)],
    ids=["delay_elapsed", "delay_outstanding"],
)
async def test_a_failed_row_becomes_crawlable_once_its_delay_passes(
    reschedule_delay: timedelta, crawlable: bool, tmp_path: Path
) -> None:
    """`reschedule_delay` is the whole of the failed row's new schedule.

    Args:
    reschedule_delay: The delay the worker is configured with.
    crawlable: Whether the failed row must be claimable again afterwards.
    tmp_path: This test's private directory, holding its own database file.

    Returns:
    None
    """
    database = str(tmp_path / "crawl.db")
    repository = make_repository(database)
    await repository.initialize()
    queue = make_queue()
    producer = make_producer(queue)
    worker = make_worker(
        repository,
        queue=queue,
        reader=make_reader(queue),
        producer=producer,
        fetcher=FakeFetcher(failures={SEED: RuntimeError("the host refused")}),
        reschedule_delay=reschedule_delay,
    )
    try:
        await seed_rows(repository, [SEED])
        await queue_rows(repository, producer, [SEED])
        await drive(worker, until=lambda: not queue.peek(1))
        await asyncio.sleep(0.01)

        due = await repository.get_crawlable_urls(
            now(), -1, job_timeout=TIMEOUTS.job, queue_timeout=TIMEOUTS.queue
        )

        assert [url.get_url() for url in due] == ([SEED] if crawlable else [])
        assert rows_by_state(database) == {CrawlState.FINISHED_CRAWL.value: 1}
    finally:
        await repository.close()


async def test_a_politeness_wait_defers_the_url_instead_of_blocking_the_batch(
    tmp_path: Path,
) -> None:
    """A wait is never slept: the row is scheduled later and the message ends.

    Args:
    tmp_path: This test's private directory, holding its own database file.

    Returns:
    None
    """
    database = str(tmp_path / "crawl.db")
    repository = make_repository(database)
    await repository.initialize()
    queue = make_queue()
    producer = make_producer(queue)
    policy = CountingPolitenessPolicy(wait_ms=DEFERRED_MS)
    fetcher = FakeFetcher(bodies={SEED: BODY})
    worker = make_worker(
        repository,
        queue=queue,
        reader=make_reader(queue),
        producer=producer,
        fetcher=fetcher,
        politeness_policy=policy,
    )
    try:
        await seed_rows(repository, [SEED])
        await queue_rows(repository, producer, [SEED])

        await drive(worker, until=lambda: not queue.peek(1))

        deferred = _next_crawl_time(database)
        assert fetcher.requested == []
        # No fetch was made, so the policy was given no outcome to learn from.
        assert policy.recorded == []
        assert rows_by_state(database) == {CrawlState.FINISHED_CRAWL.value: 1}
        assert deferred is not None
        assert deferred - now() >= DEFERRED_BY
    finally:
        await repository.close()


@pytest.mark.parametrize("fails", [False, True], ids=["success", "fetch_failure"])
async def test_one_fetch_gives_the_politeness_policy_exactly_one_outcome(
    fails: bool, tmp_path: Path
) -> None:
    """Every attempt, failed or not, is handed back once, as it ended.

    Args:
    fails: Whether the fetch is configured to fail.
    tmp_path: This test's private directory, holding its own database file.

    Returns:
    None
    """
    database = str(tmp_path / "crawl.db")
    repository = make_repository(database)
    await repository.initialize()
    queue = make_queue()
    producer = make_producer(queue)
    policy = CountingPolitenessPolicy()
    fetcher = (
        FakeFetcher(failures={SEED: RuntimeError("the host refused")})
        if fails
        else FakeFetcher(bodies={SEED: BODY})
    )
    worker = make_worker(
        repository,
        queue=queue,
        reader=make_reader(queue),
        producer=producer,
        fetcher=fetcher,
        politeness_policy=policy,
    )
    try:
        await seed_rows(repository, [SEED])
        await queue_rows(repository, producer, [SEED])

        await drive(worker, until=lambda: not queue.peek(1))

        assert len(policy.recorded) == 1
        assert policy.recorded[0][1].get_url() == SEED
        assert policy.recorded[0][2].is_success is not fails
    finally:
        await repository.close()


@pytest.mark.parametrize("closes", [1, 2])
async def test_close_releases_the_fetcher(closes: int, tmp_path: Path) -> None:
    """The worker owns the fetcher's session, so it is released with the worker.

    Args:
    closes: How many times the worker's `close` is called.
    tmp_path: This test's private directory, holding its own database file.

    Returns:
    None
    """
    repository = make_repository(str(tmp_path / "crawl.db"))
    await repository.initialize()
    fetcher = FakeFetcher()
    worker = make_worker(repository, fetcher=fetcher)
    try:
        for _ in range(closes):
            await worker.close()
    finally:
        await repository.close()

    assert fetcher.closed == closes
