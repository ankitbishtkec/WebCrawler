"""Tests for `URLPoller`, the producer half of the crawl loop.

Every poller here is the real one over the real store and the real queue, so
only the staleness windows are shortened, to milliseconds rather than to seconds.
"""

import asyncio
import contextlib
import inspect
from collections.abc import Callable
from datetime import timedelta
from pathlib import Path

import pytest

from tests.support import (
    HOST,
    TIMEOUTS,
    make_poller,
    make_producer,
    make_queue,
    make_repository,
    now,
    page_urls,
    queued_texts,
    rows_by_state,
    seed_rows,
)
from webcrawler.application.url_poller import URLPoller
from webcrawler.domain.crawl_state import CrawlState
from webcrawler.domain.custom_url import CustomURL

# Under a second encodes to zero whole seconds, which is how the predicate reads
# it, so a `queued` row is already past its own window.
EXPIRED: timedelta = timedelta(milliseconds=1)

# Short enough that a cancelled poll has already polled, long enough not to spin.
POLL_SECONDS: float = 0.005

# How long a drive keeps yielding before it cancels the loop anyway.
DEADLINE_SECONDS: float = 2.0

# The dedupe key the public APIs require; nothing reads it in memory.
REQUEST_ID: str = "test-request"


def _states(states: dict[str, int]) -> dict[str, int]:
    """Drop the states a count of zero means are absent from the table.

    Args:
    states: The count each state must hold, zero meaning the state is absent.

    Returns:
    dict[str, int]: The non-zero counts, which is the shape `rows_by_state` reports.
    """
    return {state: count for state, count in states.items() if count}


async def _poll_until(poller: URLPoller, done: Callable[[], bool]) -> None:
    """Run the poller's own loop until the condition holds, then cancel it.

    Args:
    poller: The poller whose `run` is driven.
    done: The condition that ends the drive, checked between yields.

    Returns:
    None
    """
    task = asyncio.create_task(poller.run())
    deadline = asyncio.get_running_loop().time() + DEADLINE_SECONDS
    try:
        while not done() and asyncio.get_running_loop().time() < deadline:
            await asyncio.sleep(POLL_SECONDS)
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


@pytest.mark.parametrize("count", [1, 3])
async def test_enqueue_urls_puts_the_callers_rows_on_the_queue(
    count: int, tmp_path: Path
) -> None:
    """A caller's own rows are claimed and fed, one message each.

    Args:
    count: How many rows the caller names.
    tmp_path: This test's private directory, holding its own database file.

    Returns:
    None
    """
    database = str(tmp_path / "crawl.db")
    urls = page_urls(count)
    repository = make_repository(database)
    await repository.initialize()
    queue = make_queue()
    try:
        await seed_rows(repository, urls)
        poller = make_poller(repository, make_producer(queue))

        await poller.enqueue_urls([CustomURL(url) for url in urls])

        # `claim_urls` takes a set and returns a set, so the queue order is the
        # claim's order and not the caller's; the fact under test is the same
        # rows, once each.
        assert set(queued_texts(queue)) == set(urls)
        assert rows_by_state(database) == {CrawlState.QUEUED.value: count}
    finally:
        await repository.close()


@pytest.mark.parametrize("count", [1, 2])
async def test_a_url_that_is_not_a_row_is_never_queued(
    count: int, tmp_path: Path
) -> None:
    """The claim only ever targets rows the store holds, so it queues nothing.

    Args:
    count: How many unknown URLs the caller names.
    tmp_path: This test's private directory, holding its own database file.

    Returns:
    None
    """
    database = str(tmp_path / "crawl.db")
    unknown = [f"{HOST}/never-seen-{index}.html" for index in range(1, count + 1)]
    repository = make_repository(database)
    await repository.initialize()
    queue = make_queue()
    try:
        await seed_rows(repository, page_urls(1))
        poller = make_poller(repository, make_producer(queue))

        await poller.enqueue_urls([CustomURL(url) for url in unknown])

        assert queued_texts(queue) == []
        assert rows_by_state(database) == {CrawlState.NOT_CRAWLED.value: 1}
    finally:
        await repository.close()


@pytest.mark.parametrize("seeded", [0, 2])
async def test_an_empty_list_queues_nothing(seeded: int, tmp_path: Path) -> None:
    """The no-links case is a no-op, so the rows it holds stay claimable.

    Args:
    seeded: How many rows the store holds before the call.
    tmp_path: This test's private directory, holding its own database file.

    Returns:
    None
    """
    database = str(tmp_path / "crawl.db")
    repository = make_repository(database)
    await repository.initialize()
    queue = make_queue()
    try:
        await seed_rows(repository, page_urls(seeded))
        poller = make_poller(repository, make_producer(queue))

        await poller.enqueue_urls([])

        assert queued_texts(queue) == []
        assert rows_by_state(database) == _states(
            {CrawlState.NOT_CRAWLED.value: seeded}
        )
    finally:
        await repository.close()


@pytest.mark.parametrize("max_items", [1, 2, 5])
async def test_queue_candidates_claims_no_more_than_it_is_asked_for(
    max_items: int, tmp_path: Path
) -> None:
    """The caller's limit reaches the store, so no surplus row is fed.

    Args:
    max_items: The row limit this call is given, over five due rows.
    tmp_path: This test's private directory, holding its own database file.

    Returns:
    None
    """
    database = str(tmp_path / "crawl.db")
    seeded = 5
    repository = make_repository(database)
    await repository.initialize()
    queue = make_queue()
    try:
        await seed_rows(repository, page_urls(seeded))
        poller = make_poller(repository, make_producer(queue))

        await poller.queue_candidates(now(), REQUEST_ID, max_items)

        assert len(queued_texts(queue)) == max_items
        assert rows_by_state(database) == _states(
            {
                CrawlState.QUEUED.value: max_items,
                CrawlState.NOT_CRAWLED.value: seeded - max_items,
            }
        )
    finally:
        await repository.close()


@pytest.mark.parametrize(
    ("queue_timeout", "pause_seconds", "claimed"),
    [(TIMEOUTS.queue, 0.0, 1), (EXPIRED, 0.01, 2)],
    ids=["inside_the_window", "past_the_window"],
)
async def test_a_queued_row_is_reclaimed_only_past_its_queue_timeout(
    queue_timeout: timedelta, pause_seconds: float, claimed: int, tmp_path: Path
) -> None:
    """A second claim takes the row only once its own window has elapsed.

    Args:
    queue_timeout: The `queued` staleness window both claims are given.
    pause_seconds: How long to wait between the two claims.
    claimed: How many messages the queue must hold afterwards, the first claim
        plus any re-claim.
    tmp_path: This test's private directory, holding its own database file.

    Returns:
    None
    """
    database = str(tmp_path / "crawl.db")
    repository = make_repository(database)
    await repository.initialize()
    queue = make_queue()
    try:
        await seed_rows(repository, page_urls(1))
        poller = make_poller(
            repository, make_producer(queue), queue_timeout=queue_timeout
        )
        await poller.enqueue_urls([CustomURL(page_urls(1)[0])])
        assert len(queued_texts(queue)) == 1

        await asyncio.sleep(pause_seconds)
        await poller.queue_candidates(now(), REQUEST_ID)

        assert len(queued_texts(queue)) == claimed
    finally:
        await repository.close()


async def test_run_polls_due_rows_onto_the_queue(tmp_path: Path) -> None:
    """The poll loop feeds the queue without the caller driving a schedule.

    Args:
    tmp_path: This test's private directory, holding its own database file.

    Returns:
    None
    """
    database = str(tmp_path / "crawl.db")
    urls = page_urls(2)
    repository = make_repository(database)
    await repository.initialize()
    queue = make_queue()
    try:
        await seed_rows(repository, urls)
        poller = make_poller(
            repository,
            make_producer(queue),
            periodic_fetch_seconds=POLL_SECONDS,
        )

        await _poll_until(poller, lambda: len(queued_texts(queue)) == 2)

        assert sorted(queued_texts(queue)) == sorted(urls)
        assert rows_by_state(database) == {CrawlState.QUEUED.value: 2}
    finally:
        await repository.close()


@pytest.mark.parametrize("name", ["retry_policy", "logger"])
async def test_the_poller_takes_neither_a_retry_policy_nor_a_logger(
    name: str,
) -> None:
    """The store owns its retries and every class logs for itself.

    Args:
    name: The constructor parameter that must be absent.

    Returns:
    None
    """
    assert name not in inspect.signature(URLPoller.__init__).parameters
