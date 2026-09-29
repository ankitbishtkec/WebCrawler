"""Tests for `SQLiteURLStateRepository`, the only crawl state store shipped.

Every store here is the real one over a real SQLite file in a per-test directory
and over the real system clock, so no stored timestamp is ever asserted: only
states and counts are. The two staleness windows are exercised by handing the
predicate a real long window, which withholds a row written milliseconds ago,
and a sub-second one, which truncates to zero and releases the row at once, so
the whole file runs in well under a second.
"""

import asyncio
import contextlib
import sqlite3
from collections.abc import Collection
from datetime import timedelta
from pathlib import Path

import pytest

from tests.support import (
    TIMEOUTS,
    make_repository,
    now,
    page_urls,
    rows_by_state,
    seed_rows,
)
from webcrawler.domain.crawl_state import CrawlState
from webcrawler.domain.custom_url import CustomURL
from webcrawler.infrastructure.db.models import MAX_BOUND_PARAMETERS
from webcrawler.infrastructure.db.sqlite_url_state_repository import (
    SQLiteURLStateRepository)

# Under a second encodes to zero whole seconds, which is how the predicate
# compares it, so a row is already past its own window.
EXPIRED: timedelta = timedelta(milliseconds=1)

# Enough for a write and a read to fall in different milliseconds, and short
# enough to keep the released cases fast.
PAUSE_SECONDS: float = 0.01

# Due times either side of now, far enough out that no rounding can reach them.
DUE_PAST: timedelta = timedelta(minutes=-5)
DUE_FUTURE: timedelta = timedelta(minutes=5)

# Wider than the parameter budget of a single mark-started statement, so the
# batch has to be split before every row is marked.
CHUNKED_URLS: int = MAX_BOUND_PARAMETERS + 200

# The methods that take a collection and must short-circuit an empty one.
COLLECTION_METHODS: tuple[str, ...] = ("create_urls", "mark_started", "claim_urls")

# The methods that hand a collection back to the caller.
READER_METHODS: tuple[str, ...] = (
    "get_crawlable_urls",
    "claim_candidates",
    "claim_urls",
)


def _states(states: dict[str, int]) -> dict[str, int]:
    """Drop the states a count of zero means are absent from the table.

    Args:
    states: The count each state must hold, zero meaning the state is absent.

    Returns:
    dict[str, int]: The non-zero counts, which is the shape `rows_by_state`
    reports.
    """
    return {state: count for state, count in states.items() if count}


def _urls(values: Collection[CustomURL]) -> set[str]:
    """Return the canonical text of each URL in a collection.

    Args:
    values: A collection of `CustomURL`, as every collection-returning method
        hands back.

    Returns:
    set[str]: The canonical URL texts, so an assertion names rows rather than
    comparing values by identity.
    """
    return {url.get_url() for url in values}


def _stored_row(database: str, url: str) -> dict[str, object]:
    """Read one stored row with the stdlib driver, not the store.

    The persisted column is the store's own output, so a column this test must
    observe is read straight from the file, the way `rows_by_state` does.

    Args:
    database: The SQLite file, opened read-only by path.
    url: The canonical URL text of the row to read.

    Returns:
    dict[str, object]: The row's columns, keyed by column name.

    Raises:
    TypeError: If the row is absent, so a missing row fails the test instead of
    reading as a row of nulls.
    """
    with contextlib.closing(sqlite3.connect(database)) as connection:
        connection.row_factory = sqlite3.Row
        row = connection.execute(
            "SELECT * FROM urls WHERE custom_url = ?", (url,)
        ).fetchone()
    return dict(row)


async def _empty_input(
    repository: SQLiteURLStateRepository, method_name: str
) -> object:
    """Call one method with an empty collection and return whatever it answers.

    Args:
    repository: The real store, which may or may not have a schema yet.
    method_name: The method to call.

    Returns:
    object: The method's return value, which is an empty set for `claim_urls`
    and None for the other two.
    """
    if method_name == "claim_urls":
        return await repository.claim_urls(
            set(),
            now(),
            -1,
            job_timeout=TIMEOUTS.job,
            queue_timeout=TIMEOUTS.queue,
        )
    if method_name == "mark_started":
        return await repository.mark_started(set(), now())
    return await repository.create_urls(set())


async def _read_or_claim(
    repository: SQLiteURLStateRepository, method_name: str
) -> set[CustomURL]:
    """Call one collection-returning method over a single due row.

    Args:
    repository: The real store, already holding one due row.
    method_name: The method to call.

    Returns:
    set[CustomURL]: Whatever the method hands back, the type under test.
    """
    if method_name == "get_crawlable_urls":
        return await repository.get_crawlable_urls(
            now(), -1, job_timeout=TIMEOUTS.job, queue_timeout=TIMEOUTS.queue
        )
    if method_name == "claim_candidates":
        return await repository.claim_candidates(
            now(), -1, job_timeout=TIMEOUTS.job, queue_timeout=TIMEOUTS.queue
        )
    return await repository.claim_urls(
        {CustomURL(page_urls(1)[0])},
        now(),
        -1,
        job_timeout=TIMEOUTS.job,
        queue_timeout=TIMEOUTS.queue,
    )


@pytest.mark.parametrize(
    ("max_items", "expected"), [(1, 1), (3, 3)], ids=["one_of_three", "all_three"]
)
async def test_a_fresh_row_is_crawlable_at_once(
    max_items: int, expected: int, tmp_path: Path
) -> None:
    """An insert stamps `created_time` and `next_crawl_time` alike, so the row is due.

    The read honours the caller's limit and reserves nothing.

    Args:
    max_items: The row limit this read is given, over three due rows.
    expected: How many of those three rows the read must return.
    tmp_path: This test's private directory, holding its own database file.

    Returns:
    None
    """
    database = str(tmp_path / "crawl.db")
    urls = page_urls(3)
    repository = make_repository(database)
    await repository.initialize()
    try:
        await seed_rows(repository, urls)

        found = await repository.get_crawlable_urls(
            now(), max_items, job_timeout=TIMEOUTS.job, queue_timeout=TIMEOUTS.queue
        )

        assert len(found) == expected
        assert _urls(found) <= set(urls)
        assert rows_by_state(database) == {CrawlState.NOT_CRAWLED.value: 3}
    finally:
        await repository.close()


@pytest.mark.parametrize("count", [1, 3])
async def test_create_urls_is_idempotent(count: int, tmp_path: Path) -> None:
    """The insert conflicts on the primary key and is ignored, so one URL is one row.

    Args:
    count: How many URLs are seeded before the same URLs are seeded again.
    tmp_path: This test's private directory, holding its own database file.

    Returns:
    None
    """
    database = str(tmp_path / "crawl.db")
    urls = page_urls(count)
    repository = make_repository(database)
    await repository.initialize()
    try:
        await seed_rows(repository, urls)
        await seed_rows(repository, urls)

        assert rows_by_state(database) == {CrawlState.NOT_CRAWLED.value: count}
    finally:
        await repository.close()


@pytest.mark.parametrize("method_name", COLLECTION_METHODS)
async def test_an_empty_input_leaves_every_row_untouched(
    method_name: str, tmp_path: Path
) -> None:
    """The no-links case is the common one, so an empty collection changes nothing.

    Args:
    method_name: The method that must answer an empty collection.
    tmp_path: This test's private directory, holding its own database file.

    Returns:
    None
    """
    database = str(tmp_path / "crawl.db")
    repository = make_repository(database)
    await repository.initialize()
    try:
        await seed_rows(repository, page_urls(2))

        result = await _empty_input(repository, method_name)

        if method_name == "claim_urls":
            assert result == set()
        else:
            assert result is None
        assert rows_by_state(database) == {CrawlState.NOT_CRAWLED.value: 2}
    finally:
        await repository.close()


@pytest.mark.parametrize("method_name", COLLECTION_METHODS)
async def test_an_empty_input_is_answered_without_reading_the_schema(
    method_name: str, tmp_path: Path
) -> None:
    """An empty collection is answered by the method itself, before any statement.

    The store is never initialized, so a statement could not have been silent.
    The contrast below is what makes the absence of one an assertion rather
    than a coincidence, and it pins the concrete class: the port promises only
    `Exception`, and `sqlite3.Error` is the class a caller can actually catch.

    Args:
    method_name: The method that must answer without a statement.
    tmp_path: This test's private directory, holding its own database file.

    Returns:
    None
    """
    database = str(tmp_path / "uninitialized.db")
    repository = make_repository(database)
    try:
        result = await _empty_input(repository, method_name)

        if method_name == "claim_urls":
            assert result == set()
        else:
            assert result is None
        with pytest.raises(sqlite3.Error):
            await repository.mark_started({CustomURL(page_urls(1)[0])}, now())
    finally:
        await repository.close()


@pytest.mark.parametrize(
    ("queue_timeout", "pause_seconds", "reclaimed"),
    [(TIMEOUTS.queue, 0.0, False), (EXPIRED, PAUSE_SECONDS, True)],
    ids=["inside_the_window", "past_the_window"],
)
async def test_a_queued_row_is_reclaimed_only_past_its_queue_timeout(
    queue_timeout: timedelta,
    pause_seconds: float,
    reclaimed: bool,
    tmp_path: Path,
) -> None:
    """A `queued` row is reclaimable only once its own window has elapsed, which
    recovers a send that was lost.

    Args:
    queue_timeout: The `queued` staleness window both claims are given.
    pause_seconds: How long to wait between the two claims.
    reclaimed: Whether the second claim must hand the row back.
    tmp_path: This test's private directory, holding its own database file.

    Returns:
    None
    """
    database = str(tmp_path / "crawl.db")
    urls = page_urls(1)
    repository = make_repository(database)
    await repository.initialize()
    try:
        await seed_rows(repository, urls)
        first = await repository.claim_candidates(
            now(), 1, job_timeout=TIMEOUTS.job, queue_timeout=queue_timeout
        )
        assert _urls(first) == set(urls)

        await asyncio.sleep(pause_seconds)
        second = await repository.claim_candidates(
            now(), 1, job_timeout=TIMEOUTS.job, queue_timeout=queue_timeout
        )

        assert _urls(second) == (set(urls) if reclaimed else set())
        assert rows_by_state(database) == {CrawlState.QUEUED.value: 1}
    finally:
        await repository.close()


@pytest.mark.parametrize(
    ("job_timeout", "pause_seconds", "reclaimed"),
    [(TIMEOUTS.job, 0.0, False), (EXPIRED, PAUSE_SECONDS, True)],
    ids=["inside_the_window", "past_the_window"],
)
async def test_a_started_row_is_reclaimed_only_past_its_job_timeout(
    job_timeout: timedelta,
    pause_seconds: float,
    reclaimed: bool,
    tmp_path: Path,
) -> None:
    """A `started_crawl` row is reclaimable only once its window has elapsed, which
    recovers a worker that died.

    Args:
    job_timeout: The `started_crawl` staleness window the claim is given.
    pause_seconds: How long to wait between the mark and the claim.
    reclaimed: Whether the claim must hand the abandoned row back.
    tmp_path: This test's private directory, holding its own database file.

    Returns:
    None
    """
    database = str(tmp_path / "crawl.db")
    urls = page_urls(1)
    repository = make_repository(database)
    await repository.initialize()
    try:
        await seed_rows(repository, urls)
        await repository.mark_started({CustomURL(urls[0])}, now())

        await asyncio.sleep(pause_seconds)
        claimed = await repository.claim_candidates(
            now(), 1, job_timeout=job_timeout, queue_timeout=TIMEOUTS.queue
        )

        state = CrawlState.QUEUED if reclaimed else CrawlState.STARTED_CRAWL
        assert _urls(claimed) == (set(urls) if reclaimed else set())
        assert rows_by_state(database) == {state.value: 1}
    finally:
        await repository.close()


@pytest.mark.parametrize("count", [1, 3])
async def test_claim_candidates_queues_a_row_once(count: int, tmp_path: Path) -> None:
    """One atomic statement hands a due row to one caller, so a second claim is empty.

    Args:
    count: How many due rows the store holds before the first claim.
    tmp_path: This test's private directory, holding its own database file.

    Returns:
    None
    """
    database = str(tmp_path / "crawl.db")
    urls = page_urls(count)
    repository = make_repository(database)
    await repository.initialize()
    try:
        await seed_rows(repository, urls)

        first = await repository.claim_candidates(
            now(), count, job_timeout=TIMEOUTS.job, queue_timeout=TIMEOUTS.queue
        )
        assert _urls(first) == set(urls)
        assert rows_by_state(database) == {CrawlState.QUEUED.value: count}

        second = await repository.claim_candidates(
            now(), count, job_timeout=TIMEOUTS.job, queue_timeout=TIMEOUTS.queue
        )

        assert second == set()
        assert rows_by_state(database) == {CrawlState.QUEUED.value: count}
    finally:
        await repository.close()


@pytest.mark.parametrize("count", [1, 3])
async def test_claim_urls_returns_only_the_callers_urls(
    count: int, tmp_path: Path
) -> None:
    """The caller's restriction sits inside the claiming subquery, so nothing else
    is queued.

    Args:
    count: How many of the seeded rows the caller names.
    tmp_path: This test's private directory, holding its own database file.

    Returns:
    None
    """
    database = str(tmp_path / "crawl.db")
    urls = page_urls(count + 1)
    wanted = urls[:count]
    repository = make_repository(database)
    await repository.initialize()
    try:
        await seed_rows(repository, urls)

        claimed = await repository.claim_urls(
            {CustomURL(url) for url in wanted},
            now(),
            -1,
            job_timeout=TIMEOUTS.job,
            queue_timeout=TIMEOUTS.queue,
        )

        assert _urls(claimed) == set(wanted)
        assert rows_by_state(database) == _states(
            {
                CrawlState.QUEUED.value: count,
                CrawlState.NOT_CRAWLED.value: 1,
            }
        )
    finally:
        await repository.close()


@pytest.mark.parametrize("count", [1, 2])
async def test_claim_urls_does_not_return_a_row_inside_its_window(
    count: int, tmp_path: Path
) -> None:
    """A URL the caller already holds is queued once, so the second claim is empty.

    Args:
    count: How many rows the store holds and the caller names.
    tmp_path: This test's private directory, holding its own database file.

    Returns:
    None
    """
    database = str(tmp_path / "crawl.db")
    urls = page_urls(count)
    repository = make_repository(database)
    await repository.initialize()
    try:
        await seed_rows(repository, urls)
        first = await repository.claim_urls(
            {CustomURL(url) for url in urls},
            now(),
            -1,
            job_timeout=TIMEOUTS.job,
            queue_timeout=TIMEOUTS.queue,
        )
        assert _urls(first) == set(urls)

        second = await repository.claim_urls(
            {CustomURL(url) for url in urls},
            now(),
            -1,
            job_timeout=TIMEOUTS.job,
            queue_timeout=TIMEOUTS.queue,
        )

        assert second == set()
        assert rows_by_state(database) == {CrawlState.QUEUED.value: count}
    finally:
        await repository.close()


@pytest.mark.parametrize("count", [0, 1, 3])
async def test_mark_started_moves_every_url_of_the_set(
    count: int, tmp_path: Path
) -> None:
    """One `IN (...)` update covers a whole batch, recording the attempt itself.

    Args:
    count: How many URLs the set holds and the update must move.
    tmp_path: This test's private directory, holding its own database file.

    Returns:
    None
    """
    database = str(tmp_path / "crawl.db")
    urls = page_urls(count)
    repository = make_repository(database)
    await repository.initialize()
    try:
        await seed_rows(repository, urls)

        await repository.mark_started({CustomURL(url) for url in urls}, now())

        assert rows_by_state(database) == _states(
            {CrawlState.STARTED_CRAWL.value: count}
        )
    finally:
        await repository.close()


async def test_mark_started_chunks_a_set_wider_than_one_statement(
    tmp_path: Path,
) -> None:
    """A batch too wide for one statement is split, and every row of it is still marked.

    Args:
    tmp_path: This test's private directory, holding its own database file.

    Returns:
    None
    """
    database = str(tmp_path / "crawl.db")
    urls = page_urls(CHUNKED_URLS)
    repository = make_repository(database)
    await repository.initialize()
    try:
        await seed_rows(repository, urls)
        assert rows_by_state(database) == {CrawlState.NOT_CRAWLED.value: CHUNKED_URLS}

        await repository.mark_started({CustomURL(url) for url in urls}, now())

        assert rows_by_state(database) == {CrawlState.STARTED_CRAWL.value: CHUNKED_URLS}
    finally:
        await repository.close()


@pytest.mark.parametrize("discovered", [0, 2])
async def test_complete_crawl_records_the_finished_rows_and_inserts_the_discovered(
    discovered: int, tmp_path: Path
) -> None:
    """One transaction writes the outcomes and then the URLs the pages revealed.

    Args:
    discovered: How many new URLs the batch found.
    tmp_path: This test's private directory, holding its own database file.

    Returns:
    None
    """
    database = str(tmp_path / "crawl.db")
    crawled = page_urls(2)
    found = page_urls(2 + discovered)[2:]
    repository = make_repository(database)
    await repository.initialize()
    try:
        await seed_rows(repository, crawled)
        finished = {CustomURL(url): now() for url in crawled}
        await repository.mark_started(set(finished), now())

        await repository.complete_crawl(
            finished, {CustomURL(url) for url in found}, now()
        )

        assert rows_by_state(database) == _states(
            {
                CrawlState.FINISHED_CRAWL.value: len(crawled),
                CrawlState.NOT_CRAWLED.value: discovered,
            }
        )
    finally:
        await repository.close()


@pytest.mark.parametrize(
    ("offset", "reclaimed"),
    [(DUE_PAST, True), (DUE_FUTURE, False), (None, False)],
    ids=["due", "not_due_yet", "never_again"],
)
async def test_a_finished_row_is_claimed_only_while_it_is_due(
    offset: timedelta | None, reclaimed: bool, tmp_path: Path
) -> None:
    """A `finished_crawl` row is due only while its next time has passed, and a NULL
    time never is.

    Args:
    offset: How far from now the next due time is set, or None for no re-crawl.
    reclaimed: Whether the read must hand the row back.
    tmp_path: This test's private directory, holding its own database file.

    Returns:
    None
    """
    database = str(tmp_path / "crawl.db")
    urls = page_urls(1)
    repository = make_repository(database)
    await repository.initialize()
    try:
        await seed_rows(repository, urls)
        await repository.mark_started({CustomURL(urls[0])}, now())

        await repository.complete_crawl(
            {CustomURL(urls[0]): None if offset is None else now() + offset},
            set(),
            now(),
        )
        found = await repository.get_crawlable_urls(
            now(), 1, job_timeout=TIMEOUTS.job, queue_timeout=TIMEOUTS.queue
        )
        stored = _stored_row(database, urls[0])

        assert _urls(found) == (set(urls) if reclaimed else set())
        assert stored["state"] == CrawlState.FINISHED_CRAWL.value
        if offset is None:
            assert stored["next_crawl_time"] is None
        else:
            assert stored["next_crawl_time"] is not None
    finally:
        await repository.close()


@pytest.mark.parametrize(
    "extra",
    [0, 1],
    ids=["the_shared_url_alone", "with_another_discovered_url"],
)
async def test_a_url_finished_and_discovered_in_one_batch_is_written_once(
    extra: int, tmp_path: Path
) -> None:
    """The finish write lands before the insert, so a URL in both is counted once and
    stays finished.

    Args:
    extra: How many further discovered URLs join the shared one.
    tmp_path: This test's private directory, holding its own database file.

    Returns:
    None
    """
    database = str(tmp_path / "crawl.db")
    crawled = page_urls(1)
    shared = CustomURL(crawled[0])
    others = page_urls(1 + extra)[1:]
    repository = make_repository(database)
    await repository.initialize()
    try:
        await seed_rows(repository, crawled)
        await repository.mark_started({shared}, now())

        await repository.complete_crawl(
            {shared: now()}, {shared, *(CustomURL(url) for url in others)}, now()
        )

        assert rows_by_state(database) == _states(
            {
                CrawlState.FINISHED_CRAWL.value: 1,
                CrawlState.NOT_CRAWLED.value: extra,
            }
        )
        assert _stored_row(database, crawled[0])["times_crawled"] == 1
    finally:
        await repository.close()


async def test_close_twice_does_not_raise(tmp_path: Path) -> None:
    """`close` is idempotent, so a shutdown path may call it without knowing who closed
    the store.

    Args:
    tmp_path: This test's private directory, holding its own database file.

    Returns:
    None
    """
    repository = make_repository(str(tmp_path / "crawl.db"))
    await repository.initialize()

    await repository.close()
    await repository.close()


@pytest.mark.parametrize("method_name", READER_METHODS)
async def test_a_collection_returning_method_returns_a_set(
    method_name: str, tmp_path: Path
) -> None:
    """Every read and claim hands back a set, so a caller never dedupes.

    Args:
    method_name: The method whose return type is checked.
    tmp_path: This test's private directory, holding its own database file.

    Returns:
    None
    """
    database = str(tmp_path / "crawl.db")
    urls = page_urls(1)
    repository = make_repository(database)
    await repository.initialize()
    try:
        await seed_rows(repository, urls)

        found = await _read_or_claim(repository, method_name)

        assert isinstance(found, set)
        assert _urls(found) == set(urls)
    finally:
        await repository.close()
