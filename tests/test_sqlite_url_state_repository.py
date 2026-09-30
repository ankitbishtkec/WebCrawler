"""Unit tests for `SQLiteURLStateRepository`, the only crawl state store shipped.

`aiosqlite.connect` is patched, so no database file is ever opened and no
statement reaches SQLite: the store's whole job is assembling a statement and
binding it, so every test here asserts that a statement and its values reached
the connection, and nothing else.
"""

from collections.abc import AsyncIterator, Generator, Iterator, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from webcrawler.domain.crawl_state import CrawlState
from webcrawler.domain.custom_url import CustomURL
from webcrawler.infrastructure.db import models
from webcrawler.infrastructure.db.sqlite_url_state_repository import (
    MARK_STARTED_BATCH_SQL,
    SQLiteURLStateRepository)

# The host every URL in this file is built on; nothing here is ever fetched.
HOST: str = "https://site.test"

# The one instant the clock double reports, so every bound value is exact.
NOW: datetime = datetime(2026, 9, 29, 6, 0, tzinfo=timezone.utc)

# The two staleness windows every read and claim in this file is given.
JOB_TIMEOUT: timedelta = timedelta(minutes=1)
QUEUE_TIMEOUT: timedelta = timedelta(seconds=30)

# The same two windows as the whole seconds the statements bind.
JOB_TIMEOUT_SECONDS: int = 60
QUEUE_TIMEOUT_SECONDS: int = 30

# The shared head of the batched mark-started statement, up to the `IN (...)`
# list, whose placeholder order follows the set's own order and is not promised.
MARK_STARTED_HEAD: str = MARK_STARTED_BATCH_SQL.split("IN (")[0]


class _FakeCursor:
    """The cursor a mocked `execute` hands back, carrying the rows a test chose.

    Args:
        rows: The rows this cursor reports, each one the `custom_url` column.
    """

    def __init__(self, rows: Sequence[Sequence[str]]) -> None:
        """Copy the rows this cursor will report.

        Args:
            rows: The rows `fetchall` returns, in the order given.
        """
        self._rows = list(rows)

    async def fetchall(self) -> list[Sequence[str]]:
        """Return every row this cursor was built with.

        Returns:
            list[Sequence[str]]: The rows, as a live cursor would drain them.
        """
        return list(self._rows)

    async def __aiter__(self) -> AsyncIterator[Sequence[str]]:
        """Yield the rows one at a time, as a live cursor would.

        Returns:
            AsyncIterator[Sequence[str]]: An iterator over the same rows.
        """
        for row in self._rows:
            yield row


class _FakeExecuteContext:
    """The async context manager every mocked statement call returns.

    Args:
        cursor: The cursor `__aenter__` hands to the repository.
    """

    def __init__(self, cursor: _FakeCursor) -> None:
        """Hold the cursor to hand back on entry.

        Args:
            cursor: The cursor the `async with` block receives.
        """
        self._cursor = cursor

    async def __aenter__(self) -> _FakeCursor:
        """Enter the block and hand over the cursor.

        Returns:
            _FakeCursor: The cursor holding the rows a test chose.
        """
        return self._cursor

    async def __aexit__(self, *error: object) -> None:
        """Leave the block, as the aiosqlite context manager does.

        Args:
            error: The exception triple, empty on the happy path.

        Returns:
            None
        """
        return None


class _FakeConnection:
    """The aiosqlite connection stand-in the repository drives.

    Args:
        rows: The rows every cursor this connection hands out reports.
    """

    def __init__(self, rows: Sequence[Sequence[str]] = ()) -> None:
        """Expose one mock per connection method and record the rows to report.

        Args:
            rows: The rows every `fetchall` hands back. A test may assign
                `rows` again before its call to choose what the cursor yields.
        """
        self.rows = list(rows)
        # aiosqlite's `execute` and `executemany` are plain methods returning a
        # context manager, so these two are sync mocks; only `close` is awaited.
        self.execute = MagicMock(side_effect=self._context)
        self.executemany = MagicMock(side_effect=self._context)
        self.close = AsyncMock()

    def _context(self, *_args: object, **_kwargs: object) -> _FakeExecuteContext:
        """Return a context manager over the rows this connection was given.

        Args:
            *_args: The statement and its parameters, recorded by the mock itself.
            **_kwargs: Nothing is passed by keyword.

        Returns:
            _FakeExecuteContext: A context manager yielding a cursor over `rows`.
        """
        return _FakeExecuteContext(_FakeCursor(self.rows))

    def __await__(self) -> Generator[Any, None, None]:
        """Stand in for `await connection`, which starts aiosqlite's thread.

        Returns:
            Generator[Any, None, None]: An awaitable that is already done, so no
            worker thread starts and no file is opened.
        """
        async def _ready() -> None:
            """Finish at once, in place of the connection's worker thread.

            Returns:
                None
            """
            return None

        return _ready().__await__()


@dataclass(frozen=True)
class _Store:
    """The store under test and the fake connection it drives.

    Args:
        repository: The store, constructed over `connection`.
        connection: The fake connection recording every statement and the values
            bound to it.
    """

    repository: SQLiteURLStateRepository
    connection: _FakeConnection


def urls(count: int) -> set[CustomURL]:
    """Build the canonical URLs of pages 1 through `count`.

    Args:
        count: How many pages to name.

    Returns:
        set[CustomURL]: One URL per page, unordered.
    """
    return {
        CustomURL(f"{HOST}/page-{index}.html") for index in range(1, count + 1)
    }


@pytest.fixture
def store() -> Iterator[_Store]:
    """Build the store over a fake connection, so nothing is opened.

    `aiosqlite.connect` is patched, so the constructor runs and the path is
    never touched; the clock double reports `NOW`, and the retry policy double
    is never consulted on the happy path.

    Yields:
        _Store: The store under test and the connection it drives.
    """
    connection = _FakeConnection()
    time_provider = MagicMock()
    time_provider.now.return_value = NOW
    with patch("aiosqlite.connect", return_value=connection):
        yield _Store(
            repository=SQLiteURLStateRepository(
                "unused.db", AsyncMock(), time_provider
            ),
            connection=connection,
        )


async def test_initialize_creates_the_schema(store: _Store) -> None:
    """The SQLite version is checked once, then every schema statement is run.

    Args:
        store: The store under test.

    Returns:
        None
    """
    with patch.object(models, "require_returning_support") as require_support:
        await store.repository.initialize()

    require_support.assert_called_once_with()
    statements = [
        args[0] for args, _ in store.connection.execute.call_args_list
    ]
    for statement in models.SCHEMA_STATEMENTS:
        assert statements.count(statement) == 1


@pytest.mark.parametrize("count", [1, 3])
async def test_create_urls_inserts_the_batch(store: _Store, count: int) -> None:
    """One insert carries every URL of the set and the injected instant.

    Args:
        store: The store under test.
        count: How many URLs the caller's set holds.

    Returns:
        None
    """
    wanted = urls(count)

    await store.repository.create_urls(wanted)

    statement, rows = store.connection.executemany.call_args.args
    assert statement == models.INSERT_URL_SQL
    assert {row["custom_url"] for row in rows} == {url.get_url() for url in wanted}
    assert {row["created_time"] for row in rows} == {NOW}


async def test_get_crawlable_urls_returns_the_rows_its_cursor_ran(
    store: _Store,
) -> None:
    """The read maps the rows the cursor yielded and binds both timeouts.

    Args:
        store: The store under test.

    Returns:
        None
    """
    store.connection.rows = [
        (f"{HOST}/page-1.html",),
        (f"{HOST}/page-2.html",),
    ]

    found = await store.repository.get_crawlable_urls(
        NOW, -1, job_timeout=JOB_TIMEOUT, queue_timeout=QUEUE_TIMEOUT
    )

    assert found == {
        CustomURL(f"{HOST}/page-1.html"), CustomURL(f"{HOST}/page-2.html")
    }
    statement, parameters = store.connection.execute.call_args.args
    assert statement == models.crawlable_select_statement()
    assert parameters["job_timeout_seconds"] == JOB_TIMEOUT_SECONDS
    assert parameters["queue_timeout_seconds"] == QUEUE_TIMEOUT_SECONDS


async def test_claim_candidates_returns_the_claimed_rows(store: _Store) -> None:
    """The claim runs between a BEGIN and a COMMIT, binding the queued state.

    Args:
        store: The store under test.

    Returns:
        None
    """
    store.connection.rows = [(f"{HOST}/page-1.html",)]

    claimed = await store.repository.claim_candidates(
        NOW, 1, job_timeout=JOB_TIMEOUT, queue_timeout=QUEUE_TIMEOUT
    )

    assert claimed == {CustomURL(f"{HOST}/page-1.html")}
    # The middle of the three calls is the claim: BEGIN, claim, then COMMIT.
    statement, parameters = store.connection.execute.call_args_list[1].args
    assert statement == models.claim_statement()
    assert parameters["claimed_state"] == CrawlState.QUEUED.value
    assert parameters["job_timeout_seconds"] == JOB_TIMEOUT_SECONDS
    assert parameters["queue_timeout_seconds"] == QUEUE_TIMEOUT_SECONDS


@pytest.mark.parametrize("count", [1, 3])
async def test_mark_started_updates_the_whole_batch(
    store: _Store, count: int
) -> None:
    """One widened update binds the started state, the instant, and every URL.

    Args:
        store: The store under test.
        count: How many URLs the caller's set holds.

    Returns:
        None
    """
    wanted = urls(count)

    await store.repository.mark_started(wanted, NOW)

    # Again the middle call: BEGIN, the one update, then COMMIT.
    statement, parameters = store.connection.execute.call_args_list[1].args
    assert statement.startswith(MARK_STARTED_HEAD)
    assert statement.count(":u") == count
    assert parameters["started_state"] == CrawlState.STARTED_CRAWL.value
    assert parameters["now"] == NOW
    bound = {value for name, value in parameters.items() if name.startswith("u")}
    assert bound == {url.get_url() for url in wanted}


async def test_complete_crawl_writes_the_outcomes_and_the_discoveries(
    store: _Store,
) -> None:
    """The outcome update runs first, then the insert of what the pages revealed.

    Args:
        store: The store under test.

    Returns:
        None
    """
    crawled = CustomURL(f"{HOST}/page-1.html")
    found = CustomURL(f"{HOST}/page-2.html")

    await store.repository.complete_crawl({crawled: NOW}, {found}, NOW)

    calls = store.connection.executemany.call_args_list
    assert [args[0] for args, _ in calls] == [
        models.COMPLETE_CRAWL_SQL,
        models.INSERT_URL_SQL,
    ]
    finished_rows = calls[0].args[1]
    discovered_rows = calls[1].args[1]
    assert [row["custom_url"] for row in finished_rows] == [crawled.get_url()]
    assert [row["custom_url"] for row in discovered_rows] == [found.get_url()]
    assert finished_rows[0]["finished_state"] == CrawlState.FINISHED_CRAWL.value


async def test_close_awaits_the_connection_close(store: _Store) -> None:
    """`close` awaits the connection, so the worker thread behind it can end.

    Args:
        store: The store under test.

    Returns:
        None
    """
    await store.repository.close()

    store.connection.close.assert_awaited_once_with()
