"""Tests for `SQLiteURLStateRepository`."""

import asyncio
import contextlib
import logging
import sqlite3
import threading
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import TypeVar

import aiosqlite
import pytest

from webcrawler.domain.crawl_state import CrawlState
from webcrawler.domain.custom_url import CustomURL
from webcrawler.infrastructure.db import models
from webcrawler.infrastructure.db import sqlite_url_state_repository as store_module
from webcrawler.infrastructure.db.sqlite_url_state_repository import (
    SQLiteURLStateRepository,
)
from webcrawler.ports.retry_policy import RetryPolicy
from webcrawler.ports.time_provider import TimeProviderFactory

T = TypeVar("T")

NOW = datetime(2026, 9, 26, 11, 28, 0, tzinfo=timezone.utc)
STAMP = "2026-09-26 11:28:00"
JOB_TIMEOUT = timedelta(minutes=5)
QUEUE_TIMEOUT = timedelta(seconds=30)
LOGGER_NAME = "tests.webcrawler.url_state"

# One entry per eligibility method, so a single table of cases drives all three:
# the read-only select and the two claims share one predicate by construction.
METHODS = ("read_only", "claim_candidates", "claim_urls")


def url(index: int) -> CustomURL:
    """Return the nth crawlable URL of the test site."""
    return CustomURL(f"http://crawlme.monzo.com/p{index}")


def stamp(moment: datetime) -> str:
    """Return the stored text of an instant."""
    return moment.strftime("%Y-%m-%d %H:%M:%S")


def read_rows(path: Path) -> list[sqlite3.Row]:
    """Read every URL row in canonical order, as named columns."""
    with contextlib.closing(sqlite3.connect(path)) as connection:
        connection.row_factory = sqlite3.Row
        return connection.execute(
            "SELECT * FROM urls ORDER BY custom_url"
        ).fetchall()


def epoch_of(path: Path, column: str) -> list[str | None]:
    """Return `strftime('%s', column)` for every row, in canonical order."""
    with contextlib.closing(sqlite3.connect(path)) as connection:
        return [
            row[0]
            for row in connection.execute(
                f"SELECT strftime('%s', {column}) FROM urls ORDER BY custom_url"
            )
        ]


def seed(
    path: Path,
    raw: str,
    state: str,
    *,
    next_crawl_time: datetime | None = None,
    last_status_update_time: datetime | None = None,
    last_crawl_time: datetime | None = None,
    created_time: datetime | None = None,
) -> None:
    """Write one row directly, so a test can pose a state the API cannot reach."""
    created = created_time or last_status_update_time or next_crawl_time or NOW
    updated = last_status_update_time or next_crawl_time or created
    with contextlib.closing(sqlite3.connect(path)) as connection:
        connection.execute(
            "INSERT OR REPLACE INTO urls (custom_url, created_time,"
            " last_crawl_time, next_crawl_time, state, last_status_update_time)"
            " VALUES (?, ?, ?, ?, ?, ?)",
            (raw, created, last_crawl_time, next_crawl_time, state, updated),
        )
        connection.commit()


def add_trigger(path: Path, name: str, timing: str, when: str = "") -> None:
    """Create a trigger that aborts a statement, so a test can fail a write."""
    with contextlib.closing(sqlite3.connect(path)) as connection:
        connection.execute(
            f"CREATE TRIGGER {name} {timing} {when}"
            f" BEGIN SELECT RAISE(ABORT, 'rejected by test'); END"
        )
        connection.commit()


def index_columns(path: Path, index_name: str) -> tuple[str, ...]:
    """Return the columns an index covers, in index order."""
    with contextlib.closing(sqlite3.connect(path)) as connection:
        return tuple(
            row[2]
            for row in connection.execute(f"PRAGMA index_info('{index_name}')")
        )


def index_names(path: Path) -> set[str]:
    """Return every index the `urls` table has."""
    with contextlib.closing(sqlite3.connect(path)) as connection:
        return {row[1] for row in connection.execute("PRAGMA index_list('urls')")}


async def read_or_claim(
    store: SQLiteURLStateRepository,
    method: str,
    urls: list[CustomURL],
    now: datetime,
    max_items: int,
    *,
    job_timeout_seconds: int | None = None,
    queue_timeout_seconds: int | None = None,
) -> list[CustomURL]:
    """Dispatch to the three eligibility methods, so one table drives all three."""
    job_timeout = timedelta(
        seconds=(
            JOB_TIMEOUT.total_seconds()
            if job_timeout_seconds is None
            else job_timeout_seconds
        )
    )
    queue_timeout = timedelta(
        seconds=(
            QUEUE_TIMEOUT.total_seconds()
            if queue_timeout_seconds is None
            else queue_timeout_seconds
        )
    )
    if method == "read_only":
        return await store.get_crawlable_urls(
            now, max_items, job_timeout=job_timeout, queue_timeout=queue_timeout
        )
    if method == "claim_candidates":
        return await store.claim_candidates(
            now, max_items, job_timeout=job_timeout, queue_timeout=queue_timeout
        )
    if method == "claim_urls":
        return await store.claim_urls(
            urls, now, max_items, job_timeout=job_timeout, queue_timeout=queue_timeout
        )
    raise ValueError(f"unknown method {method!r}")


def seed_due_rows(path: Path, count: int, *, state: str) -> list[CustomURL]:
    """Write rows that are all eligible, earliest `next_crawl_time` first."""
    targets = [url(index) for index in range(count)]
    for index, target in enumerate(targets):
        seed(
            path,
            target.get_url(),
            state,
            next_crawl_time=NOW - timedelta(seconds=100 - index),
        )
    return targets


class FixedTimeProvider(TimeProviderFactory):
    """A `TimeProviderFactory` that returns one instant, or a scripted list."""

    def __init__(self, moments: datetime | list[datetime]) -> None:
        self._moments = [moments] if isinstance(moments, datetime) else list(moments)
        self.calls = 0

    def now(self) -> datetime:
        """Return the next scripted instant."""
        moment = self._moments[min(self.calls, len(self._moments) - 1)]
        self.calls += 1
        return moment


class CountingRetryPolicy(RetryPolicy):
    """The smallest `RetryPolicy`: a fixed attempt budget and no delay."""

    def __init__(self, max_attempts: int = 3) -> None:
        self.max_attempts = max_attempts
        self.calls = 0
        self.attempts = 0

    async def execute(self, operation: Callable[[], Awaitable[T]]) -> T:
        """Run operation until it succeeds or the budget is spent."""
        self.calls += 1
        failure: BaseException | None = None
        for _ in range(self.max_attempts):
            self.attempts += 1
            try:
                return await operation()
            except Exception as error:  # recorded here, re-raised below
                failure = error
        assert failure is not None
        raise failure


class RecordingConnection:
    """Wraps an aiosqlite connection, recording statements and injecting faults."""

    def __init__(self, connection: aiosqlite.Connection) -> None:
        self._connection = connection
        self.statements: list[str] = []
        self._remaining = 0
        self._error: BaseException = sqlite3.OperationalError("unset")
        self._target: str | None = None

    @property
    def worker_thread(self) -> threading.Thread:
        """Return the aiosqlite worker thread, so a test can prove it ended."""
        return self._connection._thread

    def fail_next(self, count: int, error: BaseException) -> None:
        """Arm the next `count` statements to fail before reaching SQLite."""
        self._remaining = count
        self._error = error

    def fail_statement(self, sql: str, error: BaseException) -> None:
        """Arm one named statement to fail, wherever it appears in the batch."""
        self._target = sql
        self._error = error

    def execute(self, sql: str, parameters: object = None) -> object:
        """Record one statement and delegate, unless a fault is armed."""
        self.statements.append(sql)
        if self._remaining > 0:
            self._remaining -= 1
            raise self._error
        if self._target is not None and sql == self._target:
            self._target = None
            raise self._error
        return self._connection.execute(sql, parameters)

    def __call__(self) -> RecordingConnection:
        """Return self, so `await connection()` still starts the worker thread."""
        return self

    def __await__(self) -> object:
        """Forward the await that starts the wrapped connection's thread."""
        return self._connection.__await__()

    def __getattr__(self, name: str) -> object:
        """Forward every other attribute to the wrapped connection."""
        return getattr(self._connection, name)


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    """Return a fresh, empty database file path for one test."""
    return tmp_path / "urls.db"


@pytest.fixture
def retry_policy() -> CountingRetryPolicy:
    """Return the fake retry policy every repository under test is given."""
    return CountingRetryPolicy()


@pytest.fixture
def time_provider() -> FixedTimeProvider:
    """Return a clock frozen at `NOW`, so every written stamp is predictable."""
    return FixedTimeProvider(NOW)


@pytest.fixture
def logger() -> logging.Logger:
    """Return the real logger the repository is given, for caplog to capture."""
    return logging.getLogger(LOGGER_NAME)


def build(
    path: Path,
    retry_policy: RetryPolicy,
    time_provider: FixedTimeProvider,
    logger: logging.Logger,
) -> SQLiteURLStateRepository:
    """Construct the repository the way the composition root does."""
    return SQLiteURLStateRepository(path, retry_policy, time_provider, logger)


@pytest.fixture
async def repository(
    db_path: Path,
    retry_policy: CountingRetryPolicy,
    time_provider: FixedTimeProvider,
    logger: logging.Logger,
) -> AsyncIterator[SQLiteURLStateRepository]:
    """Yield an initialized store on a real file, always closed afterwards."""
    store = build(db_path, retry_policy, time_provider, logger)
    try:
        await store.initialize()
        yield store
    finally:
        await store.close()


@pytest.fixture
async def new_repository(
    db_path: Path,
    retry_policy: CountingRetryPolicy,
    time_provider: FixedTimeProvider,
    logger: logging.Logger,
) -> AsyncIterator[Callable[[], SQLiteURLStateRepository]]:
    """Yield a factory for extra stores on the same file, all closed after."""
    opened: list[SQLiteURLStateRepository] = []

    def factory() -> SQLiteURLStateRepository:
        """Return one more unopened store on the shared file."""
        store = build(db_path, retry_policy, time_provider, logger)
        opened.append(store)
        return store

    try:
        yield factory
    finally:
        for store in opened:
            await store.close()


@pytest.fixture
async def spied_repository(
    db_path: Path,
    retry_policy: CountingRetryPolicy,
    time_provider: FixedTimeProvider,
    logger: logging.Logger,
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncIterator[tuple[SQLiteURLStateRepository, RecordingConnection]]:
    """Yield an initialized store whose statements can be inspected."""
    real_connect = aiosqlite.connect
    spy = RecordingConnection(real_connect(db_path, isolation_level=None))
    monkeypatch.setattr(store_module.aiosqlite, "connect", lambda *a, **k: spy)
    store = build(db_path, retry_policy, time_provider, logger)
    try:
        await store.initialize()
        spy.statements.clear()
        yield store, spy
    finally:
        await store.close()


@pytest.mark.parametrize("version", [(3, 20, 0), (3, 32, 0), (3, 34, 9)])
def test_an_old_sqlite_is_refused(version: tuple[int, int, int]) -> None:
    """The claim needs 3.35, so an older runtime is reported, not discovered."""
    with pytest.raises(sqlite3.NotSupportedError):
        models.require_returning_support(version)


@pytest.mark.parametrize("version", [(3, 35, 0), (3, 35, 1), (3, 50, 4), (4, 0, 0)])
def test_a_new_enough_sqlite_is_accepted(version: tuple[int, int, int]) -> None:
    """3.35 and newer run the claim query."""
    models.require_returning_support(version)


@pytest.mark.parametrize("version", [(3, 20, 0), (3, 34, 0)])
async def test_initialize_refuses_an_old_runtime(
    db_path: Path,
    retry_policy: CountingRetryPolicy,
    time_provider: FixedTimeProvider,
    logger: logging.Logger,
    monkeypatch: pytest.MonkeyPatch,
    version: tuple[int, int, int],
) -> None:
    """`initialize` is where the version is checked, so a claim never fails on it."""
    monkeypatch.setattr(sqlite3, "sqlite_version_info", version)
    store = build(db_path, retry_policy, time_provider, logger)
    try:
        with pytest.raises(sqlite3.NotSupportedError):
            await store.initialize()
    finally:
        await store.close()


@pytest.mark.parametrize(
    ("index_name", "columns"),
    [
        (
            models.COMPOSITE_INDEX_NAME,
            ("state", "next_crawl_time", "last_status_update_time"),
        ),
        (models.PRIMARY_KEY_INDEX_NAME, ("custom_url",)),
    ],
)
async def test_initialize_creates_both_indexes(
    db_path: Path,
    retry_policy: CountingRetryPolicy,
    time_provider: FixedTimeProvider,
    logger: logging.Logger,
    index_name: str,
    columns: tuple[str, ...],
) -> None:
    """`goal.md:29-31` wants one index per key and one composite."""
    store = build(db_path, retry_policy, time_provider, logger)
    try:
        await store.initialize()
    finally:
        await store.close()
    assert index_name in index_names(db_path)
    assert index_columns(db_path, index_name) == columns


@pytest.mark.parametrize("calls", [1, 2, 3])
async def test_initialize_is_idempotent(
    repository: SQLiteURLStateRepository,
    db_path: Path,
    calls: int,
) -> None:
    """The schema is created on every start, so `IF NOT EXISTS` must hold."""
    for _ in range(calls):
        await repository.initialize()
        await repository.create_urls([url(0)])
        assert len(read_rows(db_path)) == 1


@pytest.mark.parametrize(
    ("raw", "stored"),
    [
        ("http://crawlme.monzo.com/", "http://crawlme.monzo.com/"),
        (
            "HTTPS://Crawlme.Monzo.com:443/A?b=2&a=1#top",
            "https://crawlme.monzo.com/A?a=1&b=2",
        ),
        (
            "http://crawlme.monzo.com:8080/A?b=2&a=1#top",
            "http://crawlme.monzo.com:8080/A?a=1&b=2",
        ),
        ("https://crawlme.monzo.com/x", "https://crawlme.monzo.com/x"),
    ],
)
async def test_create_urls_stores_the_canonical_url_with_equal_timestamps(
    repository: SQLiteURLStateRepository,
    db_path: Path,
    raw: str,
    stored: str,
) -> None:
    """`goal.md:27`: the key is the canonical form and the two times are equal."""
    await repository.create_urls([CustomURL(raw)])
    row = read_rows(db_path)[0]
    assert row["custom_url"] == stored
    assert row["created_time"] == row["next_crawl_time"] == STAMP


@pytest.mark.parametrize(
    ("column", "expected"),
    [
        ("state", CrawlState.NOT_CRAWLED.value),
        ("created_time", STAMP),
        ("next_crawl_time", STAMP),
        ("last_status_update_time", STAMP),
        ("last_crawl_time", None),
    ],
)
async def test_a_new_row_carries_the_documented_defaults(
    repository: SQLiteURLStateRepository,
    db_path: Path,
    column: str,
    expected: str | None,
) -> None:
    """A fresh row is `not_crawled`, never attempted, and stamped from the clock."""
    await repository.create_urls([url(0)])
    assert read_rows(db_path)[0][column] == expected


@pytest.mark.parametrize("calls", [1, 2, 5])
async def test_create_urls_never_resets_an_existing_row(
    repository: SQLiteURLStateRepository,
    db_path: Path,
    calls: int,
) -> None:
    """A repeated insert is skipped, so a re-seeding never loses a schedule."""
    target = url(0)
    await repository.create_urls([target])
    await repository.mark_started(target, NOW)
    await repository.complete_crawl(
        [(target, NOW + timedelta(hours=1))], [url(1)], NOW
    )
    for _ in range(calls):
        await repository.create_urls([target, url(1)])
        rows = read_rows(db_path)
        assert len(rows) == 2
        finished = next(
            row for row in rows if row["custom_url"] == target.get_url()
        )
        assert finished["state"] == CrawlState.FINISHED_CRAWL.value
        assert finished["next_crawl_time"] == stamp(NOW + timedelta(hours=1))


@pytest.mark.parametrize(
    "operation",
    [
        "create_urls",
        "get_crawlable_urls",
        "claim_candidates",
        "claim_urls",
        "complete_crawl",
    ],
)
async def test_last_crawl_time_is_written_only_by_mark_started(
    repository: SQLiteURLStateRepository,
    db_path: Path,
    operation: str,
) -> None:
    """Only `mark_started` records an attempt."""
    target = url(0)
    await repository.create_urls([target])
    assert read_rows(db_path)[0]["last_crawl_time"] is None
    await repository.mark_started(target, NOW)
    for _ in range(2):
        if operation == "create_urls":
            await repository.create_urls([url(1)])
        elif operation == "get_crawlable_urls":
            await repository.get_crawlable_urls(
                NOW, -1, job_timeout=JOB_TIMEOUT, queue_timeout=QUEUE_TIMEOUT
            )
        elif operation == "claim_candidates":
            await read_or_claim(repository, "claim_candidates", [], NOW, -1)
        elif operation == "claim_urls":
            await read_or_claim(repository, "claim_urls", [target], NOW, -1)
        else:
            await repository.complete_crawl([(target, None)], [], NOW)
        # The attempt time is still exactly the value mark_started stored.
        assert read_rows(db_path)[0]["last_crawl_time"] == STAMP
        assert all(row["last_crawl_time"] is None for row in read_rows(db_path)[1:])


@pytest.mark.parametrize("method", METHODS)
@pytest.mark.parametrize(
    ("state", "next_delta", "age", "expected"),
    [
        pytest.param(CrawlState.NOT_CRAWLED, None, None, True, id="not_crawled"),
        pytest.param(
            CrawlState.NOT_CRAWLED,
            3600,
            None,
            True,
            id="not_crawled_with_future_next",
        ),
        pytest.param(CrawlState.FINISHED_CRAWL, -1, None, True, id="finished_due"),
        pytest.param(
            CrawlState.FINISHED_CRAWL,
            0,
            None,
            True,
            id="finished_exactly_due",
        ),
        pytest.param(
            CrawlState.FINISHED_CRAWL,
            60,
            None,
            False,
            id="finished_not_due",
        ),
        pytest.param(
            CrawlState.FINISHED_CRAWL,
            None,
            None,
            False,
            id="finished_null_next",
        ),
        pytest.param(
            CrawlState.STARTED_CRAWL,
            None,
            3600,
            True,
            id="started_well_past_job_timeout",
        ),
        pytest.param(
            CrawlState.STARTED_CRAWL,
            None,
            300,
            True,
            id="started_exactly_at_job_timeout",
        ),
        pytest.param(CrawlState.STARTED_CRAWL, None, 299, False, id="started_fresh"),
        pytest.param(CrawlState.QUEUED, None, 60, True, id="queued_past_queue_timeout"),
        pytest.param(
            CrawlState.QUEUED,
            None,
            30,
            True,
            id="queued_exactly_at_queue_timeout",
        ),
        pytest.param(CrawlState.QUEUED, None, 29, False, id="queued_fresh"),
    ],
)
async def test_every_branch_of_the_shared_predicate(
    repository: SQLiteURLStateRepository,
    db_path: Path,
    method: str,
    state: str,
    next_delta: int | None,
    age: int | None,
    expected: bool,
) -> None:
    """One row per branch of `goal.md:76-91`, checked through all three methods."""
    target = url(0)
    seed(
        db_path,
        target.get_url(),
        state,
        next_crawl_time=(
            None if next_delta is None else NOW + timedelta(seconds=next_delta)
        ),
        last_status_update_time=(
            None if age is None else NOW - timedelta(seconds=age)
        ),
    )
    rows = await read_or_claim(repository, method, [target], NOW, -1)
    assert rows == ([target] if expected else [])


@pytest.mark.parametrize("method", ["read_only", "claim_candidates"])
@pytest.mark.parametrize(
    ("state", "age", "timeout", "expected"),
    [
        pytest.param(CrawlState.QUEUED, 29, 30, False, id="queued_one_short"),
        pytest.param(CrawlState.QUEUED, 30, 30, True, id="queued_on_the_second"),
        pytest.param(CrawlState.QUEUED, 31, 30, True, id="queued_one_over"),
        pytest.param(
            CrawlState.STARTED_CRAWL, 299, 300, False, id="started_one_short"
        ),
        pytest.param(
            CrawlState.STARTED_CRAWL, 300, 300, True, id="started_on_the_second"
        ),
        pytest.param(CrawlState.STARTED_CRAWL, 301, 300, True, id="started_one_over"),
    ],
)
async def test_staleness_is_measured_in_seconds_not_years(
    repository: SQLiteURLStateRepository,
    db_path: Path,
    method: str,
    state: str,
    age: int,
    timeout: int,
    expected: bool,
) -> None:
    """A row is stale on the second, which a TEXT subtraction would never see."""
    target = url(0)
    updated = NOW - timedelta(seconds=age)
    seed(
        db_path,
        target.get_url(),
        state,
        last_status_update_time=updated,
        created_time=updated,
    )
    reported = await read_or_claim(
        repository, method, [], NOW, -1, job_timeout_seconds=timeout
    )
    assert (reported == [target]) is expected


@pytest.mark.parametrize(
    ("state", "age", "expected"),
    [
        pytest.param(
            CrawlState.STARTED_CRAWL,
            299,
            False,
            id="started_within_the_job_timeout",
        ),
        pytest.param(
            CrawlState.STARTED_CRAWL,
            300,
            True,
            id="started_past_the_job_timeout",
        ),
        pytest.param(
            CrawlState.QUEUED, 29, False, id="queued_within_the_queue_timeout"
        ),
        pytest.param(CrawlState.QUEUED, 30, True, id="queued_past_the_queue_timeout"),
    ],
)
async def test_the_read_honours_the_real_in_flight_timeouts(
    repository: SQLiteURLStateRepository,
    db_path: Path,
    state: str,
    age: int,
    expected: bool,
) -> None:
    """The preview must not advertise a row somebody is still working on."""
    target = url(0)
    seed(
        db_path,
        target.get_url(),
        state,
        last_status_update_time=NOW - timedelta(seconds=age),
    )
    reported = await read_or_claim(repository, "read_only", [target], NOW, -1)
    assert (reported == [target]) is expected
    # The read is read-only, so the state it refused to touch is still there.
    assert read_rows(db_path)[0]["state"] == state


@pytest.mark.parametrize(
    ("state", "age", "expected"),
    [
        pytest.param(CrawlState.FINISHED_CRAWL, None, False, id="finished_null_next"),
        pytest.param(CrawlState.STARTED_CRAWL, 3600, True, id="started_null_next_stale"),
        pytest.param(CrawlState.QUEUED, 3600, True, id="queued_null_next_stale"),
    ],
)
async def test_a_null_next_crawl_time_blocks_only_the_finished_branch(
    repository: SQLiteURLStateRepository,
    db_path: Path,
    state: str,
    age: int | None,
    expected: bool,
) -> None:
    """No top-level null guard, or an abandoned row would be unreclaimable."""
    target = url(0)
    seed(
        db_path,
        target.get_url(),
        state,
        next_crawl_time=None,
        last_status_update_time=(
            None if age is None else NOW - timedelta(seconds=age)
        ),
    )
    claimed = await read_or_claim(repository, "claim_candidates", [], NOW, -1)
    assert (claimed == [target]) is expected


@pytest.mark.parametrize(
    "entry_point", ["create_urls", "complete_crawl"]
)
async def test_a_not_crawled_row_always_has_a_next_crawl_time(
    repository: SQLiteURLStateRepository,
    db_path: Path,
    entry_point: str,
) -> None:
    """Both insert paths set the column, so the state never needs a null guard."""
    if entry_point == "create_urls":
        await repository.create_urls([url(0), url(1)])
        inserted = [url(0), url(1)]
    else:
        target = url(0)
        await repository.create_urls([target])
        await repository.mark_started(target, NOW)
        await repository.complete_crawl([(target, None)], [url(1)], NOW)
        inserted = [url(1)]
    rows = {row["custom_url"]: row for row in read_rows(db_path)}
    assert len(rows) == 2
    for fresh in inserted:
        row = rows[fresh.get_url()]
        assert row["state"] == CrawlState.NOT_CRAWLED.value
        assert row["next_crawl_time"] == row["created_time"] == STAMP


@pytest.mark.parametrize("method", METHODS)
async def test_the_earliest_eligible_row_wins(
    repository: SQLiteURLStateRepository,
    db_path: Path,
    method: str,
) -> None:
    """With `max_items=1` all three methods return the same earliest row."""
    targets = seed_due_rows(db_path, 3, state=CrawlState.FINISHED_CRAWL.value)
    assert await read_or_claim(repository, method, targets, NOW, 1) == [targets[0]]


@pytest.mark.parametrize(
    ("method", "max_items", "expected"),
    [
        pytest.param("read_only", -1, 4, id="read_unlimited"),
        pytest.param("read_only", 0, 0, id="read_zero"),
        pytest.param("read_only", 2, 2, id="read_limited"),
        pytest.param("read_only", 9, 4, id="read_over_available"),
        pytest.param("claim_candidates", -1, 4, id="candidates_unlimited"),
        pytest.param("claim_candidates", 2, 2, id="candidates_limited"),
        pytest.param("claim_urls", -1, 4, id="urls_unlimited"),
        pytest.param("claim_urls", 3, 3, id="urls_limited"),
    ],
)
async def test_max_items_bounds_the_result(
    repository: SQLiteURLStateRepository,
    db_path: Path,
    method: str,
    max_items: int,
    expected: int,
) -> None:
    """`-1` means no limit, and every other value bounds the batch."""
    targets = seed_due_rows(db_path, 4, state=CrawlState.FINISHED_CRAWL.value)
    rows = await read_or_claim(repository, method, targets, NOW, max_items)
    assert rows == targets[:expected]


@pytest.mark.parametrize(
    ("claimers", "row_count"),
    [(2, 1), (2, 5), (4, 9)],
)
async def test_concurrent_claims_never_double_claim(
    db_path: Path,
    new_repository: Callable[[], SQLiteURLStateRepository],
    claimers: int,
    row_count: int,
) -> None:
    """Separate connections race for the rows and divide them exactly once."""
    stores = [new_repository() for _ in range(claimers)]
    for store in stores:
        await store.initialize()
    targets = [url(index) for index in range(row_count)]
    await stores[0].create_urls(targets)
    results = await asyncio.gather(
        *(
            store.claim_candidates(
                NOW, -1, job_timeout=JOB_TIMEOUT, queue_timeout=QUEUE_TIMEOUT
            )
            for store in stores
        )
    )
    claimed = [item for batch in results for item in batch]
    assert len(claimed) == row_count
    assert sorted(item.get_url() for item in claimed) == sorted(
        target.get_url() for target in targets
    )
    for row in read_rows(db_path):
        assert row["state"] == CrawlState.QUEUED.value
        assert row["last_status_update_time"] == STAMP


@pytest.mark.parametrize(
    "failing_statement", ["discovered_insert", "second_chunk"]
)
async def test_a_failed_transaction_writes_nothing(
    repository: SQLiteURLStateRepository,
    db_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failing_statement: str,
) -> None:
    """A failure after a successful statement still discards that statement."""
    if failing_statement == "discovered_insert":
        targets = [url(0), url(1)]
        await repository.create_urls(targets)
        for target in targets:
            await repository.mark_started(target, NOW)
        add_trigger(db_path, "reject_insert", "BEFORE INSERT ON urls")
        with pytest.raises(sqlite3.IntegrityError):
            await repository.complete_crawl(
                [(target, None) for target in targets], [url(2)], NOW
            )
        rows = read_rows(db_path)
        if failing_statement == "discovered_insert":
            # The completion was rolled back, so the discovered row is absent and
            # the two targets are exactly as mark_started left them.
            assert len(rows) == len(targets)
            assert all(row["last_crawl_time"] == STAMP for row in rows)
            for row in rows:
                assert row["state"] == CrawlState.STARTED_CRAWL.value
        else:
            assert len(rows) == len(targets)
            assert all(row["last_crawl_time"] is None for row in rows)
            for row in rows:
                assert row["state"] == CrawlState.NOT_CRAWLED.value
                assert row["last_status_update_time"] == STAMP
    else:
        monkeypatch.setattr(models, "MAX_BOUND_PARAMETERS", 6)
        targets = [url(index) for index in range(6)]
        await repository.create_urls(targets)
        add_trigger(
            db_path,
            "reject_one",
            "BEFORE UPDATE OF state ON urls",
            f"WHEN NEW.custom_url = '{targets[4].get_url()}'",
        )
        with pytest.raises(sqlite3.IntegrityError):
            await repository.claim_urls(
                targets, NOW, -1, job_timeout=JOB_TIMEOUT, queue_timeout=QUEUE_TIMEOUT
            )
        rows = read_rows(db_path)
        # The first chunk's claims were rolled back with the second chunk's failure.
        assert len(rows) == len(targets)
        assert all(row["last_crawl_time"] is None for row in rows)
        for row in rows:
            assert row["state"] == CrawlState.NOT_CRAWLED.value
            assert row["last_status_update_time"] == STAMP


@pytest.mark.parametrize(
    ("failing", "expect_rollback"),
    [
        pytest.param(models.BEGIN_IMMEDIATE_SQL, False, id="begin_never_opened"),
        pytest.param(models.COMMIT_SQL, True, id="commit_needs_rollback"),
    ],
)
async def test_a_failed_edge_of_a_transaction_is_undone_correctly(
    spied_repository: tuple[SQLiteURLStateRepository, RecordingConnection],
    db_path: Path,
    failing: str,
    expect_rollback: bool,
) -> None:
    """A failed COMMIT is rolled back; a failed BEGIN has nothing to undo."""
    store, spy = spied_repository
    spy.fail_statement(failing, sqlite3.ProgrammingError("injected"))
    with pytest.raises(sqlite3.ProgrammingError):
        await store.create_urls([url(0)])
    assert (models.ROLLBACK_SQL in spy.statements) is expect_rollback
    assert read_rows(db_path) == []


@pytest.mark.parametrize(
    ("requested", "expected"),
    [
        pytest.param(
            ["missing", "p0", "p2", "p1"], ["p0", "p1", "p2"], id="mixed"
        ),
        pytest.param(["p0", "p1", "p2"], ["p0", "p1", "p2"], id="all"),
        pytest.param(["p1", "p0"], ["p0", "p1"], id="reordered"),
    ],
)
async def test_claim_urls_restricts_before_the_limit(
    repository: SQLiteURLStateRepository,
    db_path: Path,
    requested: list[str],
    expected: list[str],
) -> None:
    """A URL that is not a row must not consume a slot in the batch."""
    for index in range(3):
        seed(
            db_path,
            url(100 + index).get_url(),
            CrawlState.FINISHED_CRAWL.value,
            next_crawl_time=NOW - timedelta(seconds=300 - index),
        )
    seed_due_rows(db_path, 3, state=CrawlState.FINISHED_CRAWL.value)
    wanted = [CustomURL(f"http://crawlme.monzo.com/{name}") for name in requested]
    claimed = await repository.claim_urls(
        wanted, NOW, -1, job_timeout=JOB_TIMEOUT, queue_timeout=QUEUE_TIMEOUT
    )
    assert claimed == [
        CustomURL(f"http://crawlme.monzo.com/{name}") for name in expected
    ]


@pytest.mark.parametrize(
    ("pose", "expected"),
    [
        pytest.param("missing", [], id="never_seen"),
        pytest.param("freshly_queued", [], id="queued_a_moment_ago"),
        pytest.param("freshly_started", [], id="started_a_moment_ago"),
        pytest.param("not_yet_due", [], id="finished_but_not_due"),
        pytest.param("missing_and_eligible", ["p0"], id="one_of_two"),
    ],
)
async def test_claim_urls_returns_nothing_for_a_row_it_may_not_take(
    repository: SQLiteURLStateRepository,
    db_path: Path,
    pose: str,
    expected: list[str],
) -> None:
    """A claim hands back only what it moved, never what the caller merely asked."""
    if pose == "missing":
        seed_due_rows(db_path, 1, state=CrawlState.FINISHED_CRAWL.value)
        wanted = [CustomURL("http://crawlme.monzo.com/never-seen")]
    elif pose == "freshly_queued":
        target = url(0)
        await repository.create_urls([target])
        await read_or_claim(repository, "claim_candidates", [target], NOW, -1)
        wanted = [target]
    elif pose == "freshly_started":
        target = url(0)
        await repository.create_urls([target])
        await repository.mark_started(target, NOW)
        wanted = [target]
    elif pose == "not_yet_due":
        target = url(0)
        seed(
            db_path,
            target.get_url(),
            CrawlState.FINISHED_CRAWL.value,
            next_crawl_time=NOW + timedelta(hours=1),
        )
        wanted = [target]
    else:
        seed_due_rows(db_path, 1, state=CrawlState.FINISHED_CRAWL.value)
        wanted = [CustomURL("http://crawlme.monzo.com/never-seen"), url(0)]
    claimed = await repository.claim_urls(
        wanted, NOW, -1, job_timeout=JOB_TIMEOUT, queue_timeout=QUEUE_TIMEOUT
    )
    assert claimed == [
        CustomURL(f"http://crawlme.monzo.com/{name}") for name in expected
    ]


@pytest.mark.parametrize("max_items", [-1, 0, 5])
async def test_claim_urls_with_an_empty_list_issues_no_statement(
    spied_repository: tuple[SQLiteURLStateRepository, RecordingConnection],
    max_items: int,
) -> None:
    """The no-links case short-circuits, because `IN ()` is not valid SQL."""
    store, spy = spied_repository
    claimed = await store.claim_urls(
        [], NOW, max_items, job_timeout=JOB_TIMEOUT, queue_timeout=QUEUE_TIMEOUT
    )
    assert claimed == []
    assert spy.statements == []


@pytest.mark.parametrize(
    ("max_items", "expected_count", "expected_statements"),
    [
        pytest.param(-1, 6, 2, id="unlimited_needs_both_chunks"),
        pytest.param(6, 6, 2, id="exactly_all_needs_both_chunks"),
        pytest.param(5, 5, 2, id="one_short_takes_two_chunks"),
        pytest.param(4, 4, 2, id="mid_batch_takes_two_chunks"),
        pytest.param(3, 3, 1, id="one_chunk_is_enough"),
        pytest.param(1, 1, 1, id="single_row_stops_the_loop"),
        pytest.param(0, 0, 0, id="nothing_owed_runs_no_statement"),
    ],
)
async def test_a_chunked_claim_never_returns_more_than_max_items(
    spied_repository: tuple[SQLiteURLStateRepository, RecordingConnection],
    db_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    max_items: int,
    expected_count: int,
    expected_statements: int,
) -> None:
    """Each chunk is limited to what is still owed, so the total is exact."""
    store, spy = spied_repository
    monkeypatch.setattr(models, "MAX_BOUND_PARAMETERS", 6)
    targets = seed_due_rows(db_path, 6, state=CrawlState.FINISHED_CRAWL.value)
    claimed = await store.claim_urls(
        targets, NOW, max_items, job_timeout=JOB_TIMEOUT, queue_timeout=QUEUE_TIMEOUT
    )
    assert claimed == targets[:expected_count]
    updates = [
        statement
        for statement in spy.statements
        if statement.lstrip().startswith("UPDATE urls")
    ]
    assert len(updates) == expected_statements


@pytest.mark.parametrize("completion_delta", [0, 3600])
async def test_complete_crawl_writes_a_mixed_batch_in_one_transaction(
    repository: SQLiteURLStateRepository,
    db_path: Path,
    completion_delta: int,
) -> None:
    """A null, an overdue, and a future schedule all land together."""
    now = NOW + timedelta(seconds=completion_delta)
    targets = [url(index) for index in range(4)]
    await repository.create_urls(targets)
    for target in targets:
        await repository.mark_started(target, NOW)
    await repository.complete_crawl(
        [
            (targets[0], None),
            (targets[1], now - timedelta(seconds=1)),
            (targets[2], now + timedelta(seconds=60)),
            (targets[3], None),
        ],
        [],
        now,
    )
    expected_next = {
        targets[0].get_url(): None,
        targets[1].get_url(): stamp(now - timedelta(seconds=1)),
        targets[2].get_url(): stamp(now + timedelta(seconds=60)),
        targets[3].get_url(): None,
    }
    rows = read_rows(db_path)
    assert len(rows) == 4
    for row in rows:
        assert row["state"] == CrawlState.FINISHED_CRAWL.value
        assert row["next_crawl_time"] == expected_next[row["custom_url"]]
        assert row["last_status_update_time"] == stamp(now)
        assert row["last_crawl_time"] == STAMP


@pytest.mark.parametrize("discovered", [[], [1], [1, 2]])
async def test_complete_crawl_inserts_discovered_urls_as_not_crawled(
    repository: SQLiteURLStateRepository,
    db_path: Path,
    discovered: list[int],
) -> None:
    """A discovered URL becomes a fresh row with the two times equal."""
    target = url(0)
    await repository.create_urls([target])
    await repository.mark_started(target, NOW)
    await repository.complete_crawl(
        [(target, None)], [url(index) for index in discovered], NOW
    )
    rows = {row["custom_url"]: row for row in read_rows(db_path)}
    assert len(rows) == 1 + len(discovered)
    for index in discovered:
        row = rows[url(index).get_url()]
        assert row["state"] == CrawlState.NOT_CRAWLED.value
        assert row["created_time"] == row["next_crawl_time"] == STAMP
        assert row["last_crawl_time"] is None


@pytest.mark.parametrize(
    "prior",
    [
        CrawlState.NOT_CRAWLED.value,
        CrawlState.QUEUED.value,
        CrawlState.STARTED_CRAWL.value,
        CrawlState.FINISHED_CRAWL.value,
    ],
)
async def test_complete_crawl_leaves_an_existing_row_untouched(
    repository: SQLiteURLStateRepository,
    db_path: Path,
    prior: str,
) -> None:
    """Re-discovering a known URL must not reset its state or its schedule."""
    known = url(1)
    seed(
        db_path,
        known.get_url(),
        prior,
        next_crawl_time=NOW + timedelta(days=1),
        last_status_update_time=NOW - timedelta(days=1),
    )
    before = next(
        row for row in read_rows(db_path) if row["custom_url"] == known.get_url()
    )
    target = url(0)
    await repository.create_urls([target])
    await repository.mark_started(target, NOW)
    await repository.complete_crawl([(target, None)], [known], NOW)
    after = next(
        row for row in read_rows(db_path) if row["custom_url"] == known.get_url()
    )
    assert tuple(after) == tuple(before)


@pytest.mark.parametrize("entry_point", ["create_urls", "complete_crawl"])
async def test_empty_batches_issue_no_statement(
    spied_repository: tuple[SQLiteURLStateRepository, RecordingConnection],
    entry_point: str,
) -> None:
    """An empty list is a no-op, because the no-links case is the common one."""
    store, spy = spied_repository
    if entry_point == "create_urls":
        await store.create_urls([])
    else:
        await store.complete_crawl([], [], NOW)
    assert spy.statements == []


@pytest.mark.parametrize(
    ("column", "expected_offsets"),
    [
        pytest.param("created_time", (0, 0), id="created"),
        pytest.param("next_crawl_time", (60, 0), id="next"),
        pytest.param("last_status_update_time", (0, 0), id="status"),
        pytest.param("last_crawl_time", (0, None), id="last_attempt"),
    ],
)
async def test_stored_timestamps_round_trip_through_strftime(
    repository: SQLiteURLStateRepository,
    db_path: Path,
    column: str,
    expected_offsets: tuple[int | None, ...],
) -> None:
    """Every stored value converts to an epoch, so no row is silently skipped."""
    target = url(0)
    await repository.create_urls([target])
    await repository.mark_started(target, NOW)
    await repository.complete_crawl(
        [(target, NOW + timedelta(minutes=1))], [url(1)], NOW
    )
    base = int(NOW.timestamp())
    assert epoch_of(db_path, column) == [
        None if offset is None else str(base + offset) for offset in expected_offsets
    ]


@pytest.mark.parametrize(
    ("failures", "expected_calls", "expected_attempts", "expected_rows"),
    [
        pytest.param(0, 0, 0, 1, id="nothing_faulty_never_reaches_the_policy"),
        pytest.param(1, 1, 1, 1, id="one_transient_error_is_absorbed"),
        pytest.param(2, 1, 2, 1, id="two_transient_errors_are_absorbed"),
        pytest.param(9, 1, 3, 0, id="exhaustion_reraises_and_writes_nothing"),
    ],
)
async def test_a_transient_sqlite_error_goes_to_the_retry_policy(
    spied_repository: tuple[SQLiteURLStateRepository, RecordingConnection],
    db_path: Path,
    retry_policy: CountingRetryPolicy,
    failures: int,
    expected_calls: int,
    expected_attempts: int,
    expected_rows: int,
) -> None:
    """`sqlite3.OperationalError` is retried, and the policy's budget decides."""
    store, spy = spied_repository
    retry_policy.max_attempts = 3
    spy.fail_next(failures, sqlite3.OperationalError("database is locked"))
    if expected_rows == 0:
        with pytest.raises(sqlite3.OperationalError):
            await store.create_urls([url(0)])
    else:
        await store.create_urls([url(0)])
        assert retry_policy.calls == expected_calls
        assert retry_policy.attempts == expected_attempts
        assert len(read_rows(db_path)) == expected_rows


@pytest.mark.parametrize(
    "error",
    [
        sqlite3.ProgrammingError("Incorrect number of bindings"),
        sqlite3.IntegrityError("UNIQUE constraint failed"),
        sqlite3.InterfaceError("closed database"),
    ],
)
async def test_a_sqlite_error_outside_the_transient_class_is_not_retried(
    spied_repository: tuple[SQLiteURLStateRepository, RecordingConnection],
    retry_policy: CountingRetryPolicy,
    error: sqlite3.Error,
) -> None:
    """A bug in the caller must fail fast, without spending a backoff delay."""
    store, spy = spied_repository
    spy.fail_next(1, error)
    with pytest.raises(type(error)):
        await store.create_urls([url(0)])
    assert retry_policy.calls == 0


@pytest.mark.parametrize("close_calls", [1, 2, 3])
async def test_close_is_idempotent_and_joins_the_worker_thread(
    spied_repository: tuple[SQLiteURLStateRepository, RecordingConnection],
    close_calls: int,
) -> None:
    """Closing ends the non-daemon thread, however many times it is called."""
    store, spy = spied_repository
    thread = spy.worker_thread
    assert thread.is_alive()
    for _ in range(close_calls):
        await store.close()
        assert not thread.is_alive()


@pytest.mark.parametrize("close_calls", [1, 2])
async def test_a_closed_repository_refuses_further_work(
    spied_repository: tuple[SQLiteURLStateRepository, RecordingConnection],
    close_calls: int,
) -> None:
    """After `close`, the store says so instead of failing deeper down."""
    store, _ = spied_repository
    for _ in range(close_calls):
        await store.close()
    with pytest.raises(RuntimeError):
        await store.create_urls([url(0)])


@pytest.mark.parametrize(
    ("level", "operation", "fragment"),
    [
        pytest.param(logging.INFO, "initialize", "schema", id="schema_at_info"),
        pytest.param(
            logging.DEBUG, "claim_candidates", "claimed", id="claim_at_debug"
        ),
        pytest.param(logging.DEBUG, "claim_urls", "claimed", id="urls_claim_at_debug"),
    ],
)
async def test_the_store_logs_through_the_injected_logger(
    caplog: pytest.LogCaptureFixture,
    db_path: Path,
    retry_policy: CountingRetryPolicy,
    time_provider: FixedTimeProvider,
    logger: logging.Logger,
    level: int,
    operation: str,
    fragment: str,
) -> None:
    """The schema is INFO and each claim batch is DEBUG, on the injected logger."""
    store = build(db_path, retry_policy, time_provider, logger)
    try:
        if operation != "initialize":
            await store.initialize()
            await store.create_urls([url(0)])
        with caplog.at_level(logging.DEBUG, logger=LOGGER_NAME):
            if operation == "initialize":
                await store.initialize()
            elif operation == "claim_candidates":
                await read_or_claim(store, "claim_candidates", [], NOW, -1)
            else:
                await read_or_claim(store, "claim_urls", [url(0)], NOW, -1)
    finally:
        await store.close()
    matching = [
        record
        for record in caplog.records
        if record.levelno == level and fragment in record.getMessage()
    ]
    assert matching
    assert all(record.name == LOGGER_NAME for record in matching)


@pytest.mark.parametrize("entry_point", ["create_urls", "complete_crawl"])
async def test_a_new_row_starts_with_times_crawled_zero(
    repository: SQLiteURLStateRepository,
    db_path: Path,
    entry_point: str,
) -> None:
    """Every path that creates a row leaves the crawl counter at zero."""
    created = url(1)
    if entry_point == "create_urls":
        await repository.create_urls([created])
    else:
        await repository.complete_crawl([(url(0), None)], [created], NOW)
    row = next(
        row for row in read_rows(db_path) if row["custom_url"] == created.get_url()
    )
    assert row["times_crawled"] == 0


@pytest.mark.parametrize("next_crawl_time", [None, NOW + timedelta(minutes=1)])
async def test_complete_crawl_increments_times_crawled_for_each_finished_row(
    repository: SQLiteURLStateRepository,
    db_path: Path,
    next_crawl_time: datetime | None,
) -> None:
    """One completed outcome is one more crawl recorded for that URL."""
    target = url(0)
    await repository.create_urls([target])
    await repository.mark_started(target, NOW)
    await repository.complete_crawl([(target, next_crawl_time)], [], NOW)
    row = next(
        row for row in read_rows(db_path) if row["custom_url"] == target.get_url()
    )
    assert row["times_crawled"] == 1


@pytest.mark.parametrize("crawl_count", [1, 2, 3])
async def test_times_crawled_accumulates_across_crawls(
    repository: SQLiteURLStateRepository,
    db_path: Path,
    crawl_count: int,
) -> None:
    """The counter is a sum, not a flag, so re-crawls are visible in the row."""
    target = url(0)
    await repository.create_urls([target])
    for _ in range(crawl_count):
        await repository.mark_started(target, NOW)
        await repository.complete_crawl([(target, None)], [], NOW)
    row = next(
        row for row in read_rows(db_path) if row["custom_url"] == target.get_url()
    )
    assert row["times_crawled"] == crawl_count


@pytest.mark.parametrize("dummy", [None])
async def test_mark_started_does_not_increment_times_crawled(
    dummy: None,
    repository: SQLiteURLStateRepository,
    db_path: Path,
) -> None:
    """Starting a crawl is not completing one, so the counter waits."""
    target = url(0)
    await repository.create_urls([target])
    await repository.mark_started(target, NOW)
    row = next(
        row for row in read_rows(db_path) if row["custom_url"] == target.get_url()
    )
    assert row["times_crawled"] == 0


@pytest.mark.parametrize("dummy", [None])
async def test_a_discovered_row_is_not_incremented(
    dummy: None,
    repository: SQLiteURLStateRepository,
    db_path: Path,
) -> None:
    """Discovering a link is not crawling it, so its counter stays zero."""
    target, discovered = url(0), url(1)
    await repository.create_urls([target])
    await repository.mark_started(target, NOW)
    await repository.complete_crawl([(target, None)], [discovered], NOW)
    rows = {row["custom_url"]: row["times_crawled"] for row in read_rows(db_path)}
    assert rows[target.get_url()] == 1
    assert rows[discovered.get_url()] == 0


@pytest.mark.parametrize("seeded_rows", [1, 2])
async def test_a_database_made_before_the_column_is_migrated(
    db_path: Path,
    retry_policy: CountingRetryPolicy,
    time_provider: FixedTimeProvider,
    logger: logging.Logger,
    seeded_rows: int,
) -> None:
    """An old database gains the counter without losing a row or a value."""
    old_table_sql = """
    CREATE TABLE urls (
    custom_url TEXT NOT NULL PRIMARY KEY,
    created_time TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_crawl_time TEXT,
    next_crawl_time TEXT DEFAULT CURRENT_TIMESTAMP,
    state TEXT NOT NULL DEFAULT 'not_crawled',
    last_status_update_time TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
    """
    with contextlib.closing(sqlite3.connect(db_path)) as connection:
        connection.execute(old_table_sql)
        for index in range(seeded_rows):
            connection.execute(
                "INSERT INTO urls (custom_url) VALUES (?)",
                (url(index).get_url(),),
            )
        connection.commit()
    store = build(db_path, retry_policy, time_provider, logger)
    try:
        await store.initialize()
        migrated = next(
            row
            for row in read_rows(db_path)
            if row["custom_url"] == url(0).get_url()
        )
        assert migrated["times_crawled"] == 0
        target = url(0)
        await store.mark_started(target, NOW)
        await store.complete_crawl([(target, None)], [], NOW)
        crawled = next(
            row for row in read_rows(db_path) if row["custom_url"] == target.get_url()
        )
        assert crawled["times_crawled"] == 1
        assert len(read_rows(db_path)) == seeded_rows
    finally:
        await store.close()
