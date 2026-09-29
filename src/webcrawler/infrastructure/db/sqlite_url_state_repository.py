"""The default URL state store: one SQLite file driven by aiosqlite.

An interface with one simple default implementation that nothing bypasses,
so this is the only way any other module touches the crawl state. It performs
I/O, so it owns retry, backoff, and jitter.
"""

import asyncio
import contextlib
import logging
import sqlite3
from collections.abc import Awaitable, Callable, Sequence
from datetime import datetime, timedelta
from pathlib import Path
from typing import Final, TypeVar

import aiosqlite

from webcrawler.domain.crawl_state import CrawlState
from webcrawler.domain.custom_url import CustomURL
from webcrawler.infrastructure.db import models
from webcrawler.ports.retry_policy import RetryPolicy
from webcrawler.ports.time_provider import TimeProviderFactory
from webcrawler.ports.url_state_repository import URLStateRepository

T = TypeVar("T")

Parameters = dict[str, object]

# `:started_state` and `:now` are bound by every chunk, so only the remainder of
# the parameter budget is available for the `:uN` URL placeholders.
MARK_STARTED_RESERVED_PARAMETERS: Final = 2

# The crawl-queue's statement widened to the caller's `IN (...)` list, so the
# columns and the stored values still come from one place. Every chunk fills
# `{url_placeholders}` and reuses the same `:started_state` and `:now`.
MARK_STARTED_BATCH_SQL: Final = models.MARK_STARTED_SQL.replace(
    "WHERE custom_url = :custom_url", "WHERE custom_url IN ({url_placeholders})"
)

MAX_MARK_STARTED_URLS: Final = max(
    1, models.MAX_BOUND_PARAMETERS - MARK_STARTED_RESERVED_PARAMETERS
)


def _timeout_seconds(timeout: timedelta) -> int:
    """Encode one timeout the way the shared predicate compares it.

    Staleness uses `strftime('%s', ...)` arithmetic, so a `timedelta` bound directly would compare as text.

    Args:
        timeout: How long the caller allows before a row is reclaimable.

    Returns:
        int: The timeout in whole seconds.
    """
    # The read and the claim both encode here, so a timeout cannot mean two things.
    return int(timeout.total_seconds())

# The one class allowed to implement a port by inheritance, and nothing extends
# it, which is what keeps the state machine substitutable.
class SQLiteURLStateRepository(URLStateRepository):
    """Crawl state for every URL, held in one SQLite file.

    Every statement text is assembled in `models.py`; this class owns the transactions, the retry, the connection's lock, and the worker thread `aiosqlite` runs the connection on.

    `initialize` asserts SQLite 3.35 or newer, because the claim query is an `UPDATE ... RETURNING` inside one `BEGIN IMMEDIATE`.

    Args:
        db_path: The database file, or `":memory:"`. Its parent directory must already exist, because one process owns the whole crawler and so only ever opens the file itself.
        retry_policy: Applied to every unit of work, and the only holder of the backoff, jitter, and per-attempt timeout this store needs.
        time_provider: The only source of "now" for a write, so `created_time == next_crawl_time` is the application's guarantee and never a coincidence of two clocks.
    """

    def __init__(
        self,
        db_path: str | Path,
        retry_policy: RetryPolicy,
        time_provider: TimeProviderFactory,
    ) -> None:
        """Open a connection to the store. The schema is `initialize`'s job.

        Args:
            db_path: The database file, or `":memory:"`.
            retry_policy: Applied to every unit of work.
            time_provider: The only source of "now" for a write.
        """
        self._logger = logging.getLogger(__name__)
        self._db_path = db_path
        self._retry_policy = retry_policy
        self._time_provider = time_provider
        self._transaction_lock = asyncio.Lock()
        # isolation_level=None disables the driver's implicit transaction, so
        # the explicit BEGIN IMMEDIATE is not wrapped as nested.
        self._connection = aiosqlite.connect(db_path, isolation_level=None)
        self._started = False
        self._closed = False

    async def initialize(self) -> None:
        """Create the table and the composite index when they are absent.

        Idempotent, so it is safe on every start, and it is the first thing the composition root awaits.

        Raises:
            sqlite3.Error: If the linked SQLite is older than 3.35 and cannot run `UPDATE ... RETURNING`, or the schema cannot be created.
        """
        models.require_returning_support()
        await self._open()
        await self._transaction(self._create_schema)

    async def create_urls(self, urls: list[CustomURL]) -> None:
        """Insert the given URLs, ignoring any that already exist.

        An insert is the only way a row becomes `not_crawled`, and it sets `created_time` and `next_crawl_time` to the same injected instant, so a fresh row is immediately claimable. An empty list is a no-op that issues no statement.

        Args:
            urls: The canonical URLs to make known to the crawler.

        Raises:
            sqlite3.Error: If the batch cannot be committed, in which case no row is inserted.
        """
        if not urls:
            return
        await self._open()
        now = self._time_provider.now()
        rows: list[Parameters] = [
            {
                "custom_url": url.get_url(),
                "created_time": now,
                "next_crawl_time": now,
                "fresh_state": CrawlState.NOT_CRAWLED.value,
                "status_time": now,
            }
            for url in urls
        ]
        await self._transaction(
            lambda: self._run_many(models.INSERT_URL_SQL, rows)
        )

    async def get_crawlable_urls(
        self,
        now: datetime,
        max_items: int,
        *,
        job_timeout: timedelta,
        queue_timeout: timedelta,
    ) -> list[CustomURL]:
        """Read the rows a claim would select, changing nothing.

        The timeouts are bound, not defaulted, because an in-flight row is eligible only once its own timeout has elapsed, and the statement and its parameters are built by the same helpers the claim uses, so the two cannot drift apart.

        Args:
            now: The instant the predicates are evaluated against, in UTC.
            max_items: The row limit. `-1` means no limit.
            job_timeout: How long a `started_crawl` row may stay untouched before it is treated as abandoned and reclaimable.
            queue_timeout: How long a `queued` row may wait before it is reported as reclaimable.

        Returns:
            list[CustomURL]: Up to `max_items` eligible URLs, earliest `next_crawl_time` first.

        Raises:
            sqlite3.Error: If the query cannot be executed.
        """
        await self._open()
        return await self._execute(
            lambda: self._select_crawlable(
                now, max_items, job_timeout, queue_timeout
            )
        )

    async def claim_candidates(
        self,
        now: datetime,
        max_items: int,
        *,
        job_timeout: timedelta,
        queue_timeout: timedelta,
    ) -> list[CustomURL]:
        """Atomically move eligible rows to `queued` and return them.

        No lock guards the state transition: the predicate re-targets a `queued` or `started_crawl` row only once it is older than its timeout, and `BEGIN IMMEDIATE` keeps another writer out of the select-then-update pair.

        Args:
            now: The instant the predicates are evaluated against, in UTC.
            max_items: The row limit. `-1` means no limit.
            job_timeout: How long a `started_crawl` row may stay untouched before it is treated as abandoned and reclaimed.
            queue_timeout: How long a `queued` row may wait before it is re-claimed, which is how a lost queue send is recovered.

        Returns:
            list[CustomURL]: Only the rows this call moved to `queued`, so the caller never enqueues a row somebody else already owns.

        Raises:
            sqlite3.Error: If the transaction cannot be committed, in which case it is rolled back and no row is claimed.
        """
        return await self._claim(None, now, max_items, job_timeout, queue_timeout)

    async def claim_urls(
        self,
        urls: list[CustomURL],
        now: datetime,
        max_items: int = -1,
        *,
        job_timeout: timedelta,
        queue_timeout: timedelta,
    ) -> list[CustomURL]:
        """As `claim_candidates`, restricted to the caller's own URLs.

        The `custom_url IN (...)` restriction is applied inside the claiming subquery, ahead of the `LIMIT`, so a batch is never under-filled with rows the caller did not ask for. An empty list issues no statement at all, because the no-links case is the common one and `IN ()` is rejected outright by some engines.

        Args:
            urls: The URLs the caller wants queued, expected to exist already, which is why this claims and never inserts.
            now: The instant the predicates are evaluated against, in UTC.
            max_items: The row limit. `-1` means no limit, and is the default because the caller's list is already the batch.
            job_timeout: How long a `started_crawl` row may stay untouched before it is treated as abandoned and reclaimed.
            queue_timeout: How long a `queued` row may wait before it is re-claimed.

        Returns:
            list[CustomURL]: The subset of `urls` this call moved to `queued`, empty for a missing or a freshly queued URL.

        Raises:
            sqlite3.Error: If the transaction cannot be committed, in which case it is rolled back and no row is claimed.
        """
        if not urls:
            return []
        return await self._claim(urls, now, max_items, job_timeout, queue_timeout)

    async def mark_started(self, urls: list[CustomURL], now: datetime) -> None:
        """Record that workers have begun crawling the given URLs.

        This is the only writer of `last_crawl_time`, deliberately overriding the older plan that put it in the completion transaction, because it records the attempt rather than the outcome. The rows stay claimable while the attempts run: a worker that dies before writing a completion leaves them `started_crawl`, and the `job_timeout` branch reclaims them. One `UPDATE ... WHERE custom_url IN (...)` covers a whole batch, and an empty list is a no-op that issues no statement, because `IN ()` is rejected outright by some engines.

        Args:
            urls: The URLs being crawled, all of which the store already holds.
            now: The attempt time, in UTC.

        Raises:
            sqlite3.Error: If the update cannot be committed, in which case the rows are not marked and stay claimable.
        """
        if not urls:
            return
        await self._open()
        await self._transaction(lambda: self._mark_started_chunks(urls, now))

    async def _mark_started_chunks(
        self, urls: Sequence[CustomURL], now: datetime
    ) -> None:
        """Mark every chunk of the caller's list inside the open transaction.

        All chunks share one `BEGIN IMMEDIATE` ... `COMMIT`, so a chunked batch is as atomic as a single-statement one.

        Args:
            urls: The caller's URLs, non-empty.
            now: The attempt time, in UTC, bound to every chunk.
        """
        for chunk in self._mark_started_chunks_of(urls):
            await self._run(*self._mark_started_statement(chunk, now))
        self._logger.debug(
            "marked %d url(s) started at %s",
            len(urls),
            now.isoformat(),
        )

    def _mark_started_chunks_of(
        self, urls: Sequence[CustomURL]
    ) -> list[Sequence[CustomURL]]:
        """Split the caller's URLs into statement-sized chunks.

        One statement cannot bind more placeholders than SQLite allows parameters, so a long caller list becomes several statements, the same way the claim chunks its list.

        Args:
            urls: The caller's URLs, non-empty.

        Returns:
            list[Sequence[CustomURL]]: One entry per statement to issue.
        """
        return [
            urls[start : start + MAX_MARK_STARTED_URLS]
            for start in range(0, len(urls), MAX_MARK_STARTED_URLS)
        ]

    def _mark_started_statement(
        self, chunk: Sequence[CustomURL], now: datetime
    ) -> tuple[str, Parameters]:
        """Bind one batch mark-started statement.

        Args:
            chunk: The URLs this statement may mark, non-empty.
            now: The attempt time, in UTC.

        Returns:
            tuple[str, Parameters]: The statement text, carrying one `:uN` per URL, and the values to bind by name.
        """
        names = models.url_parameter_names(chunk)
        statement = MARK_STARTED_BATCH_SQL.format(
            url_placeholders=", ".join(f":{name}" for name in names)
        )
        parameters: Parameters = {
            "started_state": CrawlState.STARTED_CRAWL.value,
            "now": now,
        }
        for name, url in zip(names, chunk):
            parameters[name] = url.get_url()
        return statement, parameters

    async def complete_crawl(
        self,
        finished: list[tuple[CustomURL, datetime | None]],
        discovered: list[CustomURL],
        now: datetime,
    ) -> None:
        """Write one batch of outcomes and one batch of discoveries atomically.

        One bulk transaction, so a batch either lands whole or not at all, which is what lets the worker treat a failure here as an abort rather than a partial success. Two empty lists are a no-op that issues no statement.

        Args:
            finished: One `(url, next_crawl_time)` pair per attempted URL. A `None` time stores NULL and schedules no re-crawl, and every row in the call also gets `last_status_update_time = now`.
            discovered: URLs found on the crawled pages, inserted when absent with `created_time = next_crawl_time`; a row that already exists is left untouched, so a re-crawl never resets another's schedule.
            now: The completion time, in UTC.

        Raises:
            sqlite3.Error: If the transaction cannot be committed, in which case it is rolled back, no outcome is recorded, and the rows stay `started_crawl` for the `job_timeout` branch to reclaim.
        """
        if not finished and not discovered:
            return
        await self._open()
        await self._transaction(
            lambda: self._write_outcomes(finished, discovered, now)
        )

    async def close(self) -> None:
        """Close the connection and join the worker thread it owns.

        Idempotent, so the orchestrator can call it from its shutdown path without knowing whether anything already closed the store. aiosqlite runs each connection on a non-daemon thread that blocks until the connection closes, and interpreter shutdown joins non-daemon threads before finalization, so an unclosed connection hangs both `Ctrl+C` and pytest session teardown.

        Raises:
            sqlite3.Error: If closing the connection fails.
        """
        if self._closed:
            return
        self._closed = True
        await self._connection.close()
        # aiosqlite stops its worker loop but joins nothing, and `_thread` is
        # the only handle on it, so the join is done here.
        if self._started:
            self._connection._thread.join()

    async def _open(self) -> None:
        """Start the connection's worker thread once, on first use.

        `aiosqlite.connect` returns a proxy whose thread starts on the first await, so the synchronous constructor cannot open it, and awaiting it twice would try to start the thread twice.

        Raises:
            RuntimeError: If the store has already been closed.
        """
        if self._closed:
            raise RuntimeError("the url state repository is closed")
        if self._started:
            return
        self._started = True
        await self._connection

    async def _transaction(self, body: Callable[[], Awaitable[T]]) -> T:
        """Run body inside one `BEGIN IMMEDIATE` ... `COMMIT`, rolling back on error.

        The lock is held for the whole transaction because aiosqlite serialises statements but not transaction spans, and a caller of this class issues several transactions at once: the worker's task group marks every URL of a batch. An interleaved `BEGIN` would abort the batch.

        Args:
            body: The statements to run. It must not commit or roll back itself, because this owns both ends of the transaction.

        Returns:
            T: Whatever body returns.

        Raises:
            sqlite3.Error: Re-raised after the rollback, so the caller sees the real cause and no part of the batch survives.
        """

        async def _attempt() -> T:
            """One whole attempt, so the policy can retry it from scratch."""
            async with self._transaction_lock:
                begun = False
                try:
                    await self._run(models.BEGIN_IMMEDIATE_SQL)
                    begun = True
                    result = await body()
                    await self._run(models.COMMIT_SQL)
                    return result
                except BaseException:
                    # Only a BEGIN that actually succeeded may be undone: if the
                    # BEGIN itself failed, another transaction is in play here.
                    if begun:
                        with contextlib.suppress(sqlite3.Error):
                            await self._run(models.ROLLBACK_SQL)
                    raise

        return await self._execute(_attempt)

    async def _execute(self, operation: Callable[[], Awaitable[T]]) -> T:
        """Run one unit of work, retrying a transient failure through the policy.

        The first attempt runs here so that an error which is not the programmer's fault class, such as a programming or integrity error, fails without spending a backoff delay. Only `sqlite3.OperationalError`, the class that covers lock contention and a busy database, reaches the policy, and the policy re-raises once its attempts are spent.

        Args:
            operation: A callable returning a fresh awaitable per attempt, since the policy calls it more than once.

        Returns:
            T: The operation's result.

        Raises:
            sqlite3.Error: The last error, re-raised by the policy once its attempts are exhausted.
        """
        try:
            return await operation()
        except sqlite3.OperationalError as error:
            self._logger.debug("retrying a transient sqlite error: %s", error)
            return await self._retry_policy.execute(operation)

    async def _run(
        self, statement: str, parameters: Parameters | None = None
    ) -> None:
        """Run one statement and close its cursor.

        Args:
            statement: The SQL text, always from `models.py`.
            parameters: The values to bind by name, or None for a statement that takes none.
        """
        async with self._connection.execute(statement, parameters):
            return None

    async def _run_many(self, statement: str, rows: Sequence[Parameters]) -> None:
        """Run one statement over many rows in a single round trip.

        Args:
            statement: The SQL text, always from `models.py`.
            rows: One mapping of bound values per row, in insertion order.
        """
        async with self._connection.executemany(statement, rows):
            return None

    async def _create_schema(self) -> None:
        """Create the table and the composite index, then record the result."""
        for statement in models.SCHEMA_STATEMENTS:
            await self._run(statement)
        self._logger.debug(
            "url state schema created or already present at %s on SQLite %s",
            self._db_path,
            sqlite3.sqlite_version,
        )

    async def _select_crawlable(
        self,
        now: datetime,
        max_items: int,
        job_timeout: timedelta,
        queue_timeout: timedelta,
    ) -> list[CustomURL]:
        """Run the read-only select and map its rows.

        Args:
            now: The instant the predicates are evaluated against, in UTC.
            max_items: The row limit. `-1` means no limit.
            job_timeout: How long a `started_crawl` row may stay untouched before it is reported as reclaimable.
            queue_timeout: How long a `queued` row may wait before it is reported as reclaimable.

        Returns:
            list[CustomURL]: The eligible URLs, earliest `next_crawl_time` first.
        """
        parameters: Parameters = {
            "now": now,
            "max_items": max_items,
            "job_timeout": _timeout_seconds(job_timeout),
            "queue_timeout": _timeout_seconds(queue_timeout),
        }
        async with self._connection.execute(
            models.crawlable_select_statement(), parameters
        ) as cursor:
            rows = await cursor.fetchall()
            return [models.row_to_custom_url(row) for row in rows]

    async def _claim(
        self,
        urls: Sequence[CustomURL] | None,
        now: datetime,
        max_items: int,
        job_timeout: timedelta,
        queue_timeout: timedelta,
    ) -> list[CustomURL]:
        """Claim inside one transaction, chunking a caller's URL list if needed.

        Args:
            urls: The caller's URLs, or None to consider every row.
            now: The instant the predicates are evaluated against, in UTC.
            max_items: The row limit. `-1` means no limit.
            job_timeout: How long a `started_crawl` row may stay untouched before it is treated as abandoned and reclaimed.
            queue_timeout: How long a `queued` row may wait before it is re-claimed.

        Returns:
            list[CustomURL]: Only the rows this call moved to `queued`.

        Raises:
            sqlite3.Error: If the transaction cannot be committed, in which case it is rolled back and no row is claimed.
        """
        if urls is not None and not urls:
            return []
        await self._open()
        return await self._transaction(
            lambda: self._run_claim_chunks(
                urls, now, max_items, job_timeout, queue_timeout
            )
        )

    async def _run_claim_chunks(
        self,
        urls: Sequence[CustomURL] | None,
        now: datetime,
        max_items: int,
        job_timeout: timedelta,
        queue_timeout: timedelta,
    ) -> list[CustomURL]:
        """Claim every chunk of the caller's list inside the open transaction.

        All chunks share one `BEGIN IMMEDIATE` ... `COMMIT`, so a chunked claim is as atomic as a single-statement one. Each chunk binds the limit to what is still owed rather than to `max_items`, so chunking cannot push the claim past the caller's limit.

        Args:
            urls: The caller's URLs, or None to consider every row.
            now: The instant the predicates are evaluated against, in UTC.
            max_items: The row limit. `-1` means no limit.
            job_timeout: How long a `started_crawl` row may stay untouched before it is treated as abandoned and reclaimed.
            queue_timeout: How long a `queued` row may wait before it is re-claimed.

        Returns:
            list[CustomURL]: Only the rows these statements moved to `queued`.
        """
        claimed: list[CustomURL] = []
        for chunk in self._claim_chunks(urls):
            limit = (
                models.NO_LIMIT
                if max_items == models.NO_LIMIT
                else max_items - len(claimed)
            )
            if limit == 0:
                break
            parameters = self._claim_parameters(
                chunk, now, limit, job_timeout, queue_timeout
            )
            statement = models.claim_statement(
                None if chunk is None else models.url_parameter_names(chunk)
            )
            # The RETURNING rows are drained before the caller commits, since
            # SQLite discards an unstepped RETURNING's effect.
            async with self._connection.execute(statement, parameters) as cursor:
                rows = await cursor.fetchall()
            batch = [models.row_to_custom_url(row) for row in rows]
            claimed.extend(batch)
            if not batch:
                break
        self._logger.debug(
            "claimed %d url(s) at %s, %d in the caller's list",
            len(claimed),
            now.isoformat(),
            0 if urls is None else len(urls),
        )
        return claimed

    def _claim_chunks(
        self, urls: Sequence[CustomURL] | None
    ) -> list[Sequence[CustomURL] | None]:
        """Split the caller's URLs into statement-sized chunks.

        One statement cannot bind more placeholders than SQLite allows parameters, so a long caller list becomes several statements. A None entry stands for the single claim over every row, which binds no `IN (...)` list at all.

        Args:
            urls: The caller's URLs, or None for the claim over every row.

        Returns:
            list[Sequence[CustomURL] | None]: One entry per statement to issue.
        """
        if urls is None:
            return [None]
        size = max(
            1, models.MAX_BOUND_PARAMETERS - models.CLAIM_RESERVED_PARAMETERS
        )
        return [urls[start : start + size] for start in range(0, len(urls), size)]

    def _claim_parameters(
        self,
        chunk: Sequence[CustomURL] | None,
        now: datetime,
        limit: int,
        job_timeout: timedelta,
        queue_timeout: timedelta,
    ) -> Parameters:
        """Bind one claim statement.

        The timeouts go through `_timeout_seconds`, the same encoder the read-only select uses, so a timeout cannot mean two things.

        Args:
            chunk: The URLs this statement may claim, or None for every row.
            now: The instant the predicates are evaluated against, in UTC.
            limit: This statement's own row limit; `-1` means no limit.
            job_timeout: How long a `started_crawl` row may stay untouched.
            queue_timeout: How long a `queued` row may wait before re-claim.

        Returns:
            Parameters: The statement's named values, including one per `:uN`.
        """
        parameters: Parameters = {
            "claimed_state": CrawlState.QUEUED.value,
            "now": now,
            "max_items": limit,
            "job_timeout": _timeout_seconds(job_timeout),
            "queue_timeout": _timeout_seconds(queue_timeout),
        }
        if chunk is not None:
            for name, url in zip(models.url_parameter_names(chunk), chunk):
                parameters[name] = url.get_url()
        return parameters

    async def _write_outcomes(
        self,
        finished: Sequence[tuple[CustomURL, datetime | None]],
        discovered: Sequence[CustomURL],
        now: datetime,
    ) -> None:
        """Update every finished row, then insert every discovered one.

        Args:
            finished: One `(url, next_crawl_time)` pair per attempted URL, where a None time stores NULL and schedules no re-crawl.
            discovered: URLs to insert when absent, each becoming `not_crawled` with `created_time = next_crawl_time`.
            now: The completion time, in UTC, stored on every written row.
        """
        if finished:
            await self._run_many(
                models.COMPLETE_CRAWL_SQL,
                [
                    {
                        "custom_url": url.get_url(),
                        "finished_state": CrawlState.FINISHED_CRAWL.value,
                        "next_crawl_time": next_crawl_time,
                        "now": now,
                    }
                    for url, next_crawl_time in finished
                ],
            )
        if discovered:
            await self._run_many(
                models.INSERT_URL_SQL,
                [
                    {
                        "custom_url": url.get_url(),
                        "created_time": now,
                        "next_crawl_time": now,
                        "fresh_state": CrawlState.NOT_CRAWLED.value,
                        "status_time": now,
                    }
                    for url in discovered
                ],
            )
