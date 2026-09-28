"""The URL state store port.

One table every module reads and writes, so nothing above this line may know the
store is SQLite; the default is `aiosqlite`. Times here are timezone-aware UTC
`datetime`; the stored text form is its job.
"""

from abc import ABC, abstractmethod
from datetime import datetime, timedelta

from webcrawler.domain.custom_url import CustomURL


class URLStateRepository(ABC):
    """Crawl state for every URL seen, keyed by canonical form.

    One row per `CustomURL`: created, last crawled, next due, lifecycle state, and when that state
    last changed. The claim methods are the only poller to worker handoff and the only place two
    callers contend for a URL, and every method is a coroutine, so the loop never blocks on this I/O.
    """

    @abstractmethod
    async def initialize(self) -> None:
        """Create the table and both indexes when they are absent, idempotently.

        Times are bound parameters rather than read from the SQLite clock, so the insert can require `next_crawl_time == created_time`.

        Raises:
            sqlite3.Error: If the schema fails or the connection is closed.
        """

    @abstractmethod
    async def create_urls(self, urls: list[CustomURL]) -> None:
        """Insert the given URLs, ignoring any that already exist.

        The only path to a `not_crawled` row, and it sets `created_time` and `next_crawl_time` to the same instant, so a fresh row is immediately claimable.

        Args:
            urls: The canonical URLs to make known; an empty list is a no-op.

        Raises:
            sqlite3.Error: If the batch cannot be written.
        """

    @abstractmethod
    async def close(self) -> None:
        """Close the connection and join the worker thread it owns; idempotent.

        aiosqlite's connection thread is non-daemon and blocks until close, so skipping this hangs `Ctrl+C` and pytest session teardown.

        Raises:
            sqlite3.Error: If closing the connection fails.
        """

    @abstractmethod
    async def get_crawlable_urls(
        self,
        now: datetime,
        max_items: int,
        *,
        job_timeout: timedelta,
        queue_timeout: timedelta,
    ) -> list[CustomURL]:
        """Read the rows a claim would select now, changing nothing.

        Never reserves work, so for observability and tests only: a read racing a claim may report an already-moved row. It applies the same predicate and timeouts as `claim_candidates`.

        Args:
            now: The instant the eligibility predicates are evaluated, in UTC.
            max_items: The row limit; `-1` means no limit, the default.
            job_timeout: How long a `started_crawl` row stays untouched before it is abandoned and reclaimable.
            queue_timeout: How long a `queued` row may wait before it is reported as reclaimable.

        Returns:
            list[CustomURL]: Up to `max_items` eligible URLs ordered by `next_crawl_time` ascending.

        Raises:
            sqlite3.Error: If the query cannot be executed.
        """

    @abstractmethod
    async def claim_candidates(
        self,
        now: datetime,
        max_items: int,
        *,
        job_timeout: timedelta,
        queue_timeout: timedelta,
    ) -> list[CustomURL]:
        """Atomically move eligible rows to `queued` and return them.

        One `BEGIN IMMEDIATE` plus `UPDATE...RETURNING` hands a row to at most one caller with no lock. The predicate re-targets only rows past their timeout, which also recovers a row whose DB write landed but whose queue send did not.

        Args:
            now: The instant the eligibility predicates are evaluated, in UTC.
            max_items: The row limit; `-1` means no limit.
            job_timeout: How long a `started_crawl` row stays untouched before it is abandoned and reclaimable.
            queue_timeout: How long a `queued` row may wait before it is reported as reclaimable.

        Returns:
            list[CustomURL]: The rows this call moved, ordered by `next_crawl_time` ascending.

        Raises:
            sqlite3.Error: If the claim cannot be executed.
        """

    @abstractmethod
    async def claim_urls(
        self,
        urls: list[CustomURL],
        now: datetime,
        max_items: int = -1,
        *,
        job_timeout: timedelta,
        queue_timeout: timedelta,
    ) -> list[CustomURL]:
        """Atomically move eligible rows among the given URLs to `queued`.

        The URL restriction sits inside the claiming subquery and before the limit, so `max_items` bounds the result rather than the scan, and an ineligible URL returns nothing instead of displacing another row.

        Args:
            urls: The URLs the caller wants queued; an empty list issues no statement.
            now: The instant the eligibility predicates are evaluated, in UTC.
            max_items: The row limit; `-1` means no limit.
            job_timeout: How long a `started_crawl` row stays untouched before it is abandoned and reclaimable.
            queue_timeout: How long a `queued` row may wait before it is reported as reclaimable.

        Returns:
            list[CustomURL]: The claimed rows among `urls`, ordered by `next_crawl_time` ascending.

        Raises:
            sqlite3.Error: If the claim cannot be executed.
        """

    @abstractmethod
    async def mark_started(self, urls: list[CustomURL], now: datetime) -> None:
        """Record that attempts on the given URLs begin now.

        Writes the attempt time, not the outcome, so the row stays claimable while the attempt runs: a worker that dies leaves a `started_crawl` row for `job_timeout` to reclaim.

        Args:
            urls: The URLs being crawled, all already held by the store; an empty list issues no statement.
            now: The attempt time, in UTC.

        Raises:
            sqlite3.Error: If the update cannot be executed.
        """

    @abstractmethod
    async def complete_crawl(
        self,
        finished: list[tuple[CustomURL, datetime | None]],
        discovered: list[CustomURL],
        now: datetime,
    ) -> None:
        """Record every finished row and insert the URLs they revealed.

        One transaction, so a failure rolls the whole batch back and no reader ever sees a row finished while the URLs it discovered are missing.

        Args:
            finished: Each crawled URL with the instant it becomes due again, or None for no re-crawl; a `finished_crawl` row with no `next_crawl_time` is never selected again.
            discovered: URLs found on the crawled pages, inserted when absent with `next_crawl_time` equal to their `created_time` so they are claimable at once; an existing row is left exactly as it is.
            now: The completion time, in UTC.

        Raises:
            sqlite3.Error: If the batch cannot be written.
        """
