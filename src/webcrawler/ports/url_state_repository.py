"""The URL state store port (goal.md:20-53).

`goal.md:20-53` describes one table that every module reads and writes: the
poller claims rows from it, the worker marks and completes them, and the
orchestrator seeds it. Nothing above this line may know that the store is
SQLite, so the whole state machine is expressed as an abstract class here and
the default implementation stays the simple one, SQLite through `aiosqlite`
(goal.md:11, goal.md:16).

All times crossing this boundary are timezone-aware UTC `datetime` values;
converting to and from the stored `%Y-%m-%d %H:%M:%S` text is the
implementation's job (goal.md:25).
"""

from abc import ABC, abstractmethod
from datetime import datetime, timedelta

from webcrawler.domain.custom_url import CustomURL

class URLStateRepository(ABC):
    """Crawl state for every URL the crawler has seen, keyed by canonical form.

    One row per `CustomURL`: created, last crawled, next due, lifecycle state,
    and when that state last changed. The claim methods are the only handoff
    between the poller and the workers, and they are the sole place where two
    concurrent callers can contend for the same URL.

    Every method is a coroutine, so the single event loop of `goal.md:13` is
    never blocked by this store's own I/O.
    """

@abstractmethod
async def initialize(self) -> None:
    """Create the table and both indexes when they are absent.

    Idempotent, so it is safe on every start. Times are stored as UTC text
    and every insert or update binds them as parameters rather than
    relying on a SQLite clock, because `goal.md:27` requires
    `next_crawl_time == created_time` on insert.

    Raises:
    sqlite3.Error: If the schema cannot be created or the connection
    is already closed.
    """

@abstractmethod
async def create_urls(self, urls: list[CustomURL]) -> None:
    """Insert the given URLs, ignoring any that already exist.

    An insert is the only way a row gets `not_crawled` state, and it sets
    `created_time` and `next_crawl_time` to the same instant, so a fresh
    row is immediately eligible for a claim (goal.md:27).

    Args:
    urls: The canonical URLs to make known to the crawler. An empty
    list is a no-op.

    Raises:
    sqlite3.Error: If the batch cannot be written.
    """

@abstractmethod
async def close(self) -> None:
    """Close the connection and join the worker thread it owns.

    Idempotent, so the orchestrator can call it from its shutdown path
    without knowing whether anything else already closed the store.

    aiosqlite runs each connection on a non-daemon thread that blocks
    until the connection closes, and interpreter shutdown joins non-daemon
    threads before finalization. Without this, `Ctrl+C` and pytest session
    teardown both hang.

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
    queue_timeout: timedelta) -> list[CustomURL]:
    """Read the rows that a claim would currently select, changing nothing.

    Observability and tests only; the poller uses the claim methods, so
    this is not the production handoff. A read that races a concurrent
    claim can report a row a claim has already moved, which is why no
    caller may treat the result as reserved work.

    It applies the same predicate and the same timeouts as
    `claim_candidates`, so a `started_crawl` or `queued` row that is still
    within its timeout is not reported as crawlable: the read must not
    preview work somebody is already doing (goal.md:33-51).

    Args:
    now: The instant the eligibility predicates are evaluated against,
    in UTC.
    max_items: The row limit. `-1` means no limit, the `goal.md:59`
    default.
    job_timeout: How long a `started_crawl` row may stay untouched
    before it is treated as abandoned and reclaimable.
    queue_timeout: How long a `queued` row may wait before it is
    reported as reclaimable.

    Returns:
    list[CustomURL]: Up to `max_items` eligible URLs ordered by
    `next_crawl_time` ascending.

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
    queue_timeout: timedelta) -> list[CustomURL]:
    """Atomically move eligible rows to `queued` and return them.

    `BEGIN IMMEDIATE` then `UPDATE... RETURNING` in one transaction
    (goal.md:67-98), so a row is handed to at most one caller even with
    several claimers on the loop. No lock guards the transition: the
    predicate only re-targets rows whose `queued` or `started_crawl`
    status is older than its timeout, which is what prevents a double
    claim (goal.md:109). A row whose DB write succeeded while
    the queue send did not is recovered by the `queue_timeout` branch.

    Args:
    now: The instant the eligibility predicates are evaluated against,
    in UTC.
    max_items: The row limit. `-1` means no limit.
    job_timeout: How long a `started_crawl` row may stay untouched
    before it is treated as abandoned and reclaimed.
    queue_timeout: How long a `queued` row may wait before it is
    re-claimed, which is how a lost queue send is recovered.

    Returns:
    list[CustomURL]: Only the rows this caller actually moved to
    `queued`, so the caller never enqueues a row somebody else
    already owns.

    Raises:
    sqlite3.Error: If the transaction cannot be committed, in which
    case it is rolled back and no row is claimed.
    """

@abstractmethod
async def claim_urls(
    self,
    urls: list[CustomURL],
    now: datetime,
    max_items: int = -1,
    *,
    job_timeout: timedelta,
    queue_timeout: timedelta) -> list[CustomURL]:
    """As `claim_candidates`, restricted to the caller's own URLs.

    Backs API (a) of `goal.md:103`, where the operator names the URLs to
    queue. The `custom_url IN (...)` restriction is applied inside the
    claiming subquery, ahead of the `LIMIT`, so a batch is never
    under-filled with rows the caller did not ask for. An empty `urls`
    list issues no statement at all.

    `max_items` defaults to `-1` because the caller's list is already the
    batch, so the limit is the caller's to impose or not.

    Args:
    urls: The URLs the caller wants queued. They are expected to
    already exist as rows, which is why this claims and never
    inserts.
    now: The instant the eligibility predicates are evaluated against,
    in UTC.
    max_items: The row limit. `-1` means no limit.
    job_timeout: How long a `started_crawl` row may stay untouched
    before it is treated as abandoned and reclaimed.
    queue_timeout: How long a `queued` row may wait before it is
    re-claimed.

    Returns:
    list[CustomURL]: The subset of `urls` that this caller moved to
    `queued`, which is empty for a missing or freshly queued URL.

    Raises:
    sqlite3.Error: If the transaction cannot be committed, in which
    case it is rolled back and no row is claimed.
    """

@abstractmethod
async def mark_started(self, url: CustomURL, now: datetime) -> None:
    """Record that a worker has begun crawling one URL.

    One transaction writes `state = started_crawl`, `last_crawl_time = now`
    and `last_status_update_time = now`. That last column is what makes the
    row reclaimable by the `job_timeout` branch if this worker dies before
    writing a completion (goal.md:82-83), so it is the only writer of
    `last_crawl_time`.

    Args:
    url: The URL being crawled.
    now: The attempt time, in UTC.

    Raises:
    sqlite3.Error: If the update cannot be committed, in which case the
    row is not marked and stays claimable.
    """

@abstractmethod
async def complete_crawl(
    self,
    finished: list[tuple[CustomURL, datetime | None]],
    discovered: list[CustomURL],
    now: datetime) -> None:
    """Write one batch of outcomes and one batch of discoveries atomically.

    A single bulk transaction (goal.md:145-148), so a batch either lands
    whole or not at all, which is what lets the worker treat a failure
    here as an abort rather than a partial success.

    Args:
    finished: One `(url, next_crawl_time)` pair per attempted URL.
    A `None` time means no re-crawl is scheduled. The pair carries
    per-row scheduling because one batch can mix success (None),
    retry exhaustion (`now + reschedule_delay`), and a politeness
    skip (`now + wait_ms`). Every row in the call also gets
    `last_status_update_time = now`.
    discovered: URLs found on the crawled pages, inserted when absent
    with `created_time = next_crawl_time`. An existing row is left
    untouched, so a re-crawl never resets another row's schedule.
    now: The completion time, in UTC.

    Raises:
    sqlite3.Error: If the transaction cannot be committed, in which
    case it is rolled back and no outcome is recorded, leaving the
    rows `started_crawl` for the `job_timeout` branch to reclaim.
    """
