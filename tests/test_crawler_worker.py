"""Tests for `CrawlerWorker`.

Every collaborator is a double: a recording repository that models the two
writes the worker makes, a recording reader over a real deque so `peek` and
`commit` behave like the in-memory topic, a scripted fetcher, extractor, store,
queuer, and politeness policy, a fake retry policy whose identity is checked,
and a frozen clock. Nothing here sleeps for real: `asyncio.sleep` is replaced
by a recorder, and `run` is stopped by a reader that raises
`asyncio.CancelledError` once its scripted batches are served, which is how the
orchestrator ends a worker in production. `drive` wraps the loop in
`asyncio.wait_for`, so a loop that does not terminate fails its test instead of
hanging the session.

Four tests need the real state machine and use `SQLiteURLStateRepository`
against a file in `tmp_path`: what `mark_started` writes, what an aborted
completion leaves behind, and whether the claim predicate can reclaim a row in
each of those states. A fake could agree with a wrong implementation about a
SQL predicate, so those assertions are made against SQLite itself.
"""

import asyncio
import contextlib
import logging
import sqlite3
from collections import deque
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable
from datetime import datetime, timedelta, timezone
from itertools import islice
from pathlib import Path
from typing import NamedTuple, TypeVar

import pytest

from webcrawler.application.worker import CrawlerWorker
from webcrawler.domain.crawl_state import CrawlState
from webcrawler.domain.custom_url import CustomURL
from webcrawler.domain.messages import BaseMessage
from webcrawler.infrastructure.db.sqlite_url_state_repository import (
    SQLiteURLStateRepository)
from webcrawler.ports.crawl_queuer import CrawlQueuer
from webcrawler.ports.link_extractor import LinkExtractor
from webcrawler.ports.politeness_policy import PolitenessPolicy
from webcrawler.ports.retry_policy import RetryPolicy
from webcrawler.ports.time_provider import TimeProviderFactory
from webcrawler.ports.topic_reader import TopicReader
from webcrawler.ports.url_state_repository import URLStateRepository
from webcrawler.ports.web_page_fetcher import WebPageFetcher

T = TypeVar("T")

NOW = datetime(2026, 9, 26, 11, 28, 0, tzinfo=timezone.utc)
SITE = "http://crawlme.monzo.com"
LOGGER_NAME = "tests.webcrawler.crawler_worker"
JOB_TIMEOUT = timedelta(minutes=5)
QUEUE_TIMEOUT = timedelta(seconds=30)
STAMP_FORMAT = "%Y-%m-%d %H:%M:%S"
DEFAULT_RESCHEDULE_DELAY = timedelta(minutes=1)
# The loop guard: a `run` that has not stopped inside this budget fails its
# test instead of sticking the session.
LOOP_GUARD_SECONDS = 5.0
STOP_MESSAGE = "the scripted batches are exhausted"

def real_logger() -> logging.Logger:
    """Return the real logger the worker and the store are given.

Returns:
logging.Logger: A named stdlib logger, so `caplog` can capture the
records the worker's own output consists of.
"""
    return logging.getLogger(LOGGER_NAME)

def page_url(index: int) -> CustomURL:
    """Return the nth page of the crawl site.

Args:
index: The page position, which also fixes a batch's canonical order.

Returns:
CustomURL: `http://crawlme.monzo.com/p<index>`.
"""
    return CustomURL(f"{SITE}/p{index}")

def message_for(url: CustomURL) -> BaseMessage:
    """Return the queue message that would carry a URL.

Args:
url: The URL the message carries.

Returns:
BaseMessage: The message, routed on `hash(url)` as the poller builds it
(goal.md:113).
"""
    return BaseMessage(url, partition_key=hash(url))

def bodies_for(urls: Iterable[CustomURL]) -> dict[str, str]:
    """Return a distinct body per URL, so a mixed-up body is visible.

Args:
urls: The pages being crawled.

Returns:
dict[str, str]: Canonical URL text to a body naming only that page.
"""
    return {url.get_url: f"<html><body>{url.get_url}</body></html>" for url in urls}

def stamp(moment: datetime) -> str:
    """Return the stored text of an instant.

Args:
moment: The instant to render.

Returns:
str: The instant in the store's `%Y-%m-%d %H:%M:%S` UTC format.
"""
    return moment.strftime(STAMP_FORMAT)

def flatten(group: BaseExceptionGroup[BaseException]) -> list[BaseException]:
    """Return every leaf of an exception group, however deeply nested.

Args:
group: The group an `asyncio.TaskGroup` raised.

Returns:
list[BaseException]: The individual errors, in no particular order.
"""
    found: list[BaseException] = []
    for error in group.exceptions:
        if isinstance(error, BaseExceptionGroup):
            found.extend(flatten(error))
        else:
            found.append(error)
            return found

def read_row(path: Path, raw: str) -> sqlite3.Row | None:
    """Read one URL row through an independent connection.

A second connection is used so the assertion sees the file the store wrote,
not a cursor the store still owns.

Args:
path: The database file to read.
raw: The canonical URL, which is the primary key.

Returns:
sqlite3.Row | None: The row, or None when there is no such URL.
"""
    with contextlib.closing(sqlite3.connect(path)) as connection:
        connection.row_factory = sqlite3.Row()
        return connection.execute(
            "SELECT * FROM urls WHERE custom_url = ?", (raw)
        ).fetchone

class CallRecorder:
    """One ordered log of the calls every double received.

A single shared list is what makes the ordering assertions meaningful: the
doubles cannot consult each other, so the only evidence of sequence is the
order in which they appended here.

    Attributes:
entries: The recorded `(name, detail)` pairs, in call order.
"""

def __init__(self) -> None:
    """Start with an empty log."""
    self.entries: list[tuple[str, object]] = []

def record(self, name: str, detail: object = None) -> None:
    """Append one call to the log.

Args:
name: The collaborator and method, as `"fetch"`.
detail: Whatever identifies the call, for an assertion to match on.
"""
    self.entries.append((name, detail))

def names(self) -> list[str]:
    """Return the recorded call names, in order.

Returns:
list[str]: One name per recorded call, repeats included.
"""
    return [name for name, _ in self.entries]

def count(self, name: str) -> int:
    """Return how many times a call was recorded.

Args:
name: The call name to count.

Returns:
int: The number of recorded calls with that name.
"""
    return self.names.count(name)

def positions(self, name: str) -> list[int]:
    """Return the log positions of every call with a given name.

Args:
name: The call name to locate.

Returns:
list[int]: The indexes into `entries`, in call order.
"""
    return [index for index, entry in enumerate(self.entries) if entry[0] == name]

def details(self, name: str) -> list[object]:
    """Return the detail of every call with a given name, in call order.

Args:
name: The call name to look up.

Returns:
list[object]: One detail per recorded call, repeats included.
"""
    return [detail for recorded, detail in self.entries if recorded == name]

class SleepRecorder:
    """A stand-in for `asyncio.sleep` that records instead of waiting.

Monkeypatched over `asyncio.sleep`, so a test asserts the length of every
wait the worker asked for and the suite never spends real time in one. It
records without yielding, because a real yield would make the numbers of
concurrent arrivals in the barrier test mean nothing.

    Attributes:
durations: The waits requested, in request order, in seconds.
"""

def __init__(self) -> None:
    """Start with no recorded waits."""
    self.durations: list[float] = []

async def __call__(self, delay: float) -> None:
    """Record one requested wait and return immediately.

Args:
delay: The seconds the worker asked to wait.
"""
    self.durations.append(delay)

class ArrivalBarrier:
    """A gate that opens only once every expected caller has arrived.

A barrier is the only honest way to show simultaneity: a sequential worker
parks on the first arrival and never opens the gate, so the loop guard fails
the test rather than passing it slowly.

Args:
expected: How many arrivals must be in flight before the gate opens.
"""

def __init__(self, expected: int) -> None:
    """Open the gate once `expected` callers have arrived.

Args:
expected: How many arrivals must overlap.
"""
    self._expected = expected
    self.arrivals = 0
    self.peak = 0
    self._gate = asyncio.Event()

async def arrive(self) -> None:
    """Register one arrival and block until every expected caller arrived.

Returns:
None: The caller resumes only once the gate is open, so a batch
cannot be recorded as finished while a URL is outstanding.
"""
    self.arrivals += 1
    self.peak = max(self.peak, self.arrivals)
    if self.arrivals >= self._expected:
        self._gate.set()
        await self._gate.wait()

class MarkSnapshot(NamedTuple):
    """One row as it stood the instant `mark_started` returned."""

    state: str
    last_crawl_time: str | None
    last_status_update_time: str
    next_crawl_time: str | None

class FakeRepository(URLStateRepository):
    """A recording store that models the two writes the worker makes.

It models the state machine rather than the SQL, so a test can assert that
an abort left a row `started_crawl`; the tests that need the real predicate
behind that claim use `SQLiteURLStateRepository` instead.

Args:
recorder: The shared ordered call log.
complete_crawl_error: The error `complete_crawl` raises, or None to
record the batch normally.
"""

def __init__(
    self,
    recorder: CallRecorder,
    *,
    complete_crawl_error: BaseException | None = None) -> None:
    """Record into `recorder` and start with no rows.

Args:
recorder: The shared ordered call log.
complete_crawl_error: The error `complete_crawl` raises, or None.
"""
    self._recorder = recorder
    self._complete_crawl_error = complete_crawl_error
    self.states: dict[str, CrawlState] = {}
    self.finished: list[tuple[CustomURL, datetime | None]] = []
    self.discovered: list[CustomURL] = []
    self.completed_at: list[datetime] = []

async def initialize(self) -> None:
    """Do nothing; the composition root initializes a store, not the worker."""

async def create_urls(self, urls: list[CustomURL]) -> None:
    """Do nothing; the worker only discovers through its completion call.

Args:
urls: The URLs, unused because the worker never inserts one itself.
"""

async def close(self) -> None:
    """Do nothing; the orchestrator owns the store's lifetime."""

async def get_crawlable_urls(
    self,
    now: datetime,
    max_items: int,
    *,
    job_timeout: timedelta,
    queue_timeout: timedelta) -> list[CustomURL]:
    """Return nothing; the claim predicate is tested against the real store.

Args:
now: The instant the predicate would be evaluated at.
max_items: The row limit.
job_timeout: The `started_crawl` staleness timeout.
queue_timeout: The `queued` staleness timeout.

Returns:
list[CustomURL]: Always empty, because this double does not model
the claim.
"""
    return []

async def claim_candidates(
    self,
    now: datetime,
    max_items: int,
    *,
    job_timeout: timedelta,
    queue_timeout: timedelta) -> list[CustomURL]:
    """Return nothing; the poller claims candidates, not the worker.

Args:
now: The instant the predicate would be evaluated at.
max_items: The row limit.
job_timeout: The `started_crawl` staleness timeout.
queue_timeout: The `queued` staleness timeout.

Returns:
list[CustomURL]: Always empty.
"""
    return []

async def claim_urls(
    self,
    urls: list[CustomURL],
    now: datetime,
    max_items: int = -1,
    *,
    job_timeout: timedelta,
    queue_timeout: timedelta) -> list[CustomURL]:
    """Return nothing; the queuer claims, not the worker.

Args:
urls: The caller's URLs.
now: The instant the predicate would be evaluated at.
max_items: The row limit.
job_timeout: The `started_crawl` staleness timeout.
queue_timeout: The `queued` staleness timeout.

Returns:
list[CustomURL]: Always empty.
"""
    return []

async def mark_started(self, url: CustomURL, now: datetime) -> None:
    """Record the call and move the row to `started_crawl`.

Args:
url: The URL being crawled.
now: The attempt instant the worker read from its clock.
"""
    self._recorder.record("mark_started", url.get_url)
    self.states[url.get_url] = CrawlState.STARTED_CRAWL

async def complete_crawl(
    self,
    finished: list[tuple[CustomURL, datetime | None]],
    discovered: list[CustomURL],
    now: datetime) -> None:
    """Record the call, then either fail or apply the whole batch.

Args:
finished: The `(url, next_crawl_time)` pairs the worker built.
discovered: The URLs the crawled pages revealed.
now: The completion instant the worker read from its clock.

Raises:
BaseException: The configured error, before any row is written, so a
test can prove nothing was recorded.
"""
    self._recorder.record(
        "complete_crawl", tuple(url.get_url for url, _ in finished)
    )
    if self._complete_crawl_error is not None:
        raise self._complete_crawl_error
        self.finished = list(finished)
        self.discovered = list(discovered)
        self.completed_at.append(now)
        for url, _ in finished:
            self.states[url.get_url] = CrawlState.FINISHED_CRAWL
            for url in discovered:
                self.states.setdefault(url.get_url, CrawlState.NOT_CRAWLED)

def state_of(self, url: CustomURL) -> CrawlState | None:
    """Return the modelled state of one row.

Args:
url: The row to look up.

Returns:
CrawlState | None: Its state, or None when the row is unknown.
"""
    return self.states.get(url.get_url())

class FailingCompleteCrawlRepository(URLStateRepository):
    """The real store, wrapped so that only `complete_crawl` fails.

Composed rather than inherited from the concrete store, so the test keeps
the real claim predicate, the real transactions, and the real rows, and
loses exactly one method: the write the worker must treat as unrecordable.

Args:
inner: The real store every other call is delegated to.
error: The error `complete_crawl` raises instead of writing.
"""

def __init__(self, inner: URLStateRepository, error: BaseException) -> None:
    """Hold the store to delegate to and the error to raise.

Args:
inner: The real store, already initialized by the caller.
error: The error `complete_crawl` raises.
"""
    self._inner = inner
    self._error = error

async def initialize(self) -> None:
    """Create the schema in the wrapped store."""

async def create_urls(self, urls: list[CustomURL]) -> None:
    """Insert URLs in the wrapped store.

Args:
urls: The URLs to insert when absent.
"""
    await self._inner.create_urls(urls)

async def close(self) -> None:
    """Close the wrapped store."""

async def get_crawlable_urls(
    self,
    now: datetime,
    max_items: int,
    *,
    job_timeout: timedelta,
    queue_timeout: timedelta) -> list[CustomURL]:
    """Ask the wrapped store which rows a claim would select.

Args:
now: The instant the predicate is evaluated against.
max_items: The row limit.
job_timeout: The `started_crawl` staleness timeout.
queue_timeout: The `queued` staleness timeout.

Returns:
list[CustomURL]: The eligible rows, from the real predicate.
"""
    return await self._inner.get_crawlable_urls(
        now, max_items, job_timeout=job_timeout, queue_timeout=queue_timeout
    )

async def claim_candidates(
    self,
    now: datetime,
    max_items: int,
    *,
    job_timeout: timedelta,
    queue_timeout: timedelta) -> list[CustomURL]:
    """Claim through the wrapped store.

Args:
now: The instant the predicate is evaluated against.
max_items: The row limit.
job_timeout: The `started_crawl` staleness timeout.
queue_timeout: The `queued` staleness timeout.

Returns:
list[CustomURL]: The rows this call moved to `queued`.
"""
    return await self._inner.claim_candidates(
        now, max_items, job_timeout=job_timeout, queue_timeout=queue_timeout
    )

async def claim_urls(
    self,
    urls: list[CustomURL],
    now: datetime,
    max_items: int = -1,
    *,
    job_timeout: timedelta,
    queue_timeout: timedelta) -> list[CustomURL]:
    """Claim the caller's URLs through the wrapped store.

Args:
urls: The caller's URLs.
now: The instant the predicate is evaluated against.
max_items: The row limit.
job_timeout: The `started_crawl` staleness timeout.
queue_timeout: The `queued` staleness timeout.

Returns:
list[CustomURL]: The rows this call moved to `queued`.
"""
    return await self._inner.claim_urls(
        urls, now, max_items, job_timeout=job_timeout, queue_timeout=queue_timeout
    )

async def mark_started(self, url: CustomURL, now: datetime) -> None:
    """Mark started in the wrapped store.

Args:
url: The URL being crawled.
now: The attempt instant.
"""
    await self._inner.mark_started(url, now)

async def complete_crawl(
    self,
    finished: list[tuple[CustomURL, datetime | None]],
    discovered: list[CustomURL],
    now: datetime) -> None:
    """Refuse the write, which is the failure the worker must survive.

Args:
finished: The pairs the worker built, discarded.
discovered: The discovered URLs, discarded.
now: The completion instant, discarded.

Raises:
BaseException: Always the configured error, because a real
`complete_crawl` failure leaves nothing recorded.
"""
    raise self._error

class MarkObservingRepository(URLStateRepository):
    """The real store, wrapped to snapshot each row the moment it is marked.

The completion transaction overwrites the state, so a row read after the
batch cannot show what `mark_started` wrote; reading it from inside the
delegation is what makes the mark's own writes observable.

Args:
inner: The real store every call is delegated to.
path: The database file, read through a second connection.
"""

def __init__(self, inner: URLStateRepository, path: Path) -> None:
    """Hold the store to delegate to and the file to read.

Args:
inner: The real store, already initialized by the caller.
path: The database file the store writes.
"""
    self._inner = inner
    self._path = path
    self.marks: list[MarkSnapshot] = []

async def initialize(self) -> None:
    """Create the schema in the wrapped store."""

async def create_urls(self, urls: list[CustomURL]) -> None:
    """Insert URLs in the wrapped store.

Args:
urls: The URLs to insert when absent.
"""
    await self._inner.create_urls(urls)

async def close(self) -> None:
    """Close the wrapped store."""

async def get_crawlable_urls(
    self,
    now: datetime,
    max_items: int,
    *,
    job_timeout: timedelta,
    queue_timeout: timedelta) -> list[CustomURL]:
    """Ask the wrapped store which rows a claim would select.

Args:
now: The instant the predicate is evaluated against.
max_items: The row limit.
job_timeout: The `started_crawl` staleness timeout.
queue_timeout: The `queued` staleness timeout.

Returns:
list[CustomURL]: The eligible rows, from the real predicate.
"""
    return await self._inner.get_crawlable_urls(
        now, max_items, job_timeout=job_timeout, queue_timeout=queue_timeout
    )

async def claim_candidates(
    self,
    now: datetime,
    max_items: int,
    *,
    job_timeout: timedelta,
    queue_timeout: timedelta) -> list[CustomURL]:
    """Claim through the wrapped store.

Args:
now: The instant the predicate is evaluated against.
max_items: The row limit.
job_timeout: The `started_crawl` staleness timeout.
queue_timeout: The `queued` staleness timeout.

Returns:
list[CustomURL]: The rows this call moved to `queued`.
"""
    return await self._inner.claim_candidates(
        now, max_items, job_timeout=job_timeout, queue_timeout=queue_timeout
    )

async def claim_urls(
    self,
    urls: list[CustomURL],
    now: datetime,
    max_items: int = -1,
    *,
    job_timeout: timedelta,
    queue_timeout: timedelta) -> list[CustomURL]:
    """Claim the caller's URLs through the wrapped store.

Args:
urls: The caller's URLs.
now: The instant the predicate is evaluated against.
max_items: The row limit.
job_timeout: The `started_crawl` staleness timeout.
queue_timeout: The `queued` staleness timeout.

Returns:
list[CustomURL]: The rows this call moved to `queued`.
"""
    return await self._inner.claim_urls(
        urls, now, max_items, job_timeout=job_timeout, queue_timeout=queue_timeout
    )

async def mark_started(self, url: CustomURL, now: datetime) -> None:
    """Delegate the mark, then snapshot the row it just wrote.

Args:
url: The URL being crawled.
now: The attempt instant.
"""
    await self._inner.mark_started(url, now)
    row = read_row(self._path, url.get_url)
    if row is None:
        raise AssertionError(f"mark_started wrote no row for {url.get_url}")
        self.marks.append(
            MarkSnapshot(
            state=row["state"],
            last_crawl_time=row["last_crawl_time"],
            last_status_update_time=row["last_status_update_time"],
            next_crawl_time=row["next_crawl_time"])
        )

async def complete_crawl(
    self,
    finished: list[tuple[CustomURL, datetime | None]],
    discovered: list[CustomURL],
    now: datetime) -> None:
    """Apply the batch in the wrapped store.

Args:
finished: The `(url, next_crawl_time)` pairs the worker built.
discovered: The URLs the crawled pages revealed.
now: The completion instant.
"""
    await self._inner.complete_crawl(finished, discovered, now)

class FakeReader(TopicReader):
    """A reader over a real deque that stops the loop when scripted out.

The deque makes `peek` non-reserving and `commit` head-based, exactly as
the in-memory topic is (goal.md:125-127), so a test can append while the
worker is busy and observe which end a commit removed. `serve_batches` is
what keeps `run` finite: once that many batches have been served, the
next `peek` raises `asyncio.CancelledError`, which is exactly how the
orchestrator stops a worker in production.

Args:
recorder: The shared ordered call log.
serve_batches: How many peeks serve a batch before the loop is stopped.
"""

def __init__(
    self,
    recorder: CallRecorder,
    *,
    serve_batches: int = 1) -> None:
    """Take a batch budget; the reader is wired by its constructor.

Args:
recorder: The shared ordered call log.
serve_batches: How many peeks serve a batch before stopping.
"""
    self._recorder = recorder
    self._serve_batches = serve_batches
    self._served = 0
    self.messages: deque[BaseMessage] = deque
    self.batches_served: list[list[BaseMessage]] = []
    self.commit_sizes: list[int] = []

async def peek(self, n: int) -> list[BaseMessage]:
    """Return up to `n` messages from the head, removing nothing.

Args:
n: The largest number of messages wanted.

Returns:
list[BaseMessage]: The head of the partition, in partition order.

Raises:
asyncio.CancelledError: Once the scripted batches are served, which
is the only thing that ends the worker's loop in a test.
"""
    self._recorder.record("peek", n)
    if self._served >= self._serve_batches:
        raise asyncio.CancelledError(STOP_MESSAGE)
        self._served += 1
        batch = list(islice(self.messages, n))
        self.batches_served.append(batch)
        return batch

async def commit(self, items: list[BaseMessage]) -> None:
    """Remove `min(len(items), available)` messages from the head.

Args:
items: The batch being acknowledged; only its count is used, as the
in-memory topic does (goal.md:126).
"""
    self._recorder.record("commit", tuple(item.url.get_url for item in items))
    self.commit_sizes.append(len(items))
    for _ in range(min(len(items), len(self.messages))):
        self.messages.popleft()

async def produce(self, items: list[BaseMessage]) -> None:
    """Append to the tail, as a concurrent producer would.

Args:
items: The messages to append, in order.
"""
    self._recorder.record("produce", tuple(item.url.get_url for item in items))
    self.messages.extend(items)

class FakeFetcher(WebPageFetcher):
    """A fetcher recording the policy it was handed and serving scripted bodies.

Args:
recorder: The shared ordered call log.
bodies: The body per canonical URL, with a default body when absent.
failures: The canonical URLs whose fetch raises, as a fetch after its
retries are already spent (goal.md:17).
barrier: A gate every fetch must pass, which is how simultaneity is
proved.
"""

def __init__(
    self,
    recorder: CallRecorder,
    *,
    bodies: dict[str, str] | None = None,
    failures: set[str] | None = None,
    barrier: ArrivalBarrier | None = None) -> None:
    """Take the body script, the failing URLs, and any gate.

Args:
recorder: The shared ordered call log.
bodies: The body per canonical URL, or None for a default body.
failures: The canonical URLs whose fetch raises.
barrier: A gate every fetch awaits, so a batch is only finished once
all of its URLs have been in flight together.
"""
    self._recorder = recorder
    self._bodies = bodies or {}
    self._failures = failures or set
    self._barrier = barrier
    self._on_fetch: Callable[[CustomURL], Awaitable[None]] | None = None
    self.received: list[tuple[CustomURL, RetryPolicy]] = []

def set_on_fetch(self, hook: Callable[[CustomURL], Awaitable[None]]) -> None:
    """Install a hook awaited by `fetch` after the body is produced.

Args:
hook: The coroutine function, called with each fetched URL, which
lets a test enqueue a message while the batch is being
processed.
"""
    self._on_fetch = hook

async def fetch(self, url: CustomURL, retry_policy: RetryPolicy) -> str:
    """Return the scripted body, or fail, after passing any gate.

Args:
url: The page to retrieve.
retry_policy: The policy the worker injected, recorded unchanged.

Returns:
str: The scripted body, or a default naming the URL.

Raises:
OSError: For a scripted failure, standing in for a fetch whose own
retries are spent.
"""
    self._recorder.record("fetch", url.get_url)
    self.received.append((url, retry_policy))
    if self._barrier is not None:
        await self._barrier.arrive()
        if url.get_url in self._failures:
            raise OSError(f"transport failure for {url.get_url}")
            if self._on_fetch is not None:
                await self._on_fetch(url)
                return self._bodies.get(url.get_url, f"<html>{url.get_url}</html>")

class FakeLinkExtractor(LinkExtractor):
    """An extractor returning the links a test scripted for the page.

Args:
recorder: The shared ordered call log.
links_by_page: The links per canonical URL of the page.
error: The error every call raises, or None to extract normally.
"""

def __init__(
    self,
    recorder: CallRecorder,
    *,
    links_by_page: dict[str, list[CustomURL]] | None = None,
    error: BaseException | None = None) -> None:
    """Take the scripted links and the scripted failure.

Args:
recorder: The shared ordered call log.
links_by_page: The links to return per canonical URL of the page.
error: The error `extract` raises, or None.
"""
    self._recorder = recorder
    self._links_by_page = links_by_page or {}
    self._error = error

def extract(self, html: str, base_url: CustomURL) -> list[CustomURL]:
    """Return the scripted links for the page just fetched.

Args:
html: The body the fetcher returned.
base_url: The URL the body came from, which is the page whose links
are returned, and never the seed.

Returns:
list[CustomURL]: The scripted links, unfiltered and undeduplicated,
so a test can prove the worker passes them on as they are.

Raises:
RuntimeError: The scripted failure, which the worker does not
handle, so it aborts the batch.
"""
    self._recorder.record("extract", base_url.get_url)
    if self._error is not None:
        raise self._error
        return list(self._links_by_page.get(base_url.get_url, []))

class FakePolitenessPolicy(PolitenessPolicy):
    """A policy reporting one fixed wait, as `goal.md:140` asks for.

Args:
recorder: The shared ordered call log.
wait_ms: The milliseconds every call reports.
"""

def __init__(self, recorder: CallRecorder, wait_ms: int = 0) -> None:
    """Report `wait_ms` from every `before_fetch`.

Args:
recorder: The shared ordered call log.
wait_ms: The milliseconds to report; 0 means call now.
"""
    self._recorder = recorder
    self._wait_ms = wait_ms

def before_fetch(self) -> int:
    """Report the configured wait without sleeping.

Returns:
int: The same wait on every call, because the decision belongs to
the worker and this policy only reports a number.
"""
    self._recorder.record("before_fetch", self._wait_ms)
    return self._wait_ms

class FakeQueuer(CrawlQueuer):
    """A queuer recording exactly what it was asked to queue.

Args:
recorder: The shared ordered call log.
"""

def __init__(self, recorder: CallRecorder) -> None:
    """Start with nothing queued.

Args:
recorder: The shared ordered call log.
"""
    self._recorder = recorder
    self.enqueued: list[tuple[list[CustomURL], str | None]] = []

async def enqueue_urls(
    self, urls: list[CustomURL], request_id: str | None = None
) -> None:
    """Record the URLs and the dedupe key it was called with.

Args:
urls: The discovered URLs, which this double does not claim.
request_id: The dedupe key, unused because the worker mints none.
"""
    self._recorder.record("enqueue_urls", tuple(url.get_url for url in urls))
    self.enqueued.append((list(urls), request_id))

async def queue_candidates(
    self,
    now: datetime,
    max_items: int | None = None,
    request_id: str | None = None) -> None:
    """Do nothing; the worker never claims candidates itself.

Args:
now: The instant the predicate would be evaluated at.
max_items: The row limit, unused.
request_id: The dedupe key, unused.
"""
    self._recorder.record("queue_candidates", request_id)

class FakeRetryPolicy(RetryPolicy):
    """A policy that runs the operation once, for the real store's own I/O.

Args:
recorder: The shared ordered call log.
"""

def __init__(self, recorder: CallRecorder) -> None:
    """Start with no attempts recorded.

Args:
recorder: The shared ordered call log.
"""
    self._recorder = recorder

async def execute(self, operation: Callable[[], Awaitable[T]]) -> T:
    """Run one attempt, because a test must not wait out a backoff.

Args:
operation: The callable returning a fresh awaitable per attempt.

Returns:
T: Whatever the single attempt returned.
"""
    self._recorder.record("retry", None)
    return await operation

class FakeTimeProvider(TimeProviderFactory):
    """A clock frozen at one instant, as every port test needs (goal.md:25).

Args:
moment: The instant every call reports.
"""

def __init__(self, moment: datetime = NOW) -> None:
    """Freeze the clock at `moment`.

Args:
moment: The instant to report from now on.
"""
    self._moment = moment
    self.calls = 0

def now(self) -> datetime:
    """Report the frozen instant.

Returns:
datetime: The instant this clock was frozen at, in UTC.
"""
    self.calls += 1
    return self._moment

class Harness:
    """The doubles under test and the worker wired from them.

Every collaborator is reachable by name, so an assertion reads as the
behaviour it is about rather than as an index into a list of fakes, and the
doubles share one `CallRecorder`, so the order assertions are about a single
sequence.

Args:
recorder: The shared ordered call log.
repository: The crawl state store the worker was given.
reader: The deque-backed reader that stops the loop.
fetcher: The scripted fetcher.
link_extractor: The scripted extractor.
politeness_policy: The fixed-wait policy.
queuer: The recording queuer.
retry_policy: The policy instance the worker must hand to `fetch`.
time_provider: The frozen clock.
worker: The worker under test.
"""

def __init__(
    self,
    *,
    recorder: CallRecorder,
    repository: URLStateRepository,
    reader: TopicReader,
    fetcher: WebPageFetcher,
    link_extractor: LinkExtractor,
    politeness_policy: PolitenessPolicy,
    queuer: CrawlQueuer,
    retry_policy: RetryPolicy,
    time_provider: TimeProviderFactory,
    reschedule_delay: timedelta,
    batch_size: int,
    idle_sleep_seconds: float,
    sleep_threshold_ms: int,
    re_crawl_interval: timedelta | None) -> None:
    """Construct the worker from the doubles, naming only ports (goal.md:16).

Args:
recorder: The ordered call log the doubles were built with, so the
order assertions see the sequence they actually produced.
repository: The crawl state store to inject.
reader: The queue reader to inject.
fetcher: The page fetcher to inject.
link_extractor: The link extractor to inject.
politeness_policy: The politeness policy to inject.
queuer: The crawl queuer to inject.
retry_policy: The retry policy to inject and to expect at `fetch`.
time_provider: The clock to inject.
reschedule_delay: The reschedule delay to construct the worker with.
batch_size: The `peek` size to construct the worker with.
idle_sleep_seconds: The idle wait to construct the worker with.
sleep_threshold_ms: The politeness threshold to construct with.
re_crawl_interval: The re-crawl interval, or None for no re-crawl.
"""
    self.recorder = recorder
    self.repository = repository
    self.reader = reader
    self.fetcher = fetcher
    self.link_extractor = link_extractor
    self.politeness_policy = politeness_policy
    self.queuer = queuer
    self.retry_policy = retry_policy
    self.time_provider = time_provider
    self.worker = CrawlerWorker(
        repository,
        reader,
        fetcher,
        link_extractor,
        politeness_policy,
        queuer,
        retry_policy,
        batch_size=batch_size,
        idle_sleep_seconds=idle_sleep_seconds,
        reschedule_delay=reschedule_delay,
        sleep_threshold_ms=sleep_threshold_ms,
        re_crawl_interval=re_crawl_interval,
        time_provider=time_provider,
        logger=real_logger)

def build(
    *,
    urls: list[CustomURL] | None = None,
    bodies: dict[str, str] | None = None,
    links_by_page: dict[str, list[CustomURL]] | None = None,
    failures: set[str] | None = None,
    wait_ms: int = 0,
    sleep_threshold_ms: int = 2_000,
    re_crawl_interval: timedelta | None = None,
    reschedule_delay: timedelta = DEFAULT_RESCHEDULE_DELAY,
    idle_sleep_seconds: float = 1.0,
    batch_size: int = 100,
    serve_batches: int = 1,
    repository: URLStateRepository | None = None,
    extract_error: BaseException | None = None,
    complete_crawl_error: BaseException | None = None,
    barrier: ArrivalBarrier | None = None) -> Harness:
    """Build a worker whose every collaborator is a double.

Args:
urls: The messages to place in the reader's partition, in order.
bodies: The body per canonical URL the fetcher serves.
links_by_page: The links the extractor returns per canonical URL of the
page.
failures: The canonical URLs whose fetch raises.
wait_ms: The politeness wait in milliseconds.
sleep_threshold_ms: The worker's politeness threshold.
re_crawl_interval: The re-crawl interval, or None for no re-crawl.
reschedule_delay: The reschedule delay to configure explicitly; the
test that needs the constructor's own default builds the worker
itself instead of naming it here.
idle_sleep_seconds: The idle wait to construct the worker with.
batch_size: The `peek` size to construct the worker with.
serve_batches: How many peeks serve a batch before the loop is stopped.
repository: A store to inject instead of the recording double.
extract_error: The error the link extractor raises.
complete_crawl_error: The error `complete_crawl` raises.
barrier: A gate every fetch awaits.

Returns:
Harness: The doubles and the worker wired from them.
"""
    recorder = CallRecorder()
    reader = FakeReader(recorder, serve_batches=serve_batches)
    for url in urls or []:
        reader.messages.append(message_for(url))
        store = (
            repository
            if repository is not None
            else FakeRepository(recorder, complete_crawl_error=complete_crawl_error)
        )
        return Harness(
            recorder=recorder,
            repository=store,
            reader=reader,
            fetcher=FakeFetcher(
            recorder, bodies=bodies, failures=failures, barrier=barrier
        ),
            link_extractor=FakeLinkExtractor(
            recorder, links_by_page=links_by_page, error=extract_error
        ),
            politeness_policy=FakePolitenessPolicy(recorder, wait_ms),
            queuer=FakeQueuer(recorder),
            retry_policy=FakeRetryPolicy(recorder),
            time_provider=FakeTimeProvider,
            reschedule_delay=reschedule_delay,
            batch_size=batch_size,
            idle_sleep_seconds=idle_sleep_seconds,
            sleep_threshold_ms=sleep_threshold_ms,
            re_crawl_interval=re_crawl_interval)

async def drive(worker: CrawlerWorker, *, timeout: float = LOOP_GUARD_SECONDS) -> None:
    """Run a worker to its scripted stop, failing rather than hanging.

The reader raises `asyncio.CancelledError` once its batches are served, so
the loop ends the way the orchestrator ends it in production; the guard
turns a loop that never ends into a `TimeoutError` failure.

Args:
worker: The worker to run.
timeout: How long the loop may take before the test fails.
"""
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(worker.run, timeout)

@pytest.fixture
def sleeps(monkeypatch: pytest.MonkeyPatch) -> SleepRecorder:
    """Replace `asyncio.sleep` so no test ever waits for real.

Args:
monkeypatch: pytest's patcher, undone after the test.

Returns:
SleepRecorder: The recorder every requested wait is written to.
"""
    recorder = SleepRecorder()
    monkeypatch.setattr(asyncio, "sleep", recorder)
    return recorder

class RealStore(NamedTuple):
    """A real store and the file it writes, for reading the rows back.

Args:
repository: The initialized store the worker is given.
path: The database file, so a test can read it through a second
connection and see what was actually written.
"""

    repository: SQLiteURLStateRepository
    path: Path

@pytest.fixture
async def real_store(tmp_path: Path) -> AsyncIterator[RealStore]:
    """Yield a real store on a file, closed however the test ends.

The store owns an aiosqlite worker thread, so the `finally` is what keeps a
test session from hanging at teardown.

Args:
tmp_path: pytest's per-test directory.

Returns:
AsyncIterator[RealStore]: The initialized store and its file.
"""
    path = tmp_path / "urls.db"
    repository = SQLiteURLStateRepository(
        path, FakeRetryPolicy(CallRecorder), FakeTimeProvider, real_logger
    )
    await repository.initialize()
    try:
        yield RealStore(repository, path)
    finally:
        await repository.close()

@pytest.mark.parametrize("batch_count", [1, 2, 5])
async def test_a_batch_of_messages_is_committed_exactly_once(
    sleeps: SleepRecorder, batch_count: int
) -> None:
    """One peek, one commit: the reader's commit is head-based and
non-idempotent, so a second commit of the same batch would discard messages
nobody crawled (goal.md:127).

Args:
sleeps: The `asyncio.sleep` recorder, proving nothing idled.
batch_count: How many messages the one batch holds.
"""
    urls = [page_url(index) for index in range(batch_count)]
    harness = build(urls=urls, batch_size=batch_count)

    await drive(harness.worker())

    assert harness.recorder.count("commit") == 1
    assert harness.reader.commit_sizes == [batch_count]
    assert harness.reader.batches_served[0] == [message_for(url) for url in urls]
    assert not harness.reader.messages
    assert sleeps.durations == []

@pytest.mark.parametrize("batch_count", [1, 3])
async def test_the_batch_is_recorded_then_queued_then_committed(
    sleeps: SleepRecorder, batch_count: int
) -> None:
    """The order is the contract: `enqueue_urls` claims the discovered rows, so
they must be committed first, and the messages are acknowledged only once
the outcome is durable.

Args:
sleeps: The `asyncio.sleep` recorder.
batch_count: How many messages the batch holds.
"""
    urls = [page_url(index) for index in range(batch_count)]
    harness = build(
        urls=urls,
        links_by_page={urls[0].get_url: [page_url(99)]},
        batch_size=batch_count)

    await drive(harness.worker())

    names = harness.recorder.names()
    recorded = names.index("complete_crawl")
    queued = names.index("enqueue_urls")
    committed = names.index("commit")
    assert recorded < queued < committed
    assert harness.recorder.count("complete_crawl") == 1
    assert harness.recorder.count("enqueue_urls") == 1
    assert harness.recorder.count("commit") == 1

@pytest.mark.parametrize("batch_count", [1, 3])
async def test_the_batch_instant_is_read_once_per_batch(
    sleeps: SleepRecorder, batch_count: int
) -> None:
    """One clock read per batch: every mark, every outcome, and
the completion write share the batch's single instant, so the clock is
consulted once however many URLs the batch holds and a batch of
concurrent tasks can never disagree about when it ran.

Args:
sleeps: The `asyncio.sleep` recorder.
batch_count: How many URLs the batch holds, so one read is proved
against more than a single-URL batch.
"""
    urls = [page_url(index) for index in range(batch_count)]
    harness = build(urls=urls, batch_size=batch_count)

    await drive(harness.worker())

    time_provider = harness.time_provider()
    assert isinstance(time_provider, FakeTimeProvider)
    assert time_provider.calls == 1
    repository = harness.repository()
    assert isinstance(repository, FakeRepository)
    assert repository.completed_at == [NOW]

@pytest.mark.parametrize("batch_count", [1, 2, 4])
async def test_every_url_is_marked_started_before_it_is_fetched(
    sleeps: SleepRecorder, batch_count: int
) -> None:
    """`goal.md:135-137` orders the mark before the fetch: a row that is not
`started_crawl` cannot be reclaimed by the `job_timeout` branch if this
worker dies mid-crawl.

Args:
sleeps: The `asyncio.sleep` recorder.
batch_count: How many URLs the batch holds.
"""
    urls = [page_url(index) for index in range(batch_count)]
    harness = build(urls=urls, batch_size=batch_count)

    await drive(harness.worker())

    for url in urls:
        raw = url.get_url()
        marked = [
            position
            for position, detail in zip(
            harness.recorder.positions("mark_started"),
            harness.recorder.details("mark_started"))
            if detail == raw
        ]
        fetched = [
            position
            for position, detail in zip(
            harness.recorder.positions("fetch"),
            harness.recorder.details("fetch"))
            if detail == raw
        ]
        assert len(marked) == 1
        assert len(fetched) == 1
        assert marked[0] < fetched[0]

@pytest.mark.parametrize("batch_count", [1, 3])
async def test_the_fetch_is_given_the_injected_retry_policy_instance(
    sleeps: SleepRecorder, batch_count: int
) -> None:
    """One policy instance is shared with the store and the fetcher so all of
the process's I/O retries with the same settings (goal.md:17); handing the
fetcher a fresh policy would quietly undo that.

Args:
sleeps: The `asyncio.sleep` recorder.
batch_count: How many URLs the batch holds, so the identity is checked
on more than one call.
"""
    urls = [page_url(index) for index in range(batch_count)]
    harness = build(urls=urls, batch_size=batch_count)

    await drive(harness.worker())

    fetcher = harness.fetcher()
    assert isinstance(fetcher, FakeFetcher)
    assert [url for url, _ in fetcher.received] == urls
    assert all(policy is harness.retry_policy for _, policy in fetcher.received)

@pytest.mark.parametrize(
    "reschedule_delay",
    [
    pytest.param(timedelta(seconds=5), id="a_five_second_retry"),
    pytest.param(timedelta(minutes=10), id="a_ten_minute_retry"),
    pytest.param(timedelta(hours=2), id="a_two_hour_retry"),
    ])
async def test_a_fetch_failure_is_due_again_after_the_reschedule_delay(
    sleeps: SleepRecorder, reschedule_delay: timedelta
) -> None:
    """A fetch whose retries are spent is a handled outcome, not an abort: the
batch still commits and the URL becomes due again.

Args:
sleeps: The `asyncio.sleep` recorder, which must record no backoff,
because the retry belongs to the fetcher rather than the worker.
reschedule_delay: The delay to configure.
"""
    urls = [page_url(index) for index in range(2)]
    harness = build(
        urls=urls,
        failures={urls[0].get_url},
        reschedule_delay=reschedule_delay,
        batch_size=2)

    await drive(harness.worker())

    repository = harness.repository()
    assert isinstance(repository, FakeRepository)
    assert harness.recorder.count("commit") == 1
    assert repository.finished == [
        (urls[0], NOW + reschedule_delay),
        (urls[1], None),
    ]
    assert repository.state_of(urls[0]) is CrawlState.FINISHED_CRAWL
    assert repository.discovered == []
    queuer = harness.queuer()
    assert isinstance(queuer, FakeQueuer)
    assert queuer.enqueued == [([], None)]
    assert sleeps.durations == []

@pytest.mark.parametrize("batch_count", [1, 3])
async def test_the_reschedule_delay_defaults_to_one_minute(
    sleeps: SleepRecorder, batch_count: int
) -> None:
    """ fixes the default at one minute and leaves the concrete
value to `main.py`, so the constructor's own default is what a fetch failure
must be scheduled by when the caller names no delay at all.

Args:
sleeps: The `asyncio.sleep` recorder.
batch_count: How many URLs the batch holds, so the outcome is checked
per URL and not only once.
"""
    urls = [page_url(index) for index in range(batch_count)]
    harness = build(
        urls=urls, failures={url.get_url for url in urls}, batch_size=batch_count
    )
    # The worker is built with no reschedule_delay at all, so the constructor
    # default is the value under test rather than one this test chose.
    worker = CrawlerWorker(
        harness.repository,
        harness.reader,
        harness.fetcher,
        harness.link_extractor,
        harness.politeness_policy,
        harness.queuer,
        harness.retry_policy,
        batch_size=batch_count,
        time_provider=harness.time_provider,
        logger=real_logger)

    await drive(worker)

    repository = harness.repository()
    assert isinstance(repository, FakeRepository)
    assert repository.finished == [
        (url, NOW + DEFAULT_RESCHEDULE_DELAY) for url in urls
    ]
    assert DEFAULT_RESCHEDULE_DELAY == timedelta(minutes=1)

@pytest.mark.parametrize(
    "error",
    [
    pytest.param(sqlite3.OperationalError("database is locked"), id="sqlite"),
    pytest.param(RuntimeError("the write was lost"), id="a_non_sqlite_failure"),
    ])
async def test_a_completion_failure_aborts_the_batch_and_leaves_the_rows_started(
    sleeps: SleepRecorder,
    real_store: RealStore,
    caplog: pytest.LogCaptureFixture,
    error: BaseException) -> None:
    """The write that would record the outcome is the write that failed, so
there is nowhere else to record it: the batch is abandoned uncommitted and
its rows stay `started_crawl` until the `job_timeout` branch reclaims them. Proved against the real store, because the reclaiming
predicate is a SQL claim a fake could not model honestly.

Args:
sleeps: The `asyncio.sleep` recorder.
real_store: The real store and the file it writes.
caplog: pytest's log capture.
error: The error `complete_crawl` raises.
"""
    url = page_url(0)
    await real_store.repository.create_urls([url])
    harness = build(
        urls=[url],
        repository=FailingCompleteCrawlRepository(real_store.repository, error))

    # The failure is per batch, so the loop continues and only the scripted
    # stop ends it, exactly as the orchestrator would.
    with caplog.at_level(logging.ERROR, logger=LOGGER_NAME):
        await drive(harness.worker())

        assert harness.recorder.count("enqueue_urls") == 0
        assert harness.recorder.count("commit") == 0
        assert harness.reader.commit_sizes == []
        assert any(
            record.name == LOGGER_NAME and str(error) in record.getMessage
            for record in caplog.records
        )
        row = read_row(real_store.path, url.get_url)
        assert row is not None
        assert row["state"] == CrawlState.STARTED_CRAWL.value
        assert row["last_crawl_time"] == stamp(NOW)
        # Freshly started, so no claim can take it while this worker still owns it.
        assert await real_store.repository.get_crawlable_urls(
            NOW, 10, job_timeout=JOB_TIMEOUT, queue_timeout=QUEUE_TIMEOUT
        ) == []
        assert await real_store.repository.get_crawlable_urls(
            NOW + JOB_TIMEOUT, 10, job_timeout=JOB_TIMEOUT, queue_timeout=QUEUE_TIMEOUT
        ) == [url]

@pytest.mark.parametrize(
    "body",
    [
    pytest.param("<html><body>one link</body></html>", id="a_plain_body"),
    pytest.param(
    "<html><body><a href='/deep'>deep</a></body></html>",
    id="a_body_with_a_link"),
    ])
async def test_a_successful_crawl_stores_the_body_reports_it_and_queues_its_links(
    sleeps: SleepRecorder, caplog: pytest.LogCaptureFixture, body: str
) -> None:
    """`goal.md:1` and `goal.md:143` ask for the visited URL and its links to be
printed, and `goal.md:149` for those links to be queued; both happen per
page, through the logger and the queuer, before the messages are committed.

Args:
sleeps: The `asyncio.sleep` recorder.
caplog: pytest's log capture.
body: The body the fetcher serves for the first page.
"""
    urls = [page_url(0), page_url(1)]
    first_links = [page_url(2), page_url(3)]
    shared = page_url(4)
    second_links = [shared, page_url(5)]
    links_by_page = {
        urls[0].get_url: [*first_links, shared],
        urls[1].get_url: second_links,
    }
    harness = build(
        urls=urls, bodies={urls[0].get_url: body}, links_by_page=links_by_page
    )

    with caplog.at_level(logging.INFO, logger=LOGGER_NAME):
        await drive(harness.worker())

        visited = [
            record
            for record in caplog.records
            if record.name == LOGGER_NAME and "visited" in record.getMessage
        ]
        assert len(visited) == 2
        for page, links in links_by_page.items:
            reported = [record for record in visited if page in record.getMessage]
            assert len(reported) == 1
            assert all(link.get_url in reported[0].getMessage for link in links)
            queuer = harness.queuer()
            assert isinstance(queuer, FakeQueuer)
            # Exactly what the pages revealed, in batch order, duplicate included: the
            # extractor applied the host scope and the store inserts only when absent.
            assert queuer.enqueued == [([*first_links, shared, *second_links], None)]

@pytest.mark.parametrize(
    ("re_crawl_interval", "expected"),
    [
    pytest.param(None, None, id="no_interval_schedules_no_recrawl"),
    pytest.param(timedelta(0), NOW, id="a_zero_interval_still_schedules"),
    pytest.param(
    timedelta(minutes=30), NOW + timedelta(minutes=30), id="thirty_minutes"
    ),
    ])
async def test_a_success_is_due_again_after_the_configured_re_crawl_interval(
    sleeps: SleepRecorder, re_crawl_interval: timedelta | None, expected: datetime | None
) -> None:
    """A NULL `next_crawl_time` means no re-crawl, and a configured interval is
measured from the attempt, so a re-crawl is never scheduled in the past.

Args:
sleeps: The `asyncio.sleep` recorder.
re_crawl_interval: The interval to configure, or None.
expected: The instant the recorded outcome must carry, or None.
"""
    url = page_url(0)
    harness = build(urls=[url], re_crawl_interval=re_crawl_interval)

    await drive(harness.worker())

    repository = harness.repository()
    assert isinstance(repository, FakeRepository)
    assert repository.finished == [(url, expected)]

@pytest.mark.parametrize(
    ("idle_sleep_seconds", "expected_duration"),
    [
    pytest.param(0.25, 0.25, id="a_quarter_second"),
    pytest.param(1.0, 1.0, id="the_documented_default"),
    pytest.param(2.5, 2.5, id="two_and_a_half_seconds"),
    ])
async def test_an_empty_batch_waits_for_the_configured_idle_time(
    sleeps: SleepRecorder, idle_sleep_seconds: float, expected_duration: float
) -> None:
    """A drained partition is the loop's only backpressure, so the wait is the
configured one and the loop keeps polling rather than returning.

Args:
sleeps: The `asyncio.sleep` recorder.
idle_sleep_seconds: The idle wait to configure.
expected_duration: The wait the worker must request, in seconds.
"""
    harness = build(urls=[], idle_sleep_seconds=idle_sleep_seconds)

    await drive(harness.worker())

    assert sleeps.durations == [expected_duration]
    assert harness.recorder.count("peek") == 2
    assert harness.recorder.count("commit") == 0

@pytest.mark.parametrize("path", ["/first", "/second/third"])
async def test_mark_started_writes_the_started_state_and_both_timestamps(
    sleeps: SleepRecorder, real_store: RealStore, path: str
) -> None:
    """`last_crawl_time` and `last_status_update_time` are the only evidence a
crashed worker left, and the second is what makes the row reclaimable, so
both are asserted against the real store, snapshotted from inside the mark
because the completion transaction overwrites the state afterwards.

Args:
sleeps: The `asyncio.sleep` recorder.
real_store: The real store and the file it writes.
path: The page's path, so the row's identity is not fixed.
"""
    url = CustomURL(f"{SITE}{path}")
    await real_store.repository.create_urls([url])
    observer = MarkObservingRepository(real_store.repository, real_store.path)
    harness = build(urls=[url], repository=observer)

    await drive(harness.worker())

    assert observer.marks == [
        MarkSnapshot(
        state=CrawlState.STARTED_CRAWL.value,
        last_crawl_time=stamp(NOW),
        last_status_update_time=stamp(NOW),
        next_crawl_time=stamp(NOW))
    ]
    row = read_row(real_store.path, url.get_url)
    assert row is not None
    assert row["state"] == CrawlState.FINISHED_CRAWL.value

@pytest.mark.parametrize(
    ("wait_ms", "sleep_threshold_ms"),
    [
    pytest.param(1, 2_000, id="a_single_millisecond"),
    pytest.param(500, 2_000, id="half_the_default_threshold"),
    pytest.param(2_000, 2_000, id="exactly_at_the_threshold"),
    pytest.param(500, 500, id="a_threshold_of_its_own"),
    ])
async def test_a_wait_within_the_threshold_is_slept_through_and_the_crawl_proceeds(
    sleeps: SleepRecorder, wait_ms: int, sleep_threshold_ms: int
) -> None:
    """`goal.md:140` reads a number as "call after that many milliseconds", so
the worker waits exactly that long and then crawls, which is the point of a
threshold the configured wait is required to respect.

Args:
sleeps: The `asyncio.sleep` recorder, which must hold the one wait.
wait_ms: The politeness wait in milliseconds.
sleep_threshold_ms: The worker's threshold.
"""
    url = page_url(0)
    harness = build(
        urls=[url],
        wait_ms=wait_ms,
        sleep_threshold_ms=sleep_threshold_ms,
        links_by_page={url.get_url: [page_url(1)]})

    await drive(harness.worker())

    assert sleeps.durations == [wait_ms / 1000]
    assert harness.recorder.count("before_fetch") == 1
    assert harness.recorder.count("fetch") == 1
    repository = harness.repository()
    assert isinstance(repository, FakeRepository)
    assert repository.finished == [(url, None)]
    assert repository.discovered == [page_url(1)]
    queuer = harness.queuer()
    assert isinstance(queuer, FakeQueuer)
    assert queuer.enqueued == [([page_url(1)], None)]

@pytest.mark.parametrize("wait_ms", [0], ids=["zero_means_call_now"])
async def test_a_zero_wait_calls_without_sleeping(
    sleeps: SleepRecorder, wait_ms: int
) -> None:
    """`goal.md:140` reads 0 as "call now", so the no-op default must not
schedule a zero-length sleep per URL.

Args:
sleeps: The `asyncio.sleep` recorder, which must stay empty.
wait_ms: The politeness wait in milliseconds.
"""
    harness = build(urls=[page_url(0)], wait_ms=wait_ms)

    await drive(harness.worker())

    assert sleeps.durations == []
    assert harness.recorder.count("fetch") == 1

@pytest.mark.parametrize(
    ("wait_ms", "sleep_threshold_ms"),
    [
    pytest.param(2_001, 2_000, id="one_millisecond_over_the_default"),
    pytest.param(30_000, 2_000, id="half_a_minute_over_the_default"),
    pytest.param(10, 5, id="twice_a_small_threshold"),
    ])
async def test_a_wait_above_the_threshold_defers_the_url_instead_of_crawling_it(
    sleeps: SleepRecorder, wait_ms: int, sleep_threshold_ms: int
) -> None:
    """Sleeping a wait above the threshold would stall the whole batch, and
requeueing the URL would repeat that for ever; recording the wait as the
URL's own `next_crawl_time` is what keeps the crawl moving.

Args:
sleeps: The `asyncio.sleep` recorder, which must hold no wait at all.
wait_ms: The politeness wait in milliseconds.
sleep_threshold_ms: The worker's threshold.
"""
    url = page_url(0)
    harness = build(urls=[url], wait_ms=wait_ms, sleep_threshold_ms=sleep_threshold_ms)

    await drive(harness.worker())

    assert sleeps.durations == []
    assert harness.recorder.count("fetch") == 0
    assert harness.recorder.count("save") == 0
    repository = harness.repository()
    assert isinstance(repository, FakeRepository)
    assert repository.finished == [(url, NOW + timedelta(milliseconds=wait_ms))]
    assert repository.state_of(url) is CrawlState.FINISHED_CRAWL
    assert repository.discovered == []
    assert harness.recorder.count("commit") == 1

@pytest.mark.parametrize(
    ("wait_ms", "sleep_threshold_ms"),
    [
    pytest.param(2_001, 2_000, id="one_millisecond_over_the_default"),
    pytest.param(45_000, 2_000, id="forty_five_seconds"),
    ])
async def test_a_deferred_url_becomes_crawlable_once_its_wait_has_elapsed(
    sleeps: SleepRecorder,
    real_store: RealStore,
    wait_ms: int,
    sleep_threshold_ms: int) -> None:
    """The deferral has to be a real schedule and not a dropped URL: the row is
`finished_crawl` with the wait as its `next_crawl_time`, and the claim
predicate picks it up exactly when that instant passes (goal.md:79-81).

Args:
sleeps: The `asyncio.sleep` recorder.
real_store: The real store and the file it writes.
wait_ms: The politeness wait in milliseconds.
sleep_threshold_ms: The worker's threshold.
"""
    url = page_url(0)
    await real_store.repository.create_urls([url])
    harness = build(
        urls=[url],
        wait_ms=wait_ms,
        sleep_threshold_ms=sleep_threshold_ms,
        repository=real_store.repository)

    await drive(harness.worker())

    row = read_row(real_store.path, url.get_url)
    assert row is not None
    assert row["state"] == CrawlState.FINISHED_CRAWL.value
    assert row["next_crawl_time"] == stamp(NOW + timedelta(milliseconds=wait_ms))
    assert await real_store.repository.get_crawlable_urls(
        NOW, 10, job_timeout=JOB_TIMEOUT, queue_timeout=QUEUE_TIMEOUT
    ) == []
    assert await real_store.repository.get_crawlable_urls(
        NOW + timedelta(milliseconds=wait_ms),
        10,
        job_timeout=JOB_TIMEOUT,
        queue_timeout=QUEUE_TIMEOUT) == [url]

@pytest.mark.parametrize("batch_count", [2, 3, 6])
async def test_every_url_of_a_batch_is_in_flight_at_the_same_time(
    sleeps: SleepRecorder, batch_count: int
) -> None:
    """`goal.md:139` asks for a task group, and a gate proves it: a sequential
worker parks on the first fetch and never opens the gate, so the loop guard
fails the test rather than passing it late.

Args:
sleeps: The `asyncio.sleep` recorder.
batch_count: How many URLs must overlap.
"""
    urls = [page_url(index) for index in range(batch_count)]
    barrier = ArrivalBarrier(batch_count)
    harness = build(urls=urls, batch_size=batch_count, barrier=barrier)

    await drive(harness.worker())

    assert barrier.arrivals == batch_count
    assert barrier.peak == batch_count
    assert harness.recorder.count("fetch") == batch_count
    assert harness.recorder.count("complete_crawl") == 1
    assert harness.recorder.count("commit") == 1

@pytest.mark.parametrize(
    ("failing_collaborator", "error"),
    [
    pytest.param("extract", RuntimeError("malformed document"), id="the_extractor"),
    ])
async def test_one_uncaught_failure_aborts_the_whole_batch(
    sleeps: SleepRecorder, failing_collaborator: str, error: BaseException
) -> None:
    """A failure after the body was fetched must not be recorded as a completed
crawl, so it is not handled here: the task group aborts the batch and
neither the queue nor the messages are touched.

Args:
sleeps: The `asyncio.sleep` recorder.
failing_collaborator: Which collaborator fails, here `extract`.
error: The error it raises.
"""
    urls = [page_url(index) for index in range(3)]
    harness = build(
        urls=urls,
        batch_size=3,
        extract_error=error if failing_collaborator == "extract" else None)

    with pytest.raises(ExceptionGroup) as raised:
        await asyncio.wait_for(harness.worker.run, LOOP_GUARD_SECONDS)

        assert any(isinstance(found, type(error)) for found in flatten(raised.value))
        assert harness.recorder.count("complete_crawl") == 0
        assert harness.recorder.count("enqueue_urls") == 0
        assert harness.recorder.count("commit") == 0
        assert harness.reader.commit_sizes == []

@pytest.mark.parametrize("batch_count", [1, 2])
async def test_a_single_commit_per_batch_survives_a_producer_enqueueing_mid_batch(
    sleeps: SleepRecorder, batch_count: int
) -> None:
    """The commit is head-based, so a message appended while the batch is being
processed must be left for the next batch rather than committed away. The producer runs as its own task, as the poller's does.

Args:
sleeps: The `asyncio.sleep` recorder.
batch_count: How many URLs the first batch holds.
"""
    first = [page_url(index) for index in range(batch_count)]
    second = [page_url(index) for index in range(50, 50 + batch_count)]
    harness = build(urls=first, batch_size=batch_count, serve_batches=2)
    reader = harness.reader()
    fetcher = harness.fetcher()
    assert isinstance(reader, FakeReader)
    assert isinstance(fetcher, FakeFetcher)
    produced = False

    async def produce_during_the_first_batch(url: CustomURL) -> None:
        """Append the next batch from another task while the first is running.

        Args:
        url: The page being fetched, which only decides whether the append
        has already happened.
        """
        nonlocal produced
        if produced:
            return
            produced = True
            await asyncio.create_task(
                reader.produce([message_for(item) for item in second])
            )

    fetcher.set_on_fetch(produce_during_the_first_batch)

    await drive(harness.worker())

    # Two batches and two commits, and the concurrently appended messages are
    # the second batch: the head was acknowledged and the tail was left alone.
    assert reader.batches_served[0] == [message_for(item) for item in first]
    assert reader.batches_served[1] == [message_for(item) for item in second]
    assert reader.commit_sizes == [batch_count, batch_count]
    assert harness.recorder.count("commit") == 2
    assert not reader.messages

@pytest.mark.parametrize("batch_count", [1, 2, 3])
async def test_every_batch_is_served_and_committed_without_a_connect_step(
    sleeps: SleepRecorder, batch_count: int
) -> None:
    """The reader is wired by its constructor, so the loop goes straight to
peeking and every batch it is given is served and committed
(review.md items 14 and 16).

Args:
sleeps: The `asyncio.sleep` recorder.
batch_count: How many single-message batches the loop serves, so more
than one batch is proved to be served and committed.
"""
    urls = [page_url(index) for index in range(batch_count)]
    # batch_size=1 makes each message its own batch; a larger peek would drain
    # every message into the first batch and prove nothing about later ones.
    harness = build(urls=urls, batch_size=1, serve_batches=batch_count)

    await drive(harness.worker())

    assert harness.recorder.count("peek") == batch_count + 1
    assert harness.recorder.count("commit") == batch_count
    assert harness.reader.commit_sizes == [1] * batch_count
    assert not harness.reader.messages
