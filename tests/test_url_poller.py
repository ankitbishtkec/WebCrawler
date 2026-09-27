"""Tests for `URLPoller` ( the "Poller" bullet).

Every collaborator is a double: a stateful fake repository that models the
`goal.md:33-51` claim predicate so a skipped or re-claimed row is visible, a
recording producer that reports scripted overflows, a set-based dedupe (the
real request-id deduplicator), and a mutable clock. Nothing here
waits for real time: `asyncio.sleep` is replaced by a recorder, and `run` is
stopped by a repository that raises `asyncio.CancelledError` once its scripted
polls are served, which is exactly how the orchestrator stops the poller in
production. `drive` wraps the loop in `asyncio.wait_for`, so a loop that does
not terminate fails its test instead of hanging the session.

The lock discipline is tested from both directions: a poll held at a gated
claim while an API call parks at the lock, and an API call held at a gated
feed while the loop's first poll parks at the same lock. Two API calls against
each other prove the lock is held across the whole dedupe-claim-feed body, so
no two entry points can interleave.
"""

import asyncio
import logging
import sqlite3
from datetime import datetime, timedelta, timezone

import pytest

from webcrawler.application.url_poller import URLPoller
from webcrawler.domain.crawl_state import CrawlState
from webcrawler.domain.custom_url import CustomURL
from webcrawler.domain.messages import BaseMessage
from webcrawler.ports.request_deduplicator import RequestDeduplicator
from webcrawler.ports.time_provider import TimeProviderFactory
from webcrawler.ports.topic_producer import TopicProducer
from webcrawler.ports.url_state_repository import URLStateRepository
from webcrawler.utils.in_memory_request_id_deduplicator import (
    InMemoryRequestIdDeduplicator)

NOW = datetime(2026, 9, 26, 11, 28, 0, tzinfo=timezone.utc)
SITE = "http://crawlme.monzo.com"
LOGGER_NAME = "tests.webcrawler.url_poller"
JOB_TIMEOUT = timedelta(minutes=5)
QUEUE_TIMEOUT = timedelta(seconds=30)
# The loop guard: a `run` that has not stopped inside this budget fails its
# test instead of sticking the session.
LOOP_GUARD_SECONDS = 5.0
STOP_MESSAGE = "the scripted polls are exhausted"

def real_logger() -> logging.Logger:
    """Return the real logger the poller is given.

    Returns:
    logging.Logger: A named stdlib logger, so `caplog` can capture the
    records a rejected enqueue writes.
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

def url_with_hash_sign(sign: int) -> CustomURL:
    """Return the first page URL whose hash carries the wanted sign.

    Args:
    sign: -1 for a negative hash, 1 for a positive one. String hashing is
    seed-randomized, so the sign is found by probing rather than by
    naming a fixed page.

    Returns:
    CustomURL: A URL whose `hash(url)` is negative or positive as asked,
    so a routing assertion covers both sides of zero (goal.md:113).
    """
    index = 0
    while True:
        url = CustomURL(f"{SITE}/p{index}")
        if (hash(url) < 0) == (sign < 0):
            return url
            index += 1

            def message_for(url: CustomURL) -> BaseMessage:
                """Return the queue message the poller builds for a URL.

                Args:
                url: The URL the message carries.

                Returns:
                BaseMessage: The message routed on `hash(url)` (goal.md:113).
                """
                return BaseMessage(url, partition_key=hash(url))

            def claim_name(api: str) -> str:
                """Return the repository call an API entry point issues.

                Args:
                api: The entry point name, "enqueue_urls" or "queue_candidates".

                Returns:
                str: "claim_urls" for API (a), "claim_candidates" for API (b).
                """
                return "claim_urls" if api == "enqueue_urls" else "claim_candidates"

            async def call_api(
                poller: URLPoller,
                api: str,
                *,
                now: datetime = NOW,
                request_id: str | None = None) -> object:
                """Invoke one public API by name, with the arguments each one takes.

                Args:
                poller: The poller under test.
                api: The entry point name, "enqueue_urls" or "queue_candidates".
                now: The instant API (b) is given, as the caller owns it (goal.md:104).
                request_id: The dedupe key, or None to let the poller mint one.

                Returns:
                object: Whatever the API returned, so a test can assert it is None.
                """
                if api == "enqueue_urls":
                    return await poller.enqueue_urls([page_url(0)], request_id=request_id)
                    return await poller.queue_candidates(now, request_id=request_id)

                    async def drive(poller: URLPoller, *, timeout: float = LOOP_GUARD_SECONDS) -> None:
                        """Run a poller to its scripted stop, failing rather than hanging.

                        The repository raises `asyncio.CancelledError` once its scripted polls are
                        served, so the loop ends the way the orchestrator ends it in production;
                        the guard turns a loop that never ends into a `TimeoutError` failure.

                        Args:
                        poller: The poller to run.
                        timeout: How long the loop may take before the test fails.
                        """
                        with pytest.raises(asyncio.CancelledError):
                            await asyncio.wait_for(poller.run, timeout)

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
                                    name: The collaborator and method, as `"claim_candidates"`.
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

                                def details(self, name: str) -> list[object]:
                                    """Return the recorded details of every call with a name.

                                    Args:
                                    name: The call name to collect.

                                    Returns:
                                    list[object]: The detail of each recorded call with that name, in
                                    call order.
                                    """
                                    return [detail for recorded, detail in self.entries if recorded == name]

                                class SleepRecorder:
                                    """A stand-in for `asyncio.sleep` that records instead of waiting.

                                    Monkeypatched over `asyncio.sleep`, so a test asserts the length of every
                                    wait the poller asked for and the suite never spends real time in one. It
                                    optionally mirrors each wait into the shared `CallRecorder`, so the exact
                                    sequence test can interleave the sleeps with the claims and feeds.

                                        Attributes:
                                            durations: The waits requested, in request order, in seconds.
                                    """

                                    def __init__(self, recorder: CallRecorder | None = None) -> None:
                                        """Start with no recorded waits and no optional mirror.

                                        Args:
                                        recorder: The shared call log to mirror each wait into, or None to
                                        record only into `durations`.
                                        """
                                        self.durations: list[float] = []
                                        self._recorder = recorder

                                    async def __call__(self, delay: float) -> None:
                                        """Record one requested wait and return immediately.

                                        Args:
                                        delay: The seconds the poller asked to wait.
                                        """
                                        self.durations.append(delay)
                                        if self._recorder is not None:
                                            self._recorder.record("sleep", delay)

                                            class MutableClock(TimeProviderFactory):
                                                """A clock the test moves by hand, so a poll never waits for real time.

                                                Args:
                                                moment: The instant every call reports until the clock is advanced.
                                                """

                                                def __init__(self, moment: datetime = NOW) -> None:
                                                    """Freeze the clock at `moment`.

                                                    Args:
                                                    moment: The instant to report from now on.
                                                    """
                                                    self._moment = moment

                                                def now(self) -> datetime:
                                                    """Report the current instant.

                                                    Returns:
                                                    datetime: The instant the clock holds, in UTC.
                                                    """
                                                    return self._moment

                                                def advance(self, delta: timedelta) -> None:
                                                    """Move the clock forward.

                                                    Args:
                                                    delta: How far to move the held instant.
                                                    """
                                                    self._moment += delta

                                                class RowState:
                                                    """One URL row as the modelled store holds it.

                                                    Args:
                                                    state: The row's lifecycle state.
                                                    next_crawl_time: When the row is due again.
                                                    last_status_update_time: When the row's state last changed.
                                                    """

                                                    def __init__(
                                                        self,
                                                        state: CrawlState,
                                                        next_crawl_time: datetime,
                                                        last_status_update_time: datetime) -> None:
                                                        """Hold the three columns a claim reads.

                                                        Args:
                                                        state: The lifecycle state.
                                                        next_crawl_time: The due time.
                                                        last_status_update_time: The last state change.
                                                        """
                                                        self.state = state
                                                        self.next_crawl_time = next_crawl_time
                                                        self.last_status_update_time = last_status_update_time

                                                    class FakeRepository(URLStateRepository):
                                                        """A stateful model of the two claims the poller makes.

                                                        It models the `goal.md:33-51` predicate rather than the SQL, so a test can
                                                        see that a freshly queued row is left alone and a timed-out one is
                                                        re-claimed; the predicate itself is tested against the real SQLite store
                                                        elsewhere. `stop_after_polls` keeps `run` finite: once that many
                                                        candidates claims have been served, the next one raises
                                                        `asyncio.CancelledError`, exactly how the orchestrator stops a poller in
                                                        production. Only the candidates claim counts, because only the loop
                                                        issues it; the API (b) claim and the loop's claim are the same method, so
                                                        a mixed test stops the loop by cancelling the task instead.

                                                        Args:
                                                        recorder: The shared ordered call log.
                                                        stop_after_polls: How many claims are served before the loop is
                                                        stopped, or None to never stop.
                                                        transient_failures: How many attempts each claim fails with a
                                                        `sqlite3.OperationalError` before succeeding, modelling the
                                                        repository-internal retry the real store owns (goal.md:17).
                                                        claim_error: The error every claim raises, or None to claim normally.
                                                        gate: An event one scripted claim waits on, so a test holds a claim
                                                        in flight while another entry point tries the lock.
                                                        gate_poll: Which claim, counted from one, waits on the gate.
                                                        clock: The clock advanced once per served candidates claim.
                                                        clock_step: How far the clock moves after each candidates claim, once
                                                        that poll's `now` has already been read by the caller.
                                                        """

                                                        def __init__(
                                                            self,
                                                            recorder: CallRecorder,
                                                            *,
                                                            stop_after_polls: int | None = None,
                                                            transient_failures: int = 0,
                                                            claim_error: BaseException | None = None,
                                                            gate: asyncio.Event | None = None,
                                                            gate_poll: int = 1,
                                                            clock: MutableClock | None = None,
                                                            clock_step: timedelta | None = None) -> None:
                                                            """Take the script and start with no rows.

                                                            Args:
                                                            recorder: The shared ordered call log.
                                                            stop_after_polls: The poll budget before the loop is stopped.
                                                            transient_failures: The internal failures each claim survives.
                                                            claim_error: The error every claim raises, or None.
                                                            gate: The event one claim waits on, or None.
                                                            gate_poll: Which claim waits on the gate, counted from one.
                                                            clock: The clock to advance per candidates claim, or None.
                                                            clock_step: The advance applied after each candidates claim.
                                                            """
                                                            self._recorder = recorder
                                                            self._stop_after_polls = stop_after_polls
                                                            self._transient_failures = transient_failures
                                                            self._claim_error = claim_error
                                                            self._gate = gate
                                                            self._gate_poll = gate_poll
                                                            self._clock = clock
                                                            self._clock_step = clock_step
                                                            self._polls = 0
                                                            self.rows: dict[str, RowState] = {}
                                                            self.claim_attempts = 0
                                                            self.arrived = asyncio.Event

                                                        def seed(
                                                            self,
                                                            urls: list[CustomURL],
                                                            *,
                                                            state: CrawlState = CrawlState.NOT_CRAWLED,
                                                            next_crawl_time: datetime = NOW,
                                                            last_status_update_time: datetime = NOW) -> None:
                                                            """Insert rows for the URLs, as the store's insert would leave them.

                                                            Args:
                                                            urls: The URLs to hold rows for.
                                                            state: The lifecycle state every row starts in.
                                                            next_crawl_time: The due time every row starts with.
                                                            last_status_update_time: The state-change time every row starts
                                                            with, which is what the timeout branches measure against.
                                                            """
                                                            for url in urls:
                                                                self.rows[url.get_url] = RowState(
                                                                    state, next_crawl_time, last_status_update_time
                                                                    )

                                                                async def initialize(self) -> None:
                                                                    """Do nothing; the composition root initializes a store, not the poller."""

                                                                async def create_urls(self, urls: list[CustomURL]) -> None:
                                                                    """Insert the URLs as fresh `not_crawled` rows.

                                                                    Args:
                                                                    urls: The URLs to make known to the modelled store.
                                                                    """
                                                                    self.seed(urls)

                                                                async def close(self) -> None:
                                                                    """Do nothing; the orchestrator owns the store's lifetime."""

                                                                async def get_crawlable_urls(
                                                                    self,
                                                                    now: datetime,
                                                                    max_items: int,
                                                                    *,
                                                                    job_timeout: timedelta,
                                                                    queue_timeout: timedelta) -> list[CustomURL]:
                                                                    """Return nothing; the poller claims, it does not preview.

                                                                    Args:
                                                                    now: The instant the predicate would be evaluated at.
                                                                    max_items: The row limit.
                                                                    job_timeout: The `started_crawl` staleness timeout.
                                                                    queue_timeout: The `queued` staleness timeout.

                                                                    Returns:
                                                                    list[CustomURL]: Always empty, because this double models only the
                                                                    claims the poller makes.
                                                                    """
                                                                    return []

                                                                async def claim_candidates(
                                                                    self,
                                                                    now: datetime,
                                                                    max_items: int,
                                                                    *,
                                                                    job_timeout: timedelta,
                                                                    queue_timeout: timedelta) -> list[CustomURL]:
                                                                    """Claim eligible rows, unrestricted to any caller's list.

                                                                    Args:
                                                                    now: The instant the eligibility predicates are evaluated against.
                                                                    max_items: The row limit. `-1` means no limit.
                                                                    job_timeout: The `started_crawl` staleness timeout.
                                                                    queue_timeout: The `queued` staleness timeout.

                                                                    Returns:
                                                                    list[CustomURL]: The rows this claim moved to `queued`.

                                                                    Raises:
                                                                    asyncio.CancelledError: Once the scripted polls are served, which
                                                                    is the only thing that ends the poller's loop in a test.
                                                                    """
                                                                    self._recorder.record(
                                                                        "claim_candidates", (now, max_items, job_timeout, queue_timeout)
                                                                        )
                                                                    self._polls += 1
                                                                    if self._gate is not None and self._polls == self._gate_poll:
                                                                        self.arrived.set()
                                                                        await self._gate.wait()
                                                                        if self._stop_after_polls is not None and self._polls > self._stop_after_polls:
                                                                            raise asyncio.CancelledError(STOP_MESSAGE)
                                                                            # The clock moves only on a poll's claim, after its `now` was read by
                                                                            # the caller, so later polls are one step later without waiting.
                                                                            if self._clock is not None:
                                                                                self._clock.advance(self._clock_step())
                                                                                return self._claim(None, now, max_items, job_timeout, queue_timeout)

                                                                                async def claim_urls(
                                                                                    self,
                                                                                    urls: list[CustomURL],
                                                                                    now: datetime,
                                                                                    max_items: int = -1,
                                                                                    *,
                                                                                    job_timeout: timedelta,
                                                                                    queue_timeout: timedelta) -> list[CustomURL]:
                                                                                    """Claim eligible rows, restricted to the caller's own URLs.

                                                                                    Args:
                                                                                    urls: The URLs the caller wants queued.
                                                                                    now: The instant the eligibility predicates are evaluated against.
                                                                                    max_items: The row limit. `-1` means no limit.
                                                                                    job_timeout: The `started_crawl` staleness timeout.
                                                                                    queue_timeout: The `queued` staleness timeout.

                                                                                    Returns:
                                                                                    list[CustomURL]: The subset of `urls` this claim moved to `queued`.
                                                                                    """
                                                                                    self._recorder.record(
                                                                                        "claim_urls",
                                                                                        (
                                                                                        tuple(url.get_url for url in urls),
                                                                                        now,
                                                                                        max_items,
                                                                                        job_timeout,
                                                                                        queue_timeout))
                                                                                    return self._claim(urls, now, max_items, job_timeout, queue_timeout)

                                                                                async def mark_started(self, url: CustomURL, now: datetime) -> None:
                                                                                    """Record the call; the poller never marks a row started.

                                                                                    Args:
                                                                                    url: The URL being crawled.
                                                                                    now: The attempt instant.
                                                                                    """
                                                                                    self._recorder.record("mark_started", url.get_url)

                                                                                async def complete_crawl(
                                                                                    self,
                                                                                    finished: list[tuple[CustomURL, datetime | None]],
                                                                                    discovered: list[CustomURL],
                                                                                    now: datetime) -> None:
                                                                                    """Record the call; the poller never completes a crawl.

                                                                                    Args:
                                                                                    finished: The outcomes the worker would record.
                                                                                    discovered: The URLs the worker would insert.
                                                                                    now: The completion instant.
                                                                                    """
                                                                                    self._recorder.record("complete_crawl", now)

                                                                                def _claim(
                                                                                    self,
                                                                                    urls: list[CustomURL] | None,
                                                                                    now: datetime,
                                                                                    max_items: int,
                                                                                    job_timeout: timedelta,
                                                                                    queue_timeout: timedelta) -> list[CustomURL]:
                                                                                    """Retry the claim internally, then apply it once, as one call.

                                                                                    The retry loop models the retry policy the real store owns: a
                                                                                    transient error is survived inside this one call, so the poller sees
                                                                                    one claim that eventually succeeded (goal.md:17).

                                                                                    Args:
                                                                                    urls: The caller's URLs, or None to claim candidates.
                                                                                    now: The instant the eligibility predicates are evaluated against.
                                                                                    max_items: The row limit. `-1` means no limit.
                                                                                    job_timeout: The `started_crawl` staleness timeout.
                                                                                    queue_timeout: The `queued` staleness timeout.

                                                                                    Returns:
                                                                                    list[CustomURL]: The rows this claim moved to `queued`.

                                                                                    Raises:
                                                                                    sqlite3.OperationalError: If a transient failure outlasts the
                                                                                    modelled retries.
                                                                                    BaseException: Whatever `claim_error` was scripted with.
                                                                                    """
                                                                                    while True:
                                                                                        self.claim_attempts += 1
                                                                                        try:
                                                                                            return self._apply(urls, now, max_items, job_timeout, queue_timeout)
                                                                                        except sqlite3.OperationalError:
                                                                                            if self.claim_attempts <= self._transient_failures:
                                                                                                continue
                                                                                                raise

                                                                                                def _apply(
                                                                                                    self,
                                                                                                    urls: list[CustomURL] | None,
                                                                                                    now: datetime,
                                                                                                    max_items: int,
                                                                                                    job_timeout: timedelta,
                                                                                                    queue_timeout: timedelta) -> list[CustomURL]:
                                                                                                    """Select, limit, and move the eligible rows to `queued`.

                                                                                                    Args:
                                                                                                    urls: The caller's URLs, or None to claim candidates.
                                                                                                    now: The instant the eligibility predicates are evaluated against.
                                                                                                    max_items: The row limit. `-1` means no limit.
                                                                                                    job_timeout: The `started_crawl` staleness timeout.
                                                                                                    queue_timeout: The `queued` staleness timeout.

                                                                                                    Returns:
                                                                                                    list[CustomURL]: The rows this application moved to `queued`.

                                                                                                    Raises:
                                                                                                    sqlite3.OperationalError: While the modelled transient failures are
                                                                                                    not yet spent.
                                                                                                    BaseException: Whatever `claim_error` was scripted with.
                                                                                                    """
                                                                                                    if self._claim_error is not None:
                                                                                                        raise self._claim_error
                                                                                                        if self.claim_attempts <= self._transient_failures:
                                                                                                            raise sqlite3.OperationalError("database is locked")
                                                                                                            if urls is not None and not urls:
                                                                                                                return []
                                                                                                                wanted = None if urls is None else {url.get_url for url in urls}
                                                                                                                matches = sorted(
                                                                                                                    (row.next_crawl_time, url_text)
                                                                                                                    for url_text, row in self.rows.items
                                                                                                                    if (wanted is None or url_text in wanted)
                                                                                                                    and self._eligible(row, now, job_timeout, queue_timeout)
                                                                                                                    )
                                                                                                                if max_items >= 0:
                                                                                                                    matches = matches[:max_items]
                                                                                                                    claimed: list[CustomURL] = []
                                                                                                                    for _, url_text in matches:
                                                                                                                        row = self.rows[url_text]
                                                                                                                        row.state = CrawlState.QUEUED
                                                                                                                        row.last_status_update_time = now
                                                                                                                        claimed.append(CustomURL(url_text))
                                                                                                                        return claimed

                                                                                                                        @staticmethod
                                                                                                                        def _eligible(
                                                                                                                            row: RowState,
                                                                                                                            now: datetime,
                                                                                                                            job_timeout: timedelta,
                                                                                                                            queue_timeout: timedelta) -> bool:
                                                                                                                            """Apply the `goal.md:33-51` predicate to one row.

                                                                                                                            Args:
                                                                                                                            row: The row being judged.
                                                                                                                            now: The instant the predicates are evaluated against.
                                                                                                                            job_timeout: The `started_crawl` staleness timeout.
                                                                                                                            queue_timeout: The `queued` staleness timeout.

                                                                                                                            Returns:
                                                                                                                            bool: True when the row is claimable at `now`.
                                                                                                                            """
                                                                                                                            if row.state is CrawlState.NOT_CRAWLED:
                                                                                                                                return True
                                                                                                                                if row.state is CrawlState.FINISHED_CRAWL:
                                                                                                                                    return row.next_crawl_time <= now
                                                                                                                                    if row.state is CrawlState.STARTED_CRAWL:
                                                                                                                                        return now - row.last_status_update_time >= job_timeout
                                                                                                                                        return now - row.last_status_update_time >= queue_timeout

                                                                                                                                        class FakeProducer(TopicProducer):
                                                                                                                                            """A recording producer whose bulk call reports scripted overflows.

                                                                                                                                            Args:
                                                                                                                                            recorder: The shared ordered call log.
                                                                                                                                            overflow: The canonical URLs whose bulk results are `False`, which is
                                                                                                                                            how a full partition is reported to the poller.
                                                                                                                                            gate: An event the first bulk call waits on, so a test holds a feed in
                                                                                                                                            flight while another entry point tries the lock.
                                                                                                                                            """

                                                                                                                                            def __init__(
                                                                                                                                                self,
                                                                                                                                                recorder: CallRecorder,
                                                                                                                                                *,
                                                                                                                                                overflow: set[str] | None = None,
                                                                                                                                                gate: asyncio.Event | None = None) -> None:
                                                                                                                                                """Take the overflow script and the optional gate, with no batches.

                                                                                                                                                Args:
                                                                                                                                                recorder: The shared ordered call log.
                                                                                                                                                overflow: The canonical URLs to report as rejected.
                                                                                                                                                gate: The event the first bulk call waits on, or None.
                                                                                                                                                """
                                                                                                                                                self._recorder = recorder
                                                                                                                                                self._overflow = set if overflow is None else overflow
                                                                                                                                                self._gate = gate
                                                                                                                                                self._gate_used = False
                                                                                                                                                self.batches: list[list[BaseMessage]] = []
                                                                                                                                                self.arrived = asyncio.Event

                                                                                                                                            async def connect(self) -> None:
                                                                                                                                                """Do nothing; the composition root connects the producer, not the poller."""

                                                                                                                                            async def enqueue(self, message: BaseMessage) -> None:
                                                                                                                                                """Record the single-message call the poller must never make.

                                                                                                                                                Args:
                                                                                                                                                message: The message a wrong implementation would have sent.
                                                                                                                                                """
                                                                                                                                                self._recorder.record("enqueue", message.url.get_url)

                                                                                                                                            async def enqueue_many(self, messages: list[BaseMessage]) -> list[bool]:
                                                                                                                                                """Record one bulk call and report one result per message.

                                                                                                                                                Args:
                                                                                                                                                messages: The batch the poller is feeding, in the order it claimed.

                                                                                                                                                Returns:
                                                                                                                                                list[bool]: One result per input message; False for a URL in the
                                                                                                                                                overflow script, True otherwise.

                                                                                                                                                Raises:
                                                                                                                                                asyncio.CancelledError: Never; the gate only delays this call, so
                                                                                                                                                the in-flight assertions observe a held lock, not a failure.
                                                                                                                                                """
                                                                                                                                                self._recorder.record(
                                                                                                                                                    "enqueue_many", tuple(message.url.get_url for message in messages)
                                                                                                                                                    )
                                                                                                                                                if self._gate is not None and not self._gate_used:
                                                                                                                                                    self._gate_used = True
                                                                                                                                                    self.arrived.set()
                                                                                                                                                    await self._gate.wait()
                                                                                                                                                    self.batches.append(list(messages))
                                                                                                                                                    return [message.url.get_url not in self._overflow for message in messages]

                                                                                                                                                    class FakeDedupe(RequestDeduplicator):
                                                                                                                                                        """A set-based dedupe that records every id it was asked about.

                                                                                                                                                        Args:
                                                                                                                                                        recorder: The shared ordered call log, mirrored per gate hit.
                                                                                                                                                        """

                                                                                                                                                        def __init__(self, recorder: CallRecorder) -> None:
                                                                                                                                                            """Start with no ids seen.

                                                                                                                                                            Args:
                                                                                                                                                            recorder: The shared ordered call log.
                                                                                                                                                            """
                                                                                                                                                            self._recorder = recorder
                                                                                                                                                            self._seen: set[str] = set
                                                                                                                                                            self.calls: list[str] = []

                                                                                                                                                        def seen_and_record(self, request_id: str) -> bool:
                                                                                                                                                            """Record one id and report whether the caller must skip its request.

                                                                                                                                                            Args:
                                                                                                                                                            request_id: The poll-check or API request id.

                                                                                                                                                            Returns:
                                                                                                                                                            bool: True when the id was already present, meaning the caller
                                                                                                                                                            must skip; False when it was new and is now recorded.
                                                                                                                                                            """
                                                                                                                                                            self._recorder.record("seen_and_record", request_id)
                                                                                                                                                            self.calls.append(request_id)
                                                                                                                                                            if request_id in self._seen:
                                                                                                                                                                return True
                                                                                                                                                                self._seen.add(request_id)
                                                                                                                                                                return False

                                                                                                                                                                class Harness:
                                                                                                                                                                    """The doubles under test and the poller wired from them.

                                                                                                                                                                    Every collaborator is reachable by name, so an assertion reads as the
                                                                                                                                                                    behaviour it is about, and the doubles share one `CallRecorder`, so the
                                                                                                                                                                    order assertions are about a single sequence.

                                                                                                                                                                    Args:
                                                                                                                                                                    recorder: The shared ordered call log.
                                                                                                                                                                    repository: The stateful model of the store.
                                                                                                                                                                    producer: The recording producer.
                                                                                                                                                                    dedupe: The dedupe the poller was given.
                                                                                                                                                                    time_provider: The clock the poller was given.
                                                                                                                                                                    poller: The poller under test.
                                                                                                                                                                    """

                                                                                                                                                                    def __init__(
                                                                                                                                                                        self,
                                                                                                                                                                        *,
                                                                                                                                                                        recorder: CallRecorder,
                                                                                                                                                                        repository: FakeRepository,
                                                                                                                                                                        producer: FakeProducer,
                                                                                                                                                                        dedupe: RequestDeduplicator,
                                                                                                                                                                        time_provider: TimeProviderFactory,
                                                                                                                                                                        poller: URLPoller) -> None:
                                                                                                                                                                        """Hold the doubles and the poller, naming only ports (goal.md:16).

                                                                                                                                                                        Args:
                                                                                                                                                                        recorder: The shared ordered call log.
                                                                                                                                                                        repository: The stateful model of the store.
                                                                                                                                                                        producer: The recording producer.
                                                                                                                                                                        dedupe: The dedupe the poller was given.
                                                                                                                                                                        time_provider: The clock the poller was given.
                                                                                                                                                                        poller: The poller under test.
                                                                                                                                                                        """
                                                                                                                                                                        self.recorder = recorder
                                                                                                                                                                        self.repository = repository
                                                                                                                                                                        self.producer = producer
                                                                                                                                                                        self.dedupe = dedupe
                                                                                                                                                                        self.time_provider = time_provider
                                                                                                                                                                        self.poller = poller

                                                                                                                                                                    def build(
                                                                                                                                                                        *,
                                                                                                                                                                        urls: list[CustomURL] | None = None,
                                                                                                                                                                        stop_after_polls: int | None = None,
                                                                                                                                                                        transient_failures: int = 0,
                                                                                                                                                                        claim_error: BaseException | None = None,
                                                                                                                                                                        gate: asyncio.Event | None = None,
                                                                                                                                                                        gate_poll: int = 1,
                                                                                                                                                                        clock_step: timedelta | None = None,
                                                                                                                                                                        overflow: set[str] | None = None,
                                                                                                                                                                        producer_gate: asyncio.Event | None = None,
                                                                                                                                                                        periodic_fetch_seconds: float = 5.0,
                                                                                                                                                                        periodic_max_items: int = -1,
                                                                                                                                                                        max_items_to_queue: int | None = None,
                                                                                                                                                                        job_timeout: timedelta = JOB_TIMEOUT,
                                                                                                                                                                        queue_timeout: timedelta = QUEUE_TIMEOUT,
                                                                                                                                                                        dedupe: RequestDeduplicator | None = None,
                                                                                                                                                                        time_provider: TimeProviderFactory | None = None) -> Harness:
                                                                                                                                                                        """Wire one poller from doubles, seeded with the URLs given.

                                                                                                                                                                        Args:
                                                                                                                                                                        urls: The URLs to seed as fresh `not_crawled` rows.
                                                                                                                                                                        stop_after_polls: The poll budget before a claim stops the loop.
                                                                                                                                                                        transient_failures: The internal failures each claim survives.
                                                                                                                                                                        claim_error: The error every claim raises, or None.
                                                                                                                                                                        gate: The event one scripted repository claim waits on.
                                                                                                                                                                        gate_poll: Which claim waits on the repository gate, from one.
                                                                                                                                                                        clock_step: The advance applied after each candidates claim, driving
                                                                                                                                                                        each later poll one step forward in time.
                                                                                                                                                                        overflow: The canonical URLs the bulk feed reports as rejected.
                                                                                                                                                                        producer_gate: The event the first bulk feed waits on.
                                                                                                                                                                        periodic_fetch_seconds: The poll interval to construct with.
                                                                                                                                                                        periodic_max_items: The periodic row limit to construct with.
                                                                                                                                                                        max_items_to_queue: The fallback row limit to construct with, or None
                                                                                                                                                                        to construct with the `-1` default (goal.md:59).
                                                                                                                                                                        job_timeout: The `started_crawl` staleness timeout to construct with.
                                                                                                                                                                        queue_timeout: The `queued` staleness timeout to construct with.
                                                                                                                                                                        dedupe: The dedupe to inject, or None for the recording set fake.
                                                                                                                                                                        time_provider: The clock to inject, or None for a mutable one.

                                                                                                                                                                        Returns:
                                                                                                                                                                        Harness: The doubles and the poller wired from them, with the seeded
                                                                                                                                                                        rows in place.
                                                                                                                                                                        """
                                                                                                                                                                        recorder = CallRecorder()
                                                                                                                                                                        clock = MutableClock if time_provider is None else time_provider
                                                                                                                                                                        repository = FakeRepository(
                                                                                                                                                                            recorder,
                                                                                                                                                                            stop_after_polls=stop_after_polls,
                                                                                                                                                                            transient_failures=transient_failures,
                                                                                                                                                                            claim_error=claim_error,
                                                                                                                                                                            gate=gate,
                                                                                                                                                                            gate_poll=gate_poll,
                                                                                                                                                                            clock=clock if clock_step is not None else None,
                                                                                                                                                                            clock_step=clock_step)
                                                                                                                                                                        producer = FakeProducer(recorder, overflow=overflow, gate=producer_gate)
                                                                                                                                                                        resolved_dedupe = FakeDedupe(recorder) if dedupe is None else dedupe
                                                                                                                                                                        poller = URLPoller(
                                                                                                                                                                            repository,
                                                                                                                                                                            producer,
                                                                                                                                                                            periodic_fetch_seconds=periodic_fetch_seconds,
                                                                                                                                                                            periodic_max_items=periodic_max_items,
                                                                                                                                                                            max_items_to_queue=-1 if max_items_to_queue is None else max_items_to_queue,
                                                                                                                                                                            job_timeout=job_timeout,
                                                                                                                                                                            queue_timeout=queue_timeout,
                                                                                                                                                                            dedupe=resolved_dedupe,
                                                                                                                                                                            time_provider=clock,
                                                                                                                                                                            logger=real_logger)
                                                                                                                                                                        if urls:
                                                                                                                                                                            repository.seed(urls)
                                                                                                                                                                            return Harness(
                                                                                                                                                                                recorder=recorder,
                                                                                                                                                                                repository=repository,
                                                                                                                                                                                producer=producer,
                                                                                                                                                                                dedupe=resolved_dedupe,
                                                                                                                                                                                time_provider=clock,
                                                                                                                                                                                poller=poller)

                                                                                                                                                                            @pytest.mark.parametrize(
                                                                                                                                                                                "api",
                                                                                                                                                                                [
                                                                                                                                                                                pytest.param("enqueue_urls", id="api_a_enqueue_urls"),
                                                                                                                                                                                pytest.param("queue_candidates", id="api_b_queue_candidates"),
                                                                                                                                                                                ])
                                                                                                                                                                            async def test_both_public_apis_return_none(api: str) -> None:
                                                                                                                                                                                """Both APIs report success by returning `None`, never a partial result.

                                                                                                                                                                                Args:
                                                                                                                                                                                api: The entry point to invoke.
                                                                                                                                                                                """
                                                                                                                                                                                harness = build(urls=[page_url(0)])

                                                                                                                                                                                assert await call_api(harness.poller, api) is None

                                                                                                                                                                            @pytest.mark.parametrize(
                                                                                                                                                                                "url_count",
                                                                                                                                                                                [
                                                                                                                                                                                pytest.param(1, id="one_url"),
                                                                                                                                                                                pytest.param(3, id="three_urls"),
                                                                                                                                                                                ])
                                                                                                                                                                            async def test_enqueue_urls_claims_exactly_the_callers_urls_with_no_limit(
                                                                                                                                                                                url_count: int) -> None:
                                                                                                                                                                                """API (a) passes the caller's own list to the claim with `max_items=-1`,
                                                                                                                                                                                because the caller's list is already the batch (goal.md:103),
                                                                                                                                                                                and it takes its `now` from the injected clock, not from a second source.

                                                                                                                                                                                Args:
                                                                                                                                                                                url_count: How many URLs the caller queues at once.
                                                                                                                                                                                """
                                                                                                                                                                                urls = [page_url(index) for index in range(url_count)]
                                                                                                                                                                                harness = build(urls=urls)

                                                                                                                                                                                await harness.poller.enqueue_urls(urls)

                                                                                                                                                                                assert harness.recorder.details("claim_urls") == [
                                                                                                                                                                                    (
                                                                                                                                                                                    tuple(url.get_url for url in urls),
                                                                                                                                                                                    NOW,
                                                                                                                                                                                    -1,
                                                                                                                                                                                    JOB_TIMEOUT,
                                                                                                                                                                                    QUEUE_TIMEOUT)
                                                                                                                                                                                    ]
                                                                                                                                                                                assert harness.recorder.names == [
                                                                                                                                                                                    "seen_and_record",
                                                                                                                                                                                    "claim_urls",
                                                                                                                                                                                    "enqueue_many",
                                                                                                                                                                                    ]
                                                                                                                                                                                assert harness.producer.batches == [[message_for(url) for url in urls]]

                                                                                                                                                                            @pytest.mark.parametrize(
                                                                                                                                                                                "eligible_count",
                                                                                                                                                                                [
                                                                                                                                                                                pytest.param(1, id="one_eligible_of_three"),
                                                                                                                                                                                pytest.param(2, id="two_eligible_of_three"),
                                                                                                                                                                                ])
                                                                                                                                                                            async def test_only_the_rows_the_claim_returns_reach_one_bulk_call(
                                                                                                                                                                                eligible_count: int) -> None:
                                                                                                                                                                                """Only the claimed subset of the caller's URLs is fed, through exactly one
                                                                                                                                                                                `enqueue_many`, because the store is the only judge of eligibility
                                                                                                                                                                                (goal.md:61).

                                                                                                                                                                                Args:
                                                                                                                                                                                eligible_count: How many of the three seeded rows are claimable; the
                                                                                                                                                                                rest are freshly `queued`, so the claim must leave them alone.
                                                                                                                                                                                """
                                                                                                                                                                                urls = [page_url(index) for index in range(3)]
                                                                                                                                                                                harness = build(urls=[])
                                                                                                                                                                                harness.repository.seed(urls[:eligible_count])
                                                                                                                                                                                harness.repository.seed(
                                                                                                                                                                                    urls[eligible_count:], state=CrawlState.QUEUED, last_status_update_time=NOW
                                                                                                                                                                                    )

                                                                                                                                                                                await harness.poller.enqueue_urls(urls)

                                                                                                                                                                                assert len(harness.producer.batches) == 1
                                                                                                                                                                                assert harness.producer.batches[0] == [
                                                                                                                                                                                    message_for(url) for url in urls[:eligible_count]
                                                                                                                                                                                    ]
                                                                                                                                                                                assert harness.recorder.count("enqueue_many") == 1

                                                                                                                                                                            @pytest.mark.parametrize(
                                                                                                                                                                                ("configured", "passed", "expected"),
                                                                                                                                                                                [
                                                                                                                                                                                pytest.param(None, None, -1, id="the_default_constructor_value_is_no_limit"),
                                                                                                                                                                                pytest.param(7, None, 7, id="the_configured_value_is_the_fallback"),
                                                                                                                                                                                pytest.param(None, 5, 5, id="an_explicit_limit_does_not_fall_back"),
                                                                                                                                                                                pytest.param(7, 3, 3, id="an_explicit_limit_beats_the_configured_value"),
                                                                                                                                                                                ])
                                                                                                                                                                            async def test_queue_candidates_resolves_the_max_items_fallback(
                                                                                                                                                                                configured: int | None, passed: int | None, expected: int
                                                                                                                                                                                ) -> None:
                                                                                                                                                                                """API (b) falls back to `max_items_to_queue` only when its own limit is
                                                                                                                                                                                None, and that fallback itself defaults to `-1`, no limit (goal.md:104,
                                                                                                                                                                                goal.md:59); its `now` is the caller's, taken as given.

                                                                                                                                                                                Args:
                                                                                                                                                                                configured: The constructor's `max_items_to_queue`, or None to exercise
                                                                                                                                                                                the `-1` default.
                                                                                                                                                                                passed: The `max_items` the API call carries, None to trigger the
                                                                                                                                                                                fallback.
                                                                                                                                                                                expected: The row limit the claim must receive.
                                                                                                                                                                                """
                                                                                                                                                                                harness = build(urls=[page_url(0)], max_items_to_queue=configured)

                                                                                                                                                                                await harness.poller.queue_candidates(NOW, max_items=passed)

                                                                                                                                                                                assert harness.recorder.details("claim_candidates") == [
                                                                                                                                                                                    (NOW, expected, JOB_TIMEOUT, QUEUE_TIMEOUT)
                                                                                                                                                                                    ]

                                                                                                                                                                            @pytest.mark.parametrize(
                                                                                                                                                                                ("api", "error"),
                                                                                                                                                                                [
                                                                                                                                                                                pytest.param(
                                                                                                                                                                                "enqueue_urls",
                                                                                                                                                                                RuntimeError("the store is down"),
                                                                                                                                                                                id="api_a_with_a_store_failure"),
                                                                                                                                                                                pytest.param(
                                                                                                                                                                                "enqueue_urls",
                                                                                                                                                                                sqlite3.OperationalError("database is locked"),
                                                                                                                                                                                id="api_a_with_a_lock_failure"),
                                                                                                                                                                                pytest.param(
                                                                                                                                                                                "queue_candidates",
                                                                                                                                                                                RuntimeError("the store is down"),
                                                                                                                                                                                id="api_b_with_a_store_failure"),
                                                                                                                                                                                pytest.param(
                                                                                                                                                                                "queue_candidates",
                                                                                                                                                                                sqlite3.OperationalError("database is locked"),
                                                                                                                                                                                id="api_b_with_a_lock_failure"),
                                                                                                                                                                                ])
                                                                                                                                                                            async def test_both_apis_raise_when_the_claim_raises(api: str, error: Exception) -> None:
                                                                                                                                                                                """A failed claim propagates unchanged and nothing is fed, because a
                                                                                                                                                                                partial queue is not reported as success.

                                                                                                                                                                                Args:
                                                                                                                                                                                api: The entry point to invoke.
                                                                                                                                                                                error: The error the claim raises.
                                                                                                                                                                                """
                                                                                                                                                                                harness = build(urls=[page_url(0)], claim_error=error)

                                                                                                                                                                                with pytest.raises(type(error)) as raised:
                                                                                                                                                                                    await call_api(harness.poller, api)

                                                                                                                                                                                    assert raised.value is error
                                                                                                                                                                                    assert harness.producer.batches == []

                                                                                                                                                                                    @pytest.mark.parametrize(
                                                                                                                                                                                        "queue_timeout",
                                                                                                                                                                                        [
                                                                                                                                                                                        pytest.param(timedelta(seconds=30), id="a_30s_queue_timeout"),
                                                                                                                                                                                        pytest.param(timedelta(minutes=2), id="a_2min_queue_timeout"),
                                                                                                                                                                                        ])
                                                                                                                                                                                    async def test_a_rejected_enqueue_is_logged_and_left_for_the_first_poll_after_queue_timeout(
                                                                                                                                                                                        monkeypatch: pytest.MonkeyPatch,
                                                                                                                                                                                        caplog: pytest.LogCaptureFixture,
                                                                                                                                                                                        queue_timeout: timedelta) -> None:
                                                                                                                                                                                        """A `False` bulk result is logged and not raised: the row is already
                                                                                                                                                                                        `queued` with a fresh `last_status_update_time`, so the next poll claims
                                                                                                                                                                                        nothing and only the first poll at or after `queue_timeout` re-claims it
                                                                                                                                                                                        ( goal.md:63-65).

                                                                                                                                                                                        Args:
                                                                                                                                                                                        monkeypatch: pytest's patcher, undone after the test.
                                                                                                                                                                                        caplog: pytest's log capture.
                                                                                                                                                                                        queue_timeout: The `queued` staleness timeout the poller is built
                                                                                                                                                                                        with, which is also how far the clock moves per poll.
                                                                                                                                                                                        """
                                                                                                                                                                                        url = page_url(0)
                                                                                                                                                                                        harness = build(
                                                                                                                                                                                            urls=[url],
                                                                                                                                                                                            overflow={url.get_url},
                                                                                                                                                                                            queue_timeout=queue_timeout,
                                                                                                                                                                                            clock_step=queue_timeout,
                                                                                                                                                                                            stop_after_polls=2)
                                                                                                                                                                                        monkeypatch.setattr(asyncio, "sleep", SleepRecorder)

                                                                                                                                                                                        with caplog.at_level(logging.INFO, logger=LOGGER_NAME):
                                                                                                                                                                                            assert await harness.poller.enqueue_urls([url]) is None
                                                                                                                                                                                            await drive(harness.poller())

                                                                                                                                                                                            rejected = [
                                                                                                                                                                                                record for record in caplog.records if url.get_url in record.getMessage
                                                                                                                                                                                                ]
                                                                                                                                                                                            assert rejected
                                                                                                                                                                                            assert harness.repository.rows[url.get_url].state is CrawlState.QUEUED
                                                                                                                                                                                            # The API call fed one row, the fresh poll claimed nothing, and the poll at
                                                                                                                                                                                            # the timeout re-claimed it: the queue grew once per genuine claim only.
                                                                                                                                                                                            assert [len(batch) for batch in harness.producer.batches] == [1, 0, 1]

                                                                                                                                                                                            @pytest.mark.parametrize(
                                                                                                                                                                                                "sign",
                                                                                                                                                                                                [
                                                                                                                                                                                                pytest.param(-1, id="a_negative_hash"),
                                                                                                                                                                                                pytest.param(1, id="a_positive_hash"),
                                                                                                                                                                                                ])
                                                                                                                                                                                            async def test_every_message_is_partitioned_by_the_hash_of_its_own_url(
                                                                                                                                                                                                sign: int) -> None:
                                                                                                                                                                                                """Each message carries `hash(url)` for its own URL, stored verbatim so a
                                                                                                                                                                                                negative key still routes through the producer's modulo (goal.md:113).

                                                                                                                                                                                                Args:
                                                                                                                                                                                                sign: The sign the seeded URL's hash must carry, so both sides of
                                                                                                                                                                                                zero are covered.
                                                                                                                                                                                                """
                                                                                                                                                                                                hashed = url_with_hash_sign(sign)
                                                                                                                                                                                                other = url_with_hash_sign(-sign)
                                                                                                                                                                                                assert (hash(hashed) < 0) == (sign < 0)
                                                                                                                                                                                                assert (hash(other) < 0) == (sign > 0)
                                                                                                                                                                                                harness = build(urls=[hashed, other])

                                                                                                                                                                                                await harness.poller.enqueue_urls([hashed, other])

                                                                                                                                                                                                (batch) = harness.producer.batches
                                                                                                                                                                                                assert len(batch) == 2
                                                                                                                                                                                                assert {message.url.get_url for message in batch} == {
                                                                                                                                                                                                    hashed.get_url,
                                                                                                                                                                                                    other.get_url,
                                                                                                                                                                                                    }
                                                                                                                                                                                                # Each message carries the hash of its own URL, whatever its sign.
                                                                                                                                                                                                assert all(message.partition_key == hash(message.url) for message in batch)

                                                                                                                                                                                            @pytest.mark.parametrize(
                                                                                                                                                                                                "transient_failures",
                                                                                                                                                                                                [
                                                                                                                                                                                                pytest.param(1, id="one_transient_failure"),
                                                                                                                                                                                                pytest.param(2, id="two_transient_failures"),
                                                                                                                                                                                                ])
                                                                                                                                                                                            async def test_a_retried_claim_inside_one_poll_check_does_not_grow_the_queue(
                                                                                                                                                                                                monkeypatch: pytest.MonkeyPatch, transient_failures: int
                                                                                                                                                                                                ) -> None:
                                                                                                                                                                                                """One poll check passes its id through the dedupe gate exactly once, so
                                                                                                                                                                                                the claim's own internal retries re-enter only the claim and the queue
                                                                                                                                                                                                grows by one bulk feed, not one per attempt (goal.md:100).

                                                                                                                                                                                                Args:
                                                                                                                                                                                                monkeypatch: pytest's patcher, undone after the test.
                                                                                                                                                                                                transient_failures: How many `sqlite3.OperationalError` attempts the
                                                                                                                                                                                                claim survives internally before succeeding.
                                                                                                                                                                                                """
                                                                                                                                                                                                harness = build(
                                                                                                                                                                                                    urls=[page_url(0)],
                                                                                                                                                                                                    stop_after_polls=1,
                                                                                                                                                                                                    transient_failures=transient_failures)
                                                                                                                                                                                                sleeps = SleepRecorder(harness.recorder())
                                                                                                                                                                                                monkeypatch.setattr(asyncio, "sleep", sleeps)

                                                                                                                                                                                                await drive(harness.poller())

                                                                                                                                                                                                # One served poll: one gate hit, one claim, one feed, one wait; then the
                                                                                                                                                                                                # stop poll reaches its own gate and claim before the loop is cancelled.
                                                                                                                                                                                                assert harness.recorder.names == [
                                                                                                                                                                                                    "seen_and_record",
                                                                                                                                                                                                    "claim_candidates",
                                                                                                                                                                                                    "enqueue_many",
                                                                                                                                                                                                    "sleep",
                                                                                                                                                                                                    "seen_and_record",
                                                                                                                                                                                                    "claim_candidates",
                                                                                                                                                                                                    ]
                                                                                                                                                                                                assert harness.repository.claim_attempts == transient_failures + 1
                                                                                                                                                                                                assert len(harness.producer.batches) == 1
                                                                                                                                                                                                assert [message.url for message in harness.producer.batches[0]] == [page_url(0)]
                                                                                                                                                                                                assert len(set(harness.dedupe.calls)) == len(harness.dedupe.calls())

                                                                                                                                                                                            @pytest.mark.parametrize("attempt", [1, 3])
                                                                                                                                                                                            async def test_a_repeated_request_id_is_never_entertained_again(
                                                                                                                                                                                                attempt: int) -> None:
                                                                                                                                                                                                """The shipped deduplicator never expires an id, so a repeat is skipped for
                                                                                                                                                                                                the rest of the run however late it arrives (review.md item 17).

                                                                                                                                                                                                Args:
                                                                                                                                                                                                attempt: Which later call re-uses the same `request_id`.
                                                                                                                                                                                                """
                                                                                                                                                                                                clock = MutableClock()
                                                                                                                                                                                                harness = build(
                                                                                                                                                                                                    urls=[page_url(0)],
                                                                                                                                                                                                    dedupe=InMemoryRequestIdDeduplicator,
                                                                                                                                                                                                    time_provider=clock)
                                                                                                                                                                                                url = page_url(0)

                                                                                                                                                                                                await harness.poller.enqueue_urls([url], request_id="same")
                                                                                                                                                                                                for _ in range(attempt):
                                                                                                                                                                                                    harness.repository.rows[url.get_url].state = CrawlState.NOT_CRAWLED
                                                                                                                                                                                                    clock.advance(timedelta(hours=1))
                                                                                                                                                                                                    await harness.poller.enqueue_urls([url], request_id="same")

                                                                                                                                                                                                    assert harness.recorder.count("claim_urls") == 1
                                                                                                                                                                                                    assert len(harness.producer.batches) == 1

                                                                                                                                                                                                    @pytest.mark.parametrize(
                                                                                                                                                                                                        ("interval", "polls"),
                                                                                                                                                                                                        [
                                                                                                                                                                                                        pytest.param(5.0, 1, id="the_default_interval_once"),
                                                                                                                                                                                                        pytest.param(1.0, 2, id="one_second_twice"),
                                                                                                                                                                                                        pytest.param(0.25, 4, id="a_quarter_second_four_times"),
                                                                                                                                                                                                        pytest.param(0.1, 3, id="a_tenth_of_a_second_three_times"),
                                                                                                                                                                                                        ])
                                                                                                                                                                                                    async def test_the_loop_polls_every_interval_starting_with_the_first_immediately(
                                                                                                                                                                                                        monkeypatch: pytest.MonkeyPatch, interval: float, polls: int
                                                                                                                                                                                                        ) -> None:
                                                                                                                                                                                                        """The first poll runs before any wait and every later poll one interval
                                                                                                                                                                                                        after the previous claim, with exactly one gate hit, one claim, and one
                                                                                                                                                                                                        bulk feed per poll, and no `claim_urls` ever leaving the loop, because
                                                                                                                                                                                                        `run` never calls the public APIs (goal.md:56-59).

                                                                                                                                                                                                        Args:
                                                                                                                                                                                                        monkeypatch: pytest's patcher, undone after the test.
                                                                                                                                                                                                        interval: The poll interval the poller is built with.
                                                                                                                                                                                                        polls: How many polls the repository serves before stopping the loop.
                                                                                                                                                                                                        """
                                                                                                                                                                                                        harness = build(
                                                                                                                                                                                                            urls=[page_url(0)],
                                                                                                                                                                                                            stop_after_polls=polls,
                                                                                                                                                                                                            periodic_fetch_seconds=interval)
                                                                                                                                                                                                        sleeps = SleepRecorder(harness.recorder())
                                                                                                                                                                                                        monkeypatch.setattr(asyncio, "sleep", sleeps)

                                                                                                                                                                                                        await drive(harness.poller())

                                                                                                                                                                                                        one_poll = ["seen_and_record", "claim_candidates", "enqueue_many", "sleep"]
                                                                                                                                                                                                        assert harness.recorder.names == one_poll * polls + [
                                                                                                                                                                                                            "seen_and_record",
                                                                                                                                                                                                            "claim_candidates",
                                                                                                                                                                                                            ]
                                                                                                                                                                                                        assert sleeps.durations == [interval] * polls
                                                                                                                                                                                                        assert harness.recorder.names[0] == "seen_and_record"
                                                                                                                                                                                                        assert "claim_urls" not in harness.recorder.names

                                                                                                                                                                                                    @pytest.mark.parametrize(
                                                                                                                                                                                                        "periodic_max_items",
                                                                                                                                                                                                        [
                                                                                                                                                                                                        pytest.param(-1, id="no_limit"),
                                                                                                                                                                                                        pytest.param(5, id="a_five_row_limit"),
                                                                                                                                                                                                        ])
                                                                                                                                                                                                    async def test_the_periodic_poll_claims_with_its_own_limit_and_the_configured_timeouts(
                                                                                                                                                                                                        monkeypatch: pytest.MonkeyPatch, periodic_max_items: int
                                                                                                                                                                                                        ) -> None:
                                                                                                                                                                                                        """Each periodic poll claims with `periodic_max_items`, reads its `now`
                                                                                                                                                                                                        from the injected clock, and passes both configured timeouts, so the poll
                                                                                                                                                                                                        and the store agree on one predicate (goal.md:59).

                                                                                                                                                                                                        Args:
                                                                                                                                                                                                        monkeypatch: pytest's patcher, undone after the test.
                                                                                                                                                                                                        periodic_max_items: The periodic row limit the poller is built with.
                                                                                                                                                                                                        """
                                                                                                                                                                                                        harness = build(
                                                                                                                                                                                                            urls=[page_url(0)],
                                                                                                                                                                                                            stop_after_polls=1,
                                                                                                                                                                                                            periodic_max_items=periodic_max_items)
                                                                                                                                                                                                        monkeypatch.setattr(asyncio, "sleep", SleepRecorder)

                                                                                                                                                                                                        await drive(harness.poller())

                                                                                                                                                                                                        # The served poll's claim; the stop poll records its claim too, before
                                                                                                                                                                                                        # raising, so the first detail is the one that ran.
                                                                                                                                                                                                        assert harness.recorder.details("claim_candidates")[0] == (
                                                                                                                                                                                                            NOW,
                                                                                                                                                                                                            periodic_max_items,
                                                                                                                                                                                                            JOB_TIMEOUT,
                                                                                                                                                                                                            QUEUE_TIMEOUT)

                                                                                                                                                                                                    @pytest.mark.parametrize(
                                                                                                                                                                                                        "api",
                                                                                                                                                                                                        [
                                                                                                                                                                                                        pytest.param("enqueue_urls", id="api_a_waits_for_the_poll"),
                                                                                                                                                                                                        pytest.param("queue_candidates", id="api_b_waits_for_the_poll"),
                                                                                                                                                                                                        ])
                                                                                                                                                                                                    async def test_an_api_call_waits_for_the_periodic_poll_in_flight(api: str) -> None:
                                                                                                                                                                                                        """While a periodic poll holds the lock, an API call parks before its own
                                                                                                                                                                                                        dedupe gate and claim, and proceeds only once the poll's feed is done, so
                                                                                                                                                                                                        the two never interleave (goal.md:107).

                                                                                                                                                                                                        Args:
                                                                                                                                                                                                        api: The entry point that must wait for the in-flight poll.
                                                                                                                                                                                                        """
                                                                                                                                                                                                        gate = asyncio.Event
                                                                                                                                                                                                        harness = build(
                                                                                                                                                                                                            urls=[page_url(0), page_url(1)],
                                                                                                                                                                                                            gate=gate,
                                                                                                                                                                                                            gate_poll=1,
                                                                                                                                                                                                            periodic_fetch_seconds=0.01)
                                                                                                                                                                                                        run_task = asyncio.create_task(harness.poller.run())
                                                                                                                                                                                                        await harness.repository.arrived.wait()
                                                                                                                                                                                                        assert harness.recorder.names == ["seen_and_record", "claim_candidates"]

                                                                                                                                                                                                        api_task = asyncio.create_task(call_api(harness.poller, api))
                                                                                                                                                                                                        for _ in range(3):
                                                                                                                                                                                                            await asyncio.sleep(0)
                                                                                                                                                                                                            # The API is parked at the lock: it has reached neither the gate nor its
                                                                                                                                                                                                            # claim while the poll's claim is in flight.
                                                                                                                                                                                                            assert harness.recorder.names == ["seen_and_record", "claim_candidates"]

                                                                                                                                                                                                            gate.set()
                                                                                                                                                                                                            await api_task
                                                                                                                                                                                                            assert harness.recorder.names[:6] == [
                                                                                                                                                                                                                "seen_and_record",
                                                                                                                                                                                                                "claim_candidates",
                                                                                                                                                                                                                "enqueue_many",
                                                                                                                                                                                                                "seen_and_record",
                                                                                                                                                                                                                claim_name(api),
                                                                                                                                                                                                                "enqueue_many",
                                                                                                                                                                                                                ]
                                                                                                                                                                                                            # The loop is stopped the way the orchestrator stops it, because an API
                                                                                                                                                                                                            # (b) claim shares this method with the loop and must not hit a stop script.
                                                                                                                                                                                                            run_task.cancel()
                                                                                                                                                                                                            with pytest.raises(asyncio.CancelledError):
                                                                                                                                                                                                                await run_task

                                                                                                                                                                                                                @pytest.mark.parametrize(
                                                                                                                                                                                                                    "api",
                                                                                                                                                                                                                    [
                                                                                                                                                                                                                    pytest.param("enqueue_urls", id="the_poll_waits_for_api_a"),
                                                                                                                                                                                                                    pytest.param("queue_candidates", id="the_poll_waits_for_api_b"),
                                                                                                                                                                                                                    ])
                                                                                                                                                                                                                async def test_the_periodic_poll_waits_for_an_api_call_in_flight(api: str) -> None:
                                                                                                                                                                                                                    """While an API call holds the lock across its claim and feed, the loop's
                                                                                                                                                                                                                    first poll parks before its own dedupe gate, so an API call never races
                                                                                                                                                                                                                    the poller run (goal.md:107).

                                                                                                                                                                                                                    Args:
                                                                                                                                                                                                                    api: The entry point that stays in flight while the loop starts.
                                                                                                                                                                                                                    """
                                                                                                                                                                                                                    gate = asyncio.Event
                                                                                                                                                                                                                    harness = build(
                                                                                                                                                                                                                        urls=[page_url(0), page_url(1)],
                                                                                                                                                                                                                        producer_gate=gate,
                                                                                                                                                                                                                        stop_after_polls=1,
                                                                                                                                                                                                                        periodic_fetch_seconds=0.01)
                                                                                                                                                                                                                    api_task = asyncio.create_task(call_api(harness.poller, api))
                                                                                                                                                                                                                    await harness.producer.arrived.wait()
                                                                                                                                                                                                                    assert harness.recorder.names == [
                                                                                                                                                                                                                        "seen_and_record",
                                                                                                                                                                                                                        claim_name(api),
                                                                                                                                                                                                                        "enqueue_many",
                                                                                                                                                                                                                        ]

                                                                                                                                                                                                                    run_task = asyncio.create_task(harness.poller.run())
                                                                                                                                                                                                                    for _ in range(3):
                                                                                                                                                                                                                        await asyncio.sleep(0)
                                                                                                                                                                                                                        # The poll is parked at the lock: it has not reached its own gate while the
                                                                                                                                                                                                                        # API's feed is in flight.
                                                                                                                                                                                                                        assert harness.recorder.names == [
                                                                                                                                                                                                                            "seen_and_record",
                                                                                                                                                                                                                            claim_name(api),
                                                                                                                                                                                                                            "enqueue_many",
                                                                                                                                                                                                                            ]

                                                                                                                                                                                                                        gate.set()
                                                                                                                                                                                                                        await api_task
                                                                                                                                                                                                                        with pytest.raises(asyncio.CancelledError):
                                                                                                                                                                                                                            await asyncio.wait_for(run_task, LOOP_GUARD_SECONDS)
                                                                                                                                                                                                                            names = harness.recorder.names
                                                                                                                                                                                                                            # The poll's own check began only after the API's feed had finished.
                                                                                                                                                                                                                            assert names.index("seen_and_record", 1) > names.index("enqueue_many")

                                                                                                                                                                                                                            @pytest.mark.parametrize(
                                                                                                                                                                                                                                ("first", "second"),
                                                                                                                                                                                                                                [
                                                                                                                                                                                                                                pytest.param("enqueue_urls", "queue_candidates", id="api_a_holds_the_lock"),
                                                                                                                                                                                                                                pytest.param("queue_candidates", "enqueue_urls", id="api_b_holds_the_lock"),
                                                                                                                                                                                                                                ])
                                                                                                                                                                                                                            async def test_two_api_calls_cannot_interleave(first: str, second: str) -> None:
                                                                                                                                                                                                                                """The lock is held across the whole dedupe-claim-feed body, so a second
                                                                                                                                                                                                                                entry point reaches neither the gate nor its claim until the first has
                                                                                                                                                                                                                                finished feeding: two entry points cannot interleave, which is also what
                                                                                                                                                                                                                                upholds the single-instance rule (goal.md:107).

                                                                                                                                                                                                                                Args:
                                                                                                                                                                                                                                first: The entry point that runs first and is held at its feed.
                                                                                                                                                                                                                                second: The entry point that must park at the lock meanwhile.
                                                                                                                                                                                                                                """
                                                                                                                                                                                                                                gate = asyncio.Event
                                                                                                                                                                                                                                harness = build(
                                                                                                                                                                                                                                    urls=[page_url(0), page_url(1)],
                                                                                                                                                                                                                                    producer_gate=gate)
                                                                                                                                                                                                                                first_task = asyncio.create_task(call_api(harness.poller, first))
                                                                                                                                                                                                                                await harness.producer.arrived.wait()
                                                                                                                                                                                                                                assert harness.recorder.names == [
                                                                                                                                                                                                                                    "seen_and_record",
                                                                                                                                                                                                                                    claim_name(first),
                                                                                                                                                                                                                                    "enqueue_many",
                                                                                                                                                                                                                                    ]

                                                                                                                                                                                                                                second_task = asyncio.create_task(call_api(harness.poller, second))
                                                                                                                                                                                                                                for _ in range(3):
                                                                                                                                                                                                                                    await asyncio.sleep(0)
                                                                                                                                                                                                                                    assert harness.recorder.names == [
                                                                                                                                                                                                                                        "seen_and_record",
                                                                                                                                                                                                                                        claim_name(first),
                                                                                                                                                                                                                                        "enqueue_many",
                                                                                                                                                                                                                                        ]
                                                                                                                                                                                                                                    assert len(harness.dedupe.calls) == 1

                                                                                                                                                                                                                                    gate.set()
                                                                                                                                                                                                                                    await first_task
                                                                                                                                                                                                                                    await second_task
                                                                                                                                                                                                                                    assert harness.recorder.names == [
                                                                                                                                                                                                                                        "seen_and_record",
                                                                                                                                                                                                                                        claim_name(first),
                                                                                                                                                                                                                                        "enqueue_many",
                                                                                                                                                                                                                                        "seen_and_record",
                                                                                                                                                                                                                                        claim_name(second),
                                                                                                                                                                                                                                        "enqueue_many",
                                                                                                                                                                                                                                        ]
                                                                                                                                                                                                                                    assert len(harness.dedupe.calls) == 2

                                                                                                                                                                                                                                    @pytest.mark.parametrize(
                                                                                                                                                                                                                                        "api",
                                                                                                                                                                                                                                        [
                                                                                                                                                                                                                                        pytest.param("enqueue_urls", id="api_a_skips_a_repeated_request_id"),
                                                                                                                                                                                                                                        pytest.param("queue_candidates", id="api_b_skips_a_repeated_request_id"),
                                                                                                                                                                                                                                        ])
                                                                                                                                                                                                                                    async def test_a_repeated_request_id_is_skipped_on_both_apis(api: str) -> None:
                                                                                                                                                                                                                                        """A repeated `request_id` reaches the gate, is told to skip, and issues
                                                                                                                                                                                                                                        neither a claim nor a feed, so a retried call does not grow the queue
                                                                                                                                                                                                                                        (goal.md:100).

                                                                                                                                                                                                                                        Args:
                                                                                                                                                                                                                                        api: The entry point invoked twice with the same id.
                                                                                                                                                                                                                                        """
                                                                                                                                                                                                                                        harness = build(urls=[page_url(0)])

                                                                                                                                                                                                                                        await call_api(harness.poller, api, request_id="same")
                                                                                                                                                                                                                                        await call_api(harness.poller, api, request_id="same")

                                                                                                                                                                                                                                        assert harness.dedupe.calls == ["same", "same"]
                                                                                                                                                                                                                                        assert harness.recorder.count(claim_name(api)) == 1
                                                                                                                                                                                                                                        assert len(harness.producer.batches) == 1

                                                                                                                                                                                                                                    @pytest.mark.parametrize(
                                                                                                                                                                                                                                        "entry",
                                                                                                                                                                                                                                        [
                                                                                                                                                                                                                                        pytest.param("enqueue_urls", id="api_a_with_an_empty_claim"),
                                                                                                                                                                                                                                        pytest.param("queue_candidates", id="api_b_with_an_empty_claim"),
                                                                                                                                                                                                                                        pytest.param("run_poll", id="a_periodic_poll_with_an_empty_claim"),
                                                                                                                                                                                                                                        ])
                                                                                                                                                                                                                                    async def test_an_empty_claim_still_issues_one_bulk_call(
                                                                                                                                                                                                                                        monkeypatch: pytest.MonkeyPatch, entry: str
                                                                                                                                                                                                                                        ) -> None:
                                                                                                                                                                                                                                        """An empty claim still issues the single `enqueue_many([])` call, because
                                                                                                                                                                                                                                        one code path means an empty poll costs the same call a full one does and
                                                                                                                                                                                                                                        the producer's result list stays the only per-row outcome; this test pins
                                                                                                                                                                                                                                        that documented choice.

                                                                                                                                                                                                                                        Args:
                                                                                                                                                                                                                                        monkeypatch: pytest's patcher, undone after the test.
                                                                                                                                                                                                                                        entry: The entry point whose claim returns no rows.
                                                                                                                                                                                                                                        """
                                                                                                                                                                                                                                        if entry == "run_poll":
                                                                                                                                                                                                                                            harness = build(urls=[], stop_after_polls=1)
                                                                                                                                                                                                                                            monkeypatch.setattr(asyncio, "sleep", SleepRecorder)
                                                                                                                                                                                                                                            await drive(harness.poller())
                                                                                                                                                                                                                                        elif entry == "enqueue_urls":
                                                                                                                                                                                                                                            harness = build(urls=[])
                                                                                                                                                                                                                                            await harness.poller.enqueue_urls([page_url(0)])
                                                                                                                                                                                                                                        else:
                                                                                                                                                                                                                                            harness = build(urls=[])
                                                                                                                                                                                                                                            await harness.poller.queue_candidates(NOW)

                                                                                                                                                                                                                                            assert harness.producer.batches == [[]]
                                                                                                                                                                                                                                            assert harness.recorder.count("enqueue_many") == 1
