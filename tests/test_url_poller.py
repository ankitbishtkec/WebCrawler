"""Tests for `URLPoller` ( the "Poller" bullet)."""

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
    """Return the real logger the poller is given."""
    return logging.getLogger(LOGGER_NAME)


def page_url(index: int) -> CustomURL:
    """Return the nth page of the crawl site."""
    return CustomURL(f"{SITE}/p{index}")


def url_with_hash_sign(sign: int) -> CustomURL:
    """Return the first page URL whose hash carries the wanted sign."""
    index = 0
    while True:
        url = CustomURL(f"{SITE}/p{index}")
        if (hash(url) < 0) == (sign < 0):
            return url
        index += 1


def message_for(url: CustomURL) -> BaseMessage:
    """Return the queue message the poller builds for a URL."""
    return BaseMessage(url, partition_key=hash(url))


def claim_name(api: str) -> str:
    """Return the repository call an API entry point issues."""
    return "claim_urls" if api == "enqueue_urls" else "claim_candidates"


async def call_api(
    poller: URLPoller,
    api: str,
    *,
    now: datetime = NOW,
    request_id: str | None = None) -> object:
    """Invoke one public API by name, with the arguments each one takes."""
    if api == "enqueue_urls":
        return await poller.enqueue_urls([page_url(0)], request_id=request_id)
    return await poller.queue_candidates(now, request_id=request_id)


async def drive(poller: URLPoller, *, timeout: float = LOOP_GUARD_SECONDS) -> None:
    """Run a poller to its scripted stop, failing rather than hanging."""
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(poller.run(), timeout)


class CallRecorder:
    """One ordered log of the calls every double received."""

    def __init__(self) -> None:
        """Start with an empty log."""
        self.entries: list[tuple[str, object]] = []

    def record(self, name: str, detail: object = None) -> None:
        """Append one call to the log."""
        self.entries.append((name, detail))

    def names(self) -> list[str]:
        """Return the recorded call names, in order."""
        return [name for name, _ in self.entries]

    def count(self, name: str) -> int:
        """Return how many times a call was recorded."""
        return self.names().count(name)

    def details(self, name: str) -> list[object]:
        """Return the recorded details of every call with a name."""
        return [detail for recorded, detail in self.entries if recorded == name]


class SleepRecorder:
    """A stand-in for `asyncio.sleep` that records instead of waiting."""

    def __init__(self, recorder: CallRecorder | None = None) -> None:
        """Start with no recorded waits and no optional mirror."""
        self.durations: list[float] = []
        self._recorder = recorder

    async def __call__(self, delay: float) -> None:
        """Record one requested wait and return immediately."""
        self.durations.append(delay)
        if self._recorder is not None:
            self._recorder.record("sleep", delay)


class MutableClock(TimeProviderFactory):
    """A clock the test moves by hand, so a poll never waits for real time."""

    def __init__(self, moment: datetime = NOW) -> None:
        """Freeze the clock at `moment`."""
        self._moment = moment

    def now(self) -> datetime:
        """Report the current instant."""
        return self._moment

    def advance(self, delta: timedelta) -> None:
        """Move the clock forward."""
        self._moment += delta


class RowState:
    """One URL row as the modelled store holds it."""

    def __init__(
        self,
        state: CrawlState,
        next_crawl_time: datetime,
        last_status_update_time: datetime) -> None:
        """Hold the three columns a claim reads."""
        self.state = state
        self.next_crawl_time = next_crawl_time
        self.last_status_update_time = last_status_update_time


class FakeRepository(URLStateRepository):
    """A stateful model of the two claims the poller makes."""

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
        """Take the script and start with no rows."""
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
        self.arrived = asyncio.Event()

    def seed(
        self,
        urls: list[CustomURL],
        *,
        state: CrawlState = CrawlState.NOT_CRAWLED,
        next_crawl_time: datetime = NOW,
        last_status_update_time: datetime = NOW) -> None:
        """Insert rows for the URLs, as the store's insert would leave them."""
        for url in urls:
            self.rows[url.get_url()] = RowState(
                state, next_crawl_time, last_status_update_time
            )

    async def initialize(self) -> None:
        """Do nothing; the composition root initializes a store, not the poller."""

    async def create_urls(self, urls: list[CustomURL]) -> None:
        """Insert the URLs as fresh `not_crawled` rows."""
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
        """Return nothing; the poller claims, it does not preview."""
        return []

    async def claim_candidates(
        self,
        now: datetime,
        max_items: int,
        *,
        job_timeout: timedelta,
        queue_timeout: timedelta) -> list[CustomURL]:
        """Claim eligible rows, unrestricted to any caller's list."""
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
            self._clock.advance(self._clock_step)
        return self._claim(None, now, max_items, job_timeout, queue_timeout)

    async def claim_urls(
        self,
        urls: list[CustomURL],
        now: datetime,
        max_items: int = -1,
        *,
        job_timeout: timedelta,
        queue_timeout: timedelta) -> list[CustomURL]:
        """Claim eligible rows, restricted to the caller's own URLs."""
        self._recorder.record(
            "claim_urls",
            (
                tuple(url.get_url() for url in urls),
                now,
                max_items,
                job_timeout,
                queue_timeout))
        return self._claim(urls, now, max_items, job_timeout, queue_timeout)

    async def mark_started(self, url: CustomURL, now: datetime) -> None:
        """Record the call; the poller never marks a row started."""
        self._recorder.record("mark_started", url.get_url())

    async def complete_crawl(
        self,
        finished: list[tuple[CustomURL, datetime | None]],
        discovered: list[CustomURL],
        now: datetime) -> None:
        """Record the call; the poller never completes a crawl."""
        self._recorder.record("complete_crawl", now)

    def _claim(
        self,
        urls: list[CustomURL] | None,
        now: datetime,
        max_items: int,
        job_timeout: timedelta,
        queue_timeout: timedelta) -> list[CustomURL]:
        """Retry the claim internally, then apply it once, as one call."""
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
        """Select, limit, and move the eligible rows to `queued`."""
        if self._claim_error is not None:
            raise self._claim_error
        if self.claim_attempts <= self._transient_failures:
            raise sqlite3.OperationalError("database is locked")
        if urls is not None and not urls:
            return []
        wanted = None if urls is None else {url.get_url() for url in urls}
        matches = sorted(
            (row.next_crawl_time, url_text)
            for url_text, row in self.rows.items()
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
        """Apply the `goal.md:33-51` predicate to one row."""
        if row.state is CrawlState.NOT_CRAWLED:
            return True
        if row.state is CrawlState.FINISHED_CRAWL:
            return row.next_crawl_time <= now
        if row.state is CrawlState.STARTED_CRAWL:
            return now - row.last_status_update_time >= job_timeout
        return now - row.last_status_update_time >= queue_timeout


class FakeProducer(TopicProducer):
    """A recording producer whose bulk call reports scripted overflows."""

    def __init__(
        self,
        recorder: CallRecorder,
        *,
        overflow: set[str] | None = None,
        gate: asyncio.Event | None = None) -> None:
        """Take the overflow script and the optional gate, with no batches."""
        self._recorder = recorder
        self._overflow = set() if overflow is None else overflow
        self._gate = gate
        self._gate_used = False
        self.batches: list[list[BaseMessage]] = []
        self.arrived = asyncio.Event()

    async def connect(self) -> None:
        """Do nothing; the composition root connects the producer, not the poller."""

    async def enqueue(self, message: BaseMessage) -> None:
        """Record the single-message call the poller must never make."""
        self._recorder.record("enqueue", message.url.get_url())

    async def enqueue_many(self, messages: list[BaseMessage]) -> list[bool]:
        """Record one bulk call and report one result per message."""
        self._recorder.record(
            "enqueue_many", tuple(message.url.get_url() for message in messages)
        )
        if self._gate is not None and not self._gate_used:
            self._gate_used = True
            self.arrived.set()
            await self._gate.wait()
        self.batches.append(list(messages))
        return [message.url.get_url() not in self._overflow for message in messages]


class FakeDedupe(RequestDeduplicator):
    """A set-based dedupe that records every id it was asked about."""

    def __init__(self, recorder: CallRecorder) -> None:
        """Start with no ids seen."""
        self._recorder = recorder
        self._seen: set[str] = set()
        self.calls: list[str] = []

    def seen_and_record(self, request_id: str) -> bool:
        """Record one id and report whether the caller must skip its request."""
        self._recorder.record("seen_and_record", request_id)
        self.calls.append(request_id)
        if request_id in self._seen:
            return True
        self._seen.add(request_id)
        return False


class Harness:
    """The doubles under test and the poller wired from them."""

    def __init__(
        self,
        *,
        recorder: CallRecorder,
        repository: FakeRepository,
        producer: FakeProducer,
        dedupe: RequestDeduplicator,
        time_provider: TimeProviderFactory,
        poller: URLPoller) -> None:
        """Hold the doubles and the poller, naming only ports (goal.md:16)."""
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
    """Wire one poller from doubles, seeded with the URLs given."""
    recorder = CallRecorder()
    clock = MutableClock() if time_provider is None else time_provider
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
        logger=real_logger())
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
    """Both APIs report success by returning `None`, never a partial result."""
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
    """API (a) passes the caller's own list to the claim with `max_items=-1`,."""
    urls = [page_url(index) for index in range(url_count)]
    harness = build(urls=urls)

    await harness.poller.enqueue_urls(urls)

    assert harness.recorder.details("claim_urls") == [
        (
            tuple(url.get_url() for url in urls),
            NOW,
            -1,
            JOB_TIMEOUT,
            QUEUE_TIMEOUT)
        ]
    assert harness.recorder.names() == [
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
    """Only the claimed subset of the caller's URLs is fed, through exactly one."""
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
    """API (b) falls back to `max_items_to_queue` only when its own limit is."""
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
    """A failed claim propagates unchanged and nothing is fed, because a."""
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
    """A `False` bulk result is logged and not raised: the row is already."""
    url = page_url(0)
    harness = build(
        urls=[url],
        overflow={url.get_url()},
        queue_timeout=queue_timeout,
        clock_step=queue_timeout,
        stop_after_polls=2)
    monkeypatch.setattr(asyncio, "sleep", SleepRecorder())

    with caplog.at_level(logging.INFO, logger=LOGGER_NAME):
        assert await harness.poller.enqueue_urls([url]) is None
        await drive(harness.poller)

    rejected = [
        record for record in caplog.records if url.get_url() in record.getMessage()
        ]
    assert rejected
    assert harness.repository.rows[url.get_url()].state is CrawlState.QUEUED
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
    """Each message carries `hash(url)` for its own URL, stored verbatim so a."""
    hashed = url_with_hash_sign(sign)
    other = url_with_hash_sign(-sign)
    assert (hash(hashed) < 0) == (sign < 0)
    assert (hash(other) < 0) == (sign > 0)
    harness = build(urls=[hashed, other])

    await harness.poller.enqueue_urls([hashed, other])

    # Unpacking is the assertion that exactly one bulk call carried the batch.
    (batch,) = harness.producer.batches
    assert len(batch) == 2
    assert {message.url.get_url() for message in batch} == {
        hashed.get_url(),
        other.get_url(),
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
    """One poll check passes its id through the dedupe gate exactly once, so."""
    harness = build(
        urls=[page_url(0)],
        stop_after_polls=1,
        transient_failures=transient_failures)
    sleeps = SleepRecorder(harness.recorder)
    monkeypatch.setattr(asyncio, "sleep", sleeps)

    await drive(harness.poller)

    # One served poll: one gate hit, one claim, one feed, one wait; then the
    # stop poll reaches its own gate and claim before the loop is cancelled.
    assert harness.recorder.names() == [
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
    assert len(set(harness.dedupe.calls)) == len(harness.dedupe.calls)


@pytest.mark.parametrize("attempt", [1, 3])
async def test_a_repeated_request_id_is_never_entertained_again(
    attempt: int) -> None:
    """The shipped deduplicator never expires an id, so a repeat is skipped for."""
    clock = MutableClock()
    harness = build(
        urls=[page_url(0)],
        dedupe=InMemoryRequestIdDeduplicator(),
        time_provider=clock)
    url = page_url(0)

    await harness.poller.enqueue_urls([url], request_id="same")
    for _ in range(attempt):
        harness.repository.rows[url.get_url()].state = CrawlState.NOT_CRAWLED
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
    """The first poll runs before any wait and every later poll one interval."""
    harness = build(
        urls=[page_url(0)],
        stop_after_polls=polls,
        periodic_fetch_seconds=interval)
    sleeps = SleepRecorder(harness.recorder)
    monkeypatch.setattr(asyncio, "sleep", sleeps)

    await drive(harness.poller)

    one_poll = ["seen_and_record", "claim_candidates", "enqueue_many", "sleep"]
    assert harness.recorder.names() == one_poll * polls + [
        "seen_and_record",
        "claim_candidates",
    ]
    assert sleeps.durations == [interval] * polls
    assert harness.recorder.names()[0] == "seen_and_record"
    assert "claim_urls" not in harness.recorder.names()


@pytest.mark.parametrize(
    "periodic_max_items",
    [
    pytest.param(-1, id="no_limit"),
    pytest.param(5, id="a_five_row_limit"),
    ])
async def test_the_periodic_poll_claims_with_its_own_limit_and_the_configured_timeouts(
    monkeypatch: pytest.MonkeyPatch, periodic_max_items: int
) -> None:
    """Each periodic poll claims with `periodic_max_items`, reads its `now`."""
    harness = build(
        urls=[page_url(0)],
        stop_after_polls=1,
        periodic_max_items=periodic_max_items)
    monkeypatch.setattr(asyncio, "sleep", SleepRecorder())

    await drive(harness.poller)

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
    """While a periodic poll holds the lock, an API call parks before its own."""
    gate = asyncio.Event()
    harness = build(
        urls=[page_url(0), page_url(1)],
        gate=gate,
        gate_poll=1,
        periodic_fetch_seconds=0.01)
    run_task = asyncio.create_task(harness.poller.run())
    await harness.repository.arrived.wait()
    assert harness.recorder.names() == ["seen_and_record", "claim_candidates"]

    api_task = asyncio.create_task(call_api(harness.poller, api))
    for _ in range(3):
        await asyncio.sleep(0)
        # The API is parked at the lock: it has reached neither the gate nor its
        # claim while the poll's claim is in flight.
        assert harness.recorder.names() == ["seen_and_record", "claim_candidates"]

    gate.set()
    await api_task
    assert harness.recorder.names()[:6] == [
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
    """While an API call holds the lock across its claim and feed, the loop's."""
    gate = asyncio.Event()
    harness = build(
        urls=[page_url(0), page_url(1)],
        producer_gate=gate,
        stop_after_polls=1,
        periodic_fetch_seconds=0.01)
    api_task = asyncio.create_task(call_api(harness.poller, api))
    await harness.producer.arrived.wait()
    assert harness.recorder.names() == [
        "seen_and_record",
        claim_name(api),
        "enqueue_many",
    ]

    run_task = asyncio.create_task(harness.poller.run())
    for _ in range(3):
        await asyncio.sleep(0)
        # The poll is parked at the lock: it has not reached its own gate while the
        # API's feed is in flight.
        assert harness.recorder.names() == [
            "seen_and_record",
            claim_name(api),
            "enqueue_many",
        ]

    gate.set()
    await api_task
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(run_task, LOOP_GUARD_SECONDS)
    names = harness.recorder.names()
    # The poll's own check began only after the API's feed had finished.
    assert names.index("seen_and_record", 1) > names.index("enqueue_many")


@pytest.mark.parametrize(
    ("first", "second"),
    [
    pytest.param("enqueue_urls", "queue_candidates", id="api_a_holds_the_lock"),
    pytest.param("queue_candidates", "enqueue_urls", id="api_b_holds_the_lock"),
    ])
async def test_two_api_calls_cannot_interleave(first: str, second: str) -> None:
    """The lock is held across the whole dedupe-claim-feed body, so a second."""
    gate = asyncio.Event()
    harness = build(
        urls=[page_url(0), page_url(1)],
        producer_gate=gate)
    first_task = asyncio.create_task(call_api(harness.poller, first))
    await harness.producer.arrived.wait()
    assert harness.recorder.names() == [
        "seen_and_record",
        claim_name(first),
        "enqueue_many",
    ]

    second_task = asyncio.create_task(call_api(harness.poller, second))
    for _ in range(3):
        await asyncio.sleep(0)
        assert harness.recorder.names() == [
            "seen_and_record",
            claim_name(first),
            "enqueue_many",
        ]
        assert len(harness.dedupe.calls) == 1

    gate.set()
    await first_task
    await second_task
    assert harness.recorder.names() == [
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
    """A repeated `request_id` reaches the gate, is told to skip, and issues."""
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
    """An empty claim still issues the single `enqueue_many([])` call, because."""
    if entry == "run_poll":
        harness = build(urls=[], stop_after_polls=1)
        monkeypatch.setattr(asyncio, "sleep", SleepRecorder())
        await drive(harness.poller)
    elif entry == "enqueue_urls":
        harness = build(urls=[])
        await harness.poller.enqueue_urls([page_url(0)])
    else:
        harness = build(urls=[])
        await harness.poller.queue_candidates(NOW)

    assert harness.producer.batches == [[]]
    assert harness.recorder.count("enqueue_many") == 1
