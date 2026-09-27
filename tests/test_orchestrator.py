"""Tests for `Orchestrator` ( the rewritten seed-once bullet).

Every collaborator is a double: a recording repository, a recording poller
and worker whose `run` either parks forever or ends in a cancellation of its
own, a recording seed source that counts every read and every close, and a
named stdlib logger for `caplog`. Nothing here touches a real console, a
real database, or real time: the crawl is stopped exactly the way the
orchestrator stops it in production, by a cancellation, and every `run` is
wrapped in `asyncio.wait_for` so a loop that never stops fails its test
instead of hanging the session.

The shared `CallRecorder` is what makes the ordering assertions meaningful:
the doubles cannot consult each other, so the only evidence of sequence is
the order in which they appended to it. The seed-once assertions rest on the
fake source's own counters — `lines` calls, `__anext__` calls, and `aclose`
calls — because those are the only ways the orchestrator could read a seed.
"""

import asyncio
import contextlib
import inspect
import logging
import sqlite3
from collections.abc import AsyncIterator
from datetime import datetime, timedelta

import pytest

from webcrawler.application.orchestrator import Orchestrator
from webcrawler.domain.custom_url import CustomURL
from webcrawler.ports.url_state_repository import URLStateRepository

SITE = "http://crawlme.monzo.com"
LOGGER_NAME = "tests.webcrawler.orchestrator"
# The loop guard: a `run` that has not stopped inside this budget fails its
# test instead of sticking the session.
LOOP_GUARD_SECONDS = 5.0
CANCEL_THE_ORCHESTRATOR_TASK = "cancel_the_orchestrator_task"
POLLER_SELF_CANCELS = "the_poller_task_ends_in_cancellation"
WORKER_SELF_CANCELS = "the_worker_task_ends_in_cancellation"
STOP_MODES = [
    pytest.param(CANCEL_THE_ORCHESTRATOR_TASK, id="the_operators_ctrl_c"),
    pytest.param(POLLER_SELF_CANCELS, id="the_poller_task_ends_in_cancellation"),
    pytest.param(WORKER_SELF_CANCELS, id="the_worker_task_ends_in_cancellation"),
    ]

def real_logger() -> logging.Logger:
    """Return the real logger the orchestrator is given.

    Returns:
    logging.Logger: A named stdlib logger, so `caplog` can capture the
    records a rejected seed writes.
    """
    return logging.getLogger(LOGGER_NAME)

def page(index: int) -> str:
    """Return the text of the nth page of the crawl site.

    Args:
    index: The page position, which keeps scripted lines distinct.

    Returns:
    str: `http://crawlme.monzo.com/p<index>`.
    """
    return f"{SITE}/p{index}"

def canonical(line: str) -> str:
    """Return the canonical form of one seed line.

    Args:
    line: The seed line as the source yielded it.

    Returns:
    str: The `CustomURL` the orchestrator builds from the whole line,
    so an assertion can compare against what was inserted.
    """
    return CustomURL(line).get_url

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
        name: The collaborator and method, as `"repository.close"`.
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
        list[object]: The detail of each recorded call with that name,
        in call order.
        """
        return [detail for recorded, detail in self.entries if recorded == name]

    class FakeRepository(URLStateRepository):
        """A recording double of the store's seed-relevant calls.

        Only `create_urls` and `close` behave: every other port method records
        nothing and returns nothing, because the orchestrator never calls them.
        `close` is what proves the shutdown ran, so it always records.

        Args:
        recorder: The shared ordered call log.
        create_error: The error every `create_urls` raises, or None to insert
        normally, modelling an exhausted DB retry.
        """

        def __init__(
            self, recorder: CallRecorder, *, create_error: Exception | None = None
            ) -> None:
            """Take the shared log and the optional scripted insert failure.

            Args:
            recorder: The shared ordered call log.
            create_error: The error every `create_urls` raises, or None.
            """
            self._recorder = recorder
            self._create_error = create_error

        async def initialize(self) -> None:
            """Do nothing; the composition root initializes a store, not the orchestrator."""

        async def create_urls(self, urls: list[CustomURL]) -> None:
            """Record the inserted URLs, or raise the scripted failure.

            Args:
            urls: The canonical URLs the orchestrator wants inserted.

            Raises:
            Exception: The scripted `create_error`, when one was given.
            """
            self._recorder.record(
                "repository.create_urls", [url.get_url for url in urls]
                )
            if self._create_error is not None:
                raise self._create_error

                async def close(self) -> None:
                    """Record the close, which is the evidence the shutdown ran."""
                    self._recorder.record("repository.close", None)

                async def get_crawlable_urls(
                    self,
                    now: datetime,
                    max_items: int,
                    *,
                    job_timeout: timedelta,
                    queue_timeout: timedelta) -> list[CustomURL]:
                    """Return nothing; the orchestrator never previews the store.

                    Args:
                    now: The instant the predicate would be evaluated at.
                    max_items: The row limit.
                    job_timeout: The `started_crawl` staleness timeout.
                    queue_timeout: The `queued` staleness timeout.

                    Returns:
                    list[CustomURL]: Always empty, because this double models only
                    the calls the orchestrator makes.
                    """
                    return []

                async def claim_candidates(
                    self,
                    now: datetime,
                    max_items: int,
                    *,
                    job_timeout: timedelta,
                    queue_timeout: timedelta) -> list[CustomURL]:
                    """Return nothing; the orchestrator never claims.

                    Args:
                    now: The instant the predicates would be evaluated against.
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
                    """Return nothing; the orchestrator never claims.

                    Args:
                    urls: The URLs a claim would be restricted to.
                    now: The instant the predicates would be evaluated against.
                    max_items: The row limit.
                    job_timeout: The `started_crawl` staleness timeout.
                    queue_timeout: The `queued` staleness timeout.

                    Returns:
                    list[CustomURL]: Always empty.
                    """
                    return []

                async def mark_started(self, url: CustomURL, now: datetime) -> None:
                    """Do nothing; the worker owns marking, not the orchestrator.

                    Args:
                    url: The URL that would be marked.
                    now: The attempt time.
                    """

                async def complete_crawl(
                    self,
                    finished: list[tuple[CustomURL, datetime | None]],
                    discovered: list[CustomURL],
                    now: datetime) -> None:
                    """Do nothing; the worker owns completions, not the orchestrator.

                    Args:
                    finished: The outcomes that would be written.
                    discovered: The URLs that would be inserted.
                    now: The completion time.
                    """

                class FakePoller:
                    """A recording double of the queuer and its one long-running loop.

                    `run` records, signals `started`, then either parks on an event forever
                    (recording the cancellation that ends it) or ends in a cancellation of
                    its own, which is the other way a task can stop in production.

                    Args:
                    recorder: The shared ordered call log.
                    enqueue_error: The error every `enqueue_urls` raises, or None to
                    queue normally, modelling an exhausted claim retry.
                    self_cancel: Whether `run` ends in an `asyncio.CancelledError` of
                    its own instead of parking.
                    """

                    def __init__(
                        self,
                        recorder: CallRecorder,
                        *,
                        enqueue_error: Exception | None = None,
                        self_cancel: bool = False) -> None:
                        """Take the shared log, the optional scripted enqueue failure, and the stop shape.

                        Args:
                        recorder: The shared ordered call log.
                        enqueue_error: The error every `enqueue_urls` raises, or None.
                        self_cancel: Whether `run` ends on its own in a cancellation.
                        """
                        self._recorder = recorder
                        self._enqueue_error = enqueue_error
                        self._self_cancel = self_cancel
                        self.started = asyncio.Event

                    async def enqueue_urls(
                        self, urls: list[CustomURL], request_id: str | None = None
                        ) -> None:
                        """Record the queued URLs, or raise the scripted failure.

                        Args:
                        urls: The canonical URLs the orchestrator wants queued.
                        request_id: The dedupe key, unused by this double.

                        Raises:
                        Exception: The scripted `enqueue_error`, when one was given.
                        """
                        self._recorder.record(
                            "poller.enqueue_urls", [url.get_url for url in urls]
                            )
                        if self._enqueue_error is not None:
                            raise self._enqueue_error

                            async def queue_candidates(
                                self,
                                now: datetime,
                                max_items: int | None = None,
                                request_id: str | None = None) -> None:
                                """Record nothing; the shipped run path never calls API (b).

                                Args:
                                now: The instant the claim would be evaluated at.
                                max_items: The row limit.
                                request_id: The dedupe key.
                                """

                            async def run(self) -> None:
                                """Record the start, then park until cancelled or self-cancel.

                                Raises:
                                asyncio.CancelledError: On the loop task's own cancellation,
                                after recording it, which is how the orchestrator stops the
                                poller in production.
                                """
                                self._recorder.record("poller.run", None)
                                self.started.set()
                                if self._self_cancel:
                                    self._recorder.record("poller.cancelled", None)
                                    raise asyncio.CancelledError
                                    try:
                                        await asyncio.Event().wait()
                                    except asyncio.CancelledError:
                                        self._recorder.record("poller.cancelled", None)
                                        raise

                                        class FakeWorker:
                                            """A recording double of the crawl worker's one long-running loop.

                                            Args:
                                            recorder: The shared ordered call log.
                                            self_cancel: Whether `run` ends in an `asyncio.CancelledError` of
                                            its own instead of parking.
                                            """

                                            def __init__(self, recorder: CallRecorder, *, self_cancel: bool = False) -> None:
                                                """Take the shared log and the stop shape.

                                                Args:
                                                recorder: The shared ordered call log.
                                                self_cancel: Whether `run` ends on its own in a cancellation.
                                                """
                                                self._recorder = recorder
                                                self._self_cancel = self_cancel
                                                self.started = asyncio.Event

                                            async def run(self) -> None:
                                                """Record the start, then park until cancelled or self-cancel.

                                                Raises:
                                                asyncio.CancelledError: On the loop task's own cancellation,
                                                after recording it, which is how the orchestrator stops the
                                                worker in production.
                                                """
                                                self._recorder.record("worker.run", None)
                                                self.started.set()
                                                if self._self_cancel:
                                                    self._recorder.record("worker.cancelled", None)
                                                    raise asyncio.CancelledError
                                                    try:
                                                        await asyncio.Event().wait()
                                                    except asyncio.CancelledError:
                                                        self._recorder.record("worker.cancelled", None)
                                                        raise

                                                        class Harness:
                                                            """One orchestrator wired over recording doubles, built per test.

                                                            Args:
                                                            seed_lines: The scripted seed lines, in arrival order.
                                                            cancel_before_first_line: Whether the first seed read is
                                                            interrupted by a cancellation.
                                                            create_error: The error every `create_urls` raises, or None.
                                                            enqueue_error: The error every `enqueue_urls` raises, or None.
                                                            poller_self_cancel: Whether the poller's `run` ends in a
                                                            cancellation of its own.
                                                            worker_self_cancel: Whether the worker's `run` does.

                                                                Attributes:
                                                                    recorder: The shared ordered call log.
                                                                    repository: The recording repository.
                                                                    poller: The recording poller, with its `started` event.
                                                                    worker: The recording worker, with its `started` event.
                                                                    seed_line: The seed line handed to the orchestrator.
                                                                    orchestrator: The orchestrator under test.
                                                            """

                                                            def __init__(
                                                                self,
                                                                *,
                                                                seed_line: str | None = None,
                                                                create_error: Exception | None = None,
                                                                enqueue_error: Exception | None = None,
                                                                poller_self_cancel: bool = False,
                                                                worker_self_cancel: bool = False) -> None:
                                                                """Wire the doubles and the orchestrator over them; nothing runs.

                                                                Args:
                                                                seed_line: The one seed line the operator entered.
                                                                create_error: The error every `create_urls` raises, or None.
                                                                enqueue_error: The error every `enqueue_urls` raises, or None.
                                                                poller_self_cancel: Whether the poller's `run` self-cancels.
                                                                worker_self_cancel: Whether the worker's `run` self-cancels.
                                                                """
                                                                self.recorder = CallRecorder()
                                                                self.repository = FakeRepository(self.recorder, create_error=create_error)
                                                                self.poller = FakePoller(
                                                                    self.recorder, enqueue_error=enqueue_error, self_cancel=poller_self_cancel
                                                                    )
                                                                self.worker = FakeWorker(self.recorder, self_cancel=worker_self_cancel)
                                                                self.seed_line = seed_line if seed_line is not None else page(0)
                                                                self.orchestrator = Orchestrator(
                                                                    self.repository,
                                                                    self.poller,
                                                                    self.worker,
                                                                    self.seed_line,
                                                                    real_logger)

                                                            async def drive(harness: Harness, stop_mode: str) -> None:
                                                                """Run one crawl to its stop, failing rather than hanging.

                                                                Args:
                                                                harness: The wired orchestrator to run.
                                                                stop_mode: How the crawl ends: `CANCEL_THE_ORCHESTRATOR_TASK` models
                                                                the operator's Ctrl+C landing on the run itself, while the two
                                                                self-cancellation modes model it landing on one of the awaited
                                                                tasks.
                                                                """
                                                                task = asyncio.create_task(harness.orchestrator.run())
                                                                await harness.poller.started.wait()
                                                                await harness.worker.started.wait()
                                                                if stop_mode == CANCEL_THE_ORCHESTRATOR_TASK:
                                                                    task.cancel()
                                                                    with pytest.raises(asyncio.CancelledError):
                                                                        await asyncio.wait_for(task, timeout=LOOP_GUARD_SECONDS)

                                                                        @pytest.mark.parametrize(
                                                                            "seed_line",
                                                                            [
                                                                            pytest.param(page(0), id="one_line"),
                                                                            pytest.param(page(0).strip, id="an_already_stripped_line"),
                                                                            ])
                                                                        async def test_the_seed_line_is_used_exactly_as_given(seed_line: str) -> None:
                                                                            """`main.py` hands over the one line it read, and the orchestrator queues
                                                                            that line and nothing else (review.md item 2).

                                                                            Args:
                                                                            seed_line: The seed line the operator entered.
                                                                            """
                                                                            harness = Harness(seed_line=seed_line)
                                                                            await drive(harness, CANCEL_THE_ORCHESTRATOR_TASK)

                                                                            assert harness.recorder.count("repository.create_urls") == 1
                                                                            assert harness.recorder.details("repository.create_urls") == [[canonical(seed_line)]]

                                                                        @pytest.mark.parametrize(
                                                                            "seed_line",
                                                                            [
                                                                            pytest.param(f"{SITE}/", id="the_site_root"),
                                                                            pytest.param(f"{SITE}/deep/start?from=seed", id="a_deep_page_with_a_query"),
                                                                            ])
                                                                        async def test_a_valid_seed_is_inserted_before_it_is_queued(seed_line: str) -> None:
                                                                            """A valid seed is inserted and then queued, in that order, before
                                                                            anything runs, because the queuer claims rows that already exist.

                                                                            Args:
                                                                            seed_line: The scripted seed line, a valid single URL.
                                                                            """
                                                                            harness = Harness(seed_line=seed_line)
                                                                            await drive(harness, CANCEL_THE_ORCHESTRATOR_TASK)

                                                                            names = harness.recorder.names
                                                                            assert names[:2] == ["repository.create_urls", "poller.enqueue_urls"]
                                                                            assert names.index("poller.enqueue_urls") < names.index("poller.run")
                                                                            assert harness.recorder.count("repository.create_urls") == 1
                                                                            assert harness.recorder.count("poller.enqueue_urls") == 1
                                                                            assert harness.recorder.details("repository.create_urls") == [[canonical(seed_line)]]
                                                                            assert harness.recorder.details("poller.enqueue_urls") == [[canonical(seed_line)]]

                                                                        @pytest.mark.parametrize("stop_mode", STOP_MODES)
                                                                        async def test_the_crawl_runs_until_both_tasks_are_cancelled(stop_mode: str) -> None:
                                                                            """Exactly one poller task and one worker task run, both end in a
                                                                            cancellation, and the store is closed only after both have.

                                                                            Args:
                                                                            stop_mode: How the crawl is ended, all three shapes of the
                                                                            interrupt: on the run itself, or on either awaited task.
                                                                            """
                                                                            harness = Harness(
                                                                                poller_self_cancel=stop_mode == POLLER_SELF_CANCELS,
                                                                                worker_self_cancel=stop_mode == WORKER_SELF_CANCELS)
                                                                            await drive(harness, stop_mode)

                                                                            names = harness.recorder.names
                                                                            assert harness.recorder.count("poller.run") == 1
                                                                            assert harness.recorder.count("worker.run") == 1
                                                                            assert names.index("poller.run") > names.index("poller.enqueue_urls")
                                                                            assert names.index("worker.run") > names.index("poller.enqueue_urls")
                                                                            assert harness.recorder.count("poller.cancelled") == 1
                                                                            assert harness.recorder.count("worker.cancelled") == 1
                                                                            assert names.index("repository.close") > names.index("poller.cancelled")
                                                                            assert names.index("repository.close") > names.index("worker.cancelled")
                                                                            assert harness.recorder.count("repository.close") == 1

                                                                        @pytest.mark.parametrize(
                                                                            ("seed_lines", "log_snippet"),
                                                                            [
                                                                            pytest.param(["not a url"], "was rejected", id="a_line_without_a_scheme"),
                                                                            pytest.param(
                                                                            ["ftp://crawlme.monzo.com/"], "was rejected", id="a_non_http_scheme"
                                                                            ),
                                                                            pytest.param([""], "no seed URL was entered", id="an_empty_line"),
                                                                            ])
                                                                        async def test_an_invalid_seed_line_is_logged_and_no_task_is_created(
                                                                            caplog: pytest.LogCaptureFixture, seed_lines: list[str], log_snippet: str
                                                                            ) -> None:
                                                                            """An unusable seed is logged at INFO and the run returns cleanly, with
                                                                            no poller or worker task ever created.

                                                                            Args:
                                                                            caplog: pytest's log capture.
                                                                            seed_lines: The scripted lines, none of which is a usable seed.
                                                                            log_snippet: The text the INFO record must carry for this shape.
                                                                            """
                                                                            harness = Harness(seed_line=seed_lines[0])

                                                                            with caplog.at_level(logging.INFO, logger=LOGGER_NAME):
                                                                                result = await asyncio.wait_for(
                                                                                    harness.orchestrator.run, timeout=LOOP_GUARD_SECONDS
                                                                                    )

                                                                                assert result is None
                                                                                assert harness.recorder.count("repository.create_urls") == 0
                                                                                assert harness.recorder.count("poller.run") == 0
                                                                                assert harness.recorder.count("worker.run") == 0
                                                                                assert harness.recorder.count("repository.close") == 1
                                                                                assert any(
                                                                                    record.levelno == logging.INFO and log_snippet in record.getMessage
                                                                                    for record in caplog.records
                                                                                    )
                                                                                assert all(record.levelno <= logging.INFO for record in caplog.records)

                                                                                @pytest.mark.parametrize(
                                                                                    ("fail_point", "error"),
                                                                                    [
                                                                                    pytest.param(
                                                                                    "create_urls",
                                                                                    sqlite3.OperationalError("the insert's retries are exhausted"),
                                                                                    id="the_insert_fails"),
                                                                                    pytest.param(
                                                                                    "enqueue_urls",
                                                                                    sqlite3.OperationalError("the claim's retries are exhausted"),
                                                                                    id="the_queue_handoff_fails"),
                                                                                    ])
                                                                                async def test_a_seed_write_failure_is_logged_and_no_task_is_created(
                                                                                    caplog: pytest.LogCaptureFixture, fail_point: str, error: sqlite3.OperationalError
                                                                                    ) -> None:
                                                                                    """An exhausted DB retry on either seed write is logged at INFO and the
                                                                                    run returns cleanly, with no task created.

                                                                                    Args:
                                                                                    caplog: pytest's log capture.
                                                                                    fail_point: Which seed write fails, the insert or the queue handoff.
                                                                                    error: The exhausted-retry error that write raises.
                                                                                    """
                                                                                    harness = Harness(
                                                                                        create_error=error if fail_point == "create_urls" else None,
                                                                                        enqueue_error=error if fail_point == "enqueue_urls" else None)

                                                                                    with caplog.at_level(logging.INFO, logger=LOGGER_NAME):
                                                                                        result = await asyncio.wait_for(
                                                                                            harness.orchestrator.run, timeout=LOOP_GUARD_SECONDS
                                                                                            )

                                                                                        assert result is None
                                                                                        assert harness.recorder.count("repository.create_urls") == 1
                                                                                        assert harness.recorder.count("poller.enqueue_urls") == (
                                                                                            0 if fail_point == "create_urls" else 1
                                                                                            )
                                                                                        assert harness.recorder.count("poller.run") == 0
                                                                                        assert harness.recorder.count("worker.run") == 0
                                                                                        assert harness.recorder.count("repository.close") == 1
                                                                                        assert any(
                                                                                            record.levelno == logging.INFO
                                                                                            and "could not be recorded or queued" in record.getMessage
                                                                                            for record in caplog.records
                                                                                            )
                                                                                        assert all(record.levelno <= logging.INFO for record in caplog.records)

                                                                                        @pytest.mark.parametrize(
                                                                                            "seed_line",
                                                                                            [pytest.param(f"{SITE}/one,{SITE}/two", id="a_comma_separated_pair")])
                                                                                        async def test_the_seed_line_is_never_split_on_commas(seed_line: str) -> None:
                                                                                            """A comma-separated line is validated whole: one seed URL whose path
                                                                                            carries the commas, never two URLs.

                                                                                            Args:
                                                                                            seed_line: A line that looks like `goal.md:103`'s comma-separated
                                                                                            form and must not be split into it.
                                                                                            """
                                                                                            harness = Harness(seed_line=seed_line)
                                                                                            await drive(harness, CANCEL_THE_ORCHESTRATOR_TASK)

                                                                                            inserted = harness.recorder.details("repository.create_urls")
                                                                                            queued = harness.recorder.details("poller.enqueue_urls")
                                                                                            assert inserted == [[canonical(seed_line)]]
                                                                                            assert queued == [[canonical(seed_line)]]
                                                                                            assert len(inserted[0]) == 1
                                                                                            assert len(queued[0]) == 1

                                                                                        @pytest.mark.parametrize(
                                                                                            "seed_line",
                                                                                            [pytest.param(f"{SITE}/sole-seed", id="the_only_line")])
                                                                                        async def test_run_consumes_the_seed_it_was_handed_and_reads_no_console(
                                                                                            seed_line: str) -> None:
                                                                                            """The orchestrator queues the line it was given; the single blocking
                                                                                            console read belongs to `main.py`, before any task exists (review.md
                                                                                            item 2).

                                                                                            Args:
                                                                                            seed_line: The one seed line, the only input the whole run sees.
                                                                                            """
                                                                                            harness = Harness(seed_line=seed_line)
                                                                                            await drive(harness, CANCEL_THE_ORCHESTRATOR_TASK)

                                                                                            assert harness.recorder.count("seed.line") == 0
                                                                                            assert harness.recorder.details("repository.create_urls") == [[canonical(seed_line)]]

                                                                                        @pytest.mark.parametrize(
                                                                                            "check",
                                                                                            [
                                                                                            pytest.param("main_is_defined", id="main_is_defined"),
                                                                                            pytest.param(
                                                                                            "main_is_a_coroutine_function", id="main_is_a_coroutine_function"
                                                                                            ),
                                                                                            pytest.param("no_task_was_started", id="no_task_was_started"),
                                                                                            ])
                                                                                        async def test_importing_main_has_no_side_effects(check: str) -> None:
                                                                                            """Importing the entry point defines the wiring but runs none of it: no
                                                                                            console loop starts and no task is left behind.

                                                                                            The import happens inside this test, while an event loop is already
                                                                                            running, so an import-time `asyncio.run` would have raised and failed
                                                                                            the import itself -- the successful import is part of the assertion.

                                                                                            Args:
                                                                                            check: Which side-effect-free property of the import to assert.
                                                                                            """
                                                                                            import webcrawler.main as main_module

                                                                                            if check == "main_is_defined":
                                                                                                assert callable(main_module.main())
                                                                                            elif check == "main_is_a_coroutine_function":
                                                                                                assert inspect.iscoroutinefunction(main_module.main())
                                                                                            else:
                                                                                                assert [
                                                                                                    task
                                                                                                    for task in asyncio.all_tasks
                                                                                                    if task is not asyncio.current_task
                                                                                                    ] == []

                                                                                                @pytest.mark.parametrize("module", ["webcrawler.main"])
                                                                                                async def test_main_runs_with_every_name_it_uses_resolved(module: str) -> None:
                                                                                                    """`main`'s body must resolve every name it reads.

                                                                                                    Importing the module only proves the module-level names; a name used
                                                                                                    inside the body (the `logging.INFO` passed to `configure_logging`, say) is
                                                                                                    invisible until the coroutine actually runs, so this executes it with the
                                                                                                    console read and the collaborators stubbed out.

                                                                                                    Args:
                                                                                                    module: The entry point under test.
                                                                                                    """
                                                                                                    import builtins

                                                                                                    import webcrawler.main as main_module

                                                                                                    # An empty seed ends the run cleanly: the blocking read happens in main,
                                                                                                    # and an empty line is the shape that starts no task.
                                                                                                    original_input = builtins.input
                                                                                                    builtins.input = lambda _prompt="": ""
                                                                                                    try:
                                                                                                        # Only the interrupt that ends a real crawl is tolerated; a
                                                                                                        # NameError from an unresolved body name must fail this test.
                                                                                                        with contextlib.suppress(asyncio.CancelledError):
                                                                                                            await asyncio.wait_for(main_module.main, timeout=LOOP_GUARD_SECONDS)
                                                                                                    finally:
                                                                                                        builtins.input = original_input
