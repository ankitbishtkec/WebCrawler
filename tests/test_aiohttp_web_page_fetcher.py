"""Tests for `AiohttpWebPageFetcher`.

Factory seam: the fetcher is constructed with a `session_factory` returning an
async context manager that yields one session per `fetch` call, and every
`session.get(...)` is itself an async context manager. These tests inject a
fake factory whose sessions script one outcome per request, so no socket is
ever opened while the session-level timeout, the non-2xx rule, the one session
per call, the per-call retry, and the one log record per attempt are all still
proved.
"""

import asyncio
import contextlib
import logging
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from typing import TypeVar

import aiohttp
import pytest

from webcrawler.domain.custom_url import CustomURL
from webcrawler.domain.errors import NonRetryableError
from webcrawler.infrastructure.fetch import aiohttp_web_page_fetcher as fetcher_module
from webcrawler.infrastructure.fetch.aiohttp_web_page_fetcher import (
    AiohttpWebPageFetcher)
from webcrawler.infrastructure.fetch.headers_middleware import HeadersMiddleware
from webcrawler.ports.request_middleware import RequestMiddleware
from webcrawler.ports.retry_policy import RetryPolicy

T = TypeVar("T")

LOGGER_NAME = "tests.webcrawler.aiohttp_fetch"
PAGE_URL = "https://crawlme.monzo.com/section/page.html?q=1"
SOCKET_TIMEOUT_SECONDS = 2.5
ATTEMPT_RECORD = "fetch attempt for"
FAILURE_RECORD = "fetch of"

class ScriptedRetryPolicy(RetryPolicy):
    """A `RetryPolicy` double with a real per-attempt deadline and no delay.

    It stands in for `ExponentialBackoffRetryPolicy` and reproduces the two
    behaviours the fetcher relies on: the policy owns the attempt count, and it
    bounds each attempt with `asyncio.wait_for`. A test therefore fails if the
    fetcher starts retrying by itself or stops handing the policy a fresh
    awaitable per attempt (`goal.md:141`). It also records the operation of
    every call, so a test can prove the operation is fresh per `fetch` call.

    Args:
    max_attempts: The attempt budget, counted the way the real one counts.
    timeout_seconds: The per-attempt deadline handed to `asyncio.wait_for`.
    """

    def __init__(self, max_attempts: int = 1, timeout_seconds: float = 5.0) -> None:
        """Hold the budget, the deadline, and the per-call counters.

        Args:
        max_attempts: The attempt budget.
        timeout_seconds: The per-attempt deadline.
        """
        self.max_attempts = max_attempts
        self.timeout_seconds = timeout_seconds
        self.calls = 0
        self.attempts = 0
        self.operations: list[Callable[[], Awaitable[object]]] = []

    async def execute(self, operation: Callable[[], Awaitable[T]]) -> T:
        """Run the operation until it succeeds or the budget is spent.

        Args:
        operation: A callable returning a fresh awaitable per attempt.

        Returns:
        T: The first successful result.

        Raises:
        NonRetryableError: Re-raised on the first attempt, matching the
        real policy's contract with the operation.
        BaseException: The last error, once the attempts are exhausted.
        """
        self.calls += 1
        self.operations.append(operation)
        failure: BaseException | None = None
        for _ in range(self.max_attempts):
            self.attempts += 1
            try:
                return await asyncio.wait_for(operation, self.timeout_seconds)
            except NonRetryableError:
                # A final answer ends the sequence, as the real policy does.
                raise
            except Exception as error: # noqa: BLE001 - recorded and re-raised
                failure = error
                assert failure is not None
                raise failure

                class FakeRequestInfo:
                    """The part of `aiohttp.RequestInfo` that `ClientResponseError.__str__` uses.

                    Args:
                    url: The URL the request asked for.
                    """

                    def __init__(self, url: str) -> None:
                        """Hold the URL both forms of the request info expose.

                        Args:
                        url: The requested URL.
                        """
                        self.url = url
                        self.real_url = url

                    class FakeResponse:
                        """The part of an `aiohttp.ClientResponse` that the fetcher uses.

                        Args:
                        status: The status the client reports.
                        body: The bytes `text` decodes.
                        delay_seconds: How long `text` awaits first, which models a server that
                        accepts the connection and then stalls.
                        """

                        def __init__(
                            self, status: int, body: bytes = b"", delay_seconds: float = 0.0
                            ) -> None:
                            """Hold the status, the body, and the scripted delay.

                            Args:
                            status: The response status.
                            body: The response body.
                            delay_seconds: How long `text` awaits before returning.
                            """
                            self.status = status
                            self.request_info = FakeRequestInfo(PAGE_URL)
                            self.history: tuple[object,...] = ()
                            self.reads = 0
                            self.closed = False
                            self._body = body
                            self._delay_seconds = delay_seconds

                        async def text(
                            self, encoding: str | None = None, errors: str = "strict"
                            ) -> str:
                            """Return the decoded body, after any scripted delay.

                            Args:
                            encoding: The encoding to decode the body with.
                            errors: The decode error handling to apply.

                            Returns:
                            str: The decoded body.
                            """
                            self.reads += 1
                            if self._delay_seconds:
                                await asyncio.sleep(self._delay_seconds())
                                return self._body.decode(encoding or "utf-8", errors=errors)

                                async def __aenter__(self) -> "FakeResponse":
                                    """Enter the response as a context manager.

                                    Returns:
                                    FakeResponse: This response.
                                    """
                                    return self

                                async def __aexit__(self, *args: object) -> None:
                                    """Leave the response, releasing it as the real client does."""
                                    self.closed = True

                                class FakeGetContext:
                                    """Stands in for the `_RequestContextManager` that `session.get` returns.

                                    Args:
                                    outcome: The response to yield on enter, or the transport error to
                                    raise, which models a failure before any response arrives.
                                    """

                                    def __init__(self, outcome: FakeResponse | BaseException) -> None:
                                        """Hold the scripted outcome of this one request.

                                        Args:
                                        outcome: The response, or the error to raise on enter.
                                        """
                                        self._outcome = outcome

                                    async def __aenter__(self) -> FakeResponse:
                                        """Enter the request, raising its scripted transport error if any.

                                        Returns:
                                        FakeResponse: The scripted response.

                                        Raises:
                                        BaseException: The scripted transport error, before a response.
                                        """
                                        if isinstance(self._outcome, BaseException):
                                            raise self._outcome
                                            return self._outcome

                                            async def __aexit__(self, *args: object) -> None:
                                                """Leave the request, releasing a scripted response if one arrived."""
                                                if isinstance(self._outcome, FakeResponse):
                                                    self._outcome.closed = True

                                                    class Call:
                                                        """One recorded `session.get` call.

                                                        Args:
                                                        url: The URL the fetcher requested.
                                                        headers: The headers the fetcher passed.
                                                        """

                                                        def __init__(self, url: str, headers: dict[str, str] | None) -> None:
                                                            """Hold the recorded URL and headers.

                                                            Args:
                                                            url: The requested URL.
                                                            headers: The request headers, or None when none were passed.
                                                            """
                                                            self.url = url
                                                            self.headers = headers

                                                        class FakeSession:
                                                            """Stands in for `aiohttp.ClientSession`: one scripted outcome per request.

                                                            Args:
                                                            outcomes: What each successive `get` yields or raises. The last entry
                                                            repeats once the script runs out, so a test that always fails only
                                                            has to say so once.
                                                            """

                                                            def __init__(self, outcomes: Sequence[FakeResponse | BaseException]) -> None:
                                                                """Hold the script and the empty call record.

                                                                Args:
                                                                outcomes: What each request into the session does.
                                                                """
                                                                self._outcomes = list(outcomes)
                                                                self.calls: list[Call] = []
                                                                self.closed = False

                                                            def get(
                                                                self, url: str, *, headers: dict[str, str] | None = None
                                                                ) -> FakeGetContext:
                                                                """Record one request and hand out its scripted outcome.

                                                                Args:
                                                                url: The URL the fetcher asked for.
                                                                headers: The request headers the fetcher passed.

                                                                Returns:
                                                                FakeGetContext: The request context manager, which yields the
                                                                scripted response or raises the scripted transport error.
                                                                """
                                                                self.calls.append(Call(url, headers))
                                                                outcome = self._outcomes[min(len(self.calls), len(self._outcomes)) - 1]
                                                                return FakeGetContext(outcome)

                                                            async def __aenter__(self) -> "FakeSession":
                                                                """Enter the session as a context manager.

                                                                Returns:
                                                                FakeSession: This session.
                                                                """
                                                                return self

                                                            async def __aexit__(self, *args: object) -> None:
                                                                """Leave the session, closing it as the real client does."""
                                                                self.closed = True

                                                            class FakeSessionFactory:
                                                                """Builds one fake session per `fetch` call, as the default factory does.

                                                                Args:
                                                                outcomes: The script every built session follows.
                                                                """

                                                                def __init__(self, outcomes: Sequence[FakeResponse | BaseException]) -> None:
                                                                    """Hold the script and the empty session record.

                                                                    Args:
                                                                    outcomes: What each request into every built session does.
                                                                    """
                                                                    self._outcomes = list(outcomes)
                                                                    self.sessions: list[FakeSession] = []

                                                                def __call__(self) -> FakeSession:
                                                                    """Build and record one session.

                                                                    Returns:
                                                                    FakeSession: The session, itself an async context manager.
                                                                    """
                                                                    session = FakeSession(self._outcomes())
                                                                    self.sessions.append(session)
                                                                    return session

                                                                def build(
                                                                    timeout_seconds: float,
                                                                    logger: logging.Logger,
                                                                    session_factory: Callable[[], FakeSession] | None = None,
                                                                    middlewares: Sequence[RequestMiddleware] =(),
                                                                    ) -> AiohttpWebPageFetcher:
                                                                    """Construct the fetcher the way the composition root constructs it.

                                                                    Args:
                                                                    timeout_seconds: The socket-side bound it is given.
                                                                    logger: The injected logger.
                                                                    session_factory: The session seam, or None for the real default.
                                                                    middlewares: The header middlewares applied to every request.

                                                                    Returns:
                                                                    AiohttpWebPageFetcher: A fetcher with no session open yet.
                                                                    """
                                                                    return AiohttpWebPageFetcher(
                                                                        timeout_seconds,
                                                                        logger,
                                                                        session_factory=session_factory,
                                                                        middlewares=middlewares)

                                                                @pytest.fixture
                                                                def logger() -> logging.Logger:
                                                                    """Return the real logger the fetcher is given, for caplog to capture.

                                                                    Returns:
                                                                    logging.Logger: The named logger, so a test can capture its records.
                                                                    """
                                                                    return logging.getLogger(LOGGER_NAME)

                                                                @pytest.mark.parametrize(
                                                                    ("status", "body"),
                                                                    [
                                                                    pytest.param(200, b"<html>ok</html>", id="ok"),
                                                                    pytest.param(201, b"created", id="created"),
                                                                    pytest.param(204, b"", id="no_content"),
                                                                    pytest.param(299, b"almost", id="top_of_the_2xx_range"),
                                                                    ])
                                                                async def test_a_2xx_response_returns_its_decoded_body(
                                                                    logger: logging.Logger, status: int, body: bytes
                                                                    ) -> None:
                                                                    """A 2xx body is the return value, and it is decoded as UTF-8.

                                                                    Args:
                                                                    logger: The injected logger.
                                                                    status: The status the client reports.
                                                                    body: The bytes the client returns.
                                                                    """
                                                                    factory = FakeSessionFactory([FakeResponse(status, body)])
                                                                    policy = ScriptedRetryPolicy(max_attempts=3)

                                                                    fetched = await build(SOCKET_TIMEOUT_SECONDS, logger, factory).fetch(
                                                                        CustomURL(PAGE_URL), policy
                                                                        )

                                                                    assert fetched == body.decode("utf-8")
                                                                    assert factory.sessions[0].calls[0].url == CustomURL(PAGE_URL).get_url
                                                                    assert policy.attempts == 1

                                                                @pytest.mark.parametrize(
                                                                    "status",
                                                                    [301, 307, 400, 404, 429, 500, 503])
                                                                async def test_a_non_2xx_status_raises(logger: logging.Logger, status: int) -> None:
                                                                    """An error page is never mistaken for a page, so a status outside 2xx is
                                                                    raised as a `NonRetryableError` naming that status.

                                                                    Args:
                                                                    logger: The injected logger.
                                                                    status: The status the client reports.
                                                                    """
                                                                    response = FakeResponse(status, b"not a page")
                                                                    factory = FakeSessionFactory([response])
                                                                    policy = ScriptedRetryPolicy(max_attempts=1)

                                                                    with pytest.raises(NonRetryableError) as raised:
                                                                        await build(SOCKET_TIMEOUT_SECONDS, logger, factory).fetch(
                                                                            CustomURL(PAGE_URL), policy
                                                                            )

                                                                        assert f"status {status}" in raised.value.reason
                                                                        assert response.closed is True
                                                                        assert len(factory.sessions[0].calls) == 1

                                                                        @pytest.mark.parametrize(
                                                                            ("failures", "error"),
                                                                            [
                                                                            pytest.param(1, aiohttp.ClientError("generic client error"), id="client_error"),
                                                                            pytest.param(
                                                                            1, aiohttp.ClientConnectionError("connection refused"), id="connection"
                                                                            ),
                                                                            pytest.param(
                                                                            2, aiohttp.ClientConnectionError("connection reset"), id="connection_twice"
                                                                            ),
                                                                            pytest.param(1, OSError("no route to host"), id="os_error"),
                                                                            pytest.param(1, TimeoutError("timed out"), id="timeout_error"),
                                                                            ])
                                                                        async def test_a_transport_error_is_retried_by_the_injected_policy(
                                                                            logger: logging.Logger, failures: int, error: BaseException
                                                                            ) -> None:
                                                                            """A transient failure is the policy's business, so the fetcher hands it
                                                                            one attempt and lets the policy decide how many more there are (`goal.md:141`).

                                                                            Args:
                                                                            logger: The injected logger.
                                                                            failures: How many attempts raise before one succeeds.
                                                                            error: The error the failing attempts raise.
                                                                            """
                                                                            factory = FakeSessionFactory([error] * failures + [FakeResponse(200, b"ok")])
                                                                            policy = ScriptedRetryPolicy(max_attempts=failures + 1)

                                                                            fetched = await build(SOCKET_TIMEOUT_SECONDS, logger, factory).fetch(
                                                                                CustomURL(PAGE_URL), policy
                                                                                )

                                                                            assert fetched == "ok"
                                                                            assert policy.attempts == failures + 1
                                                                            assert policy.calls == 1

                                                                        @pytest.mark.parametrize("max_attempts", [1, 2, 3])
                                                                        async def test_a_transport_error_is_re_raised_once_the_budget_is_spent(
                                                                            logger: logging.Logger, max_attempts: int
                                                                            ) -> None:
                                                                            """The caller sees the real cause rather than a wrapper, and the attempt
                                                                            count is exactly the budget the policy was given.

                                                                            Args:
                                                                            logger: The injected logger.
                                                                            max_attempts: The policy's attempt budget.
                                                                            """
                                                                            failure = aiohttp.ClientConnectionError("connection reset")
                                                                            factory = FakeSessionFactory([failure])
                                                                            policy = ScriptedRetryPolicy(max_attempts=max_attempts)

                                                                            with pytest.raises(aiohttp.ClientConnectionError) as raised:
                                                                                await build(SOCKET_TIMEOUT_SECONDS, logger, factory).fetch(
                                                                                    CustomURL(PAGE_URL), policy
                                                                                    )

                                                                                assert raised.value is failure
                                                                                assert policy.attempts == max_attempts
                                                                                assert len(factory.sessions[0].calls) == max_attempts

                                                                                @pytest.mark.parametrize("status", [404, 429, 500, 503])
                                                                                async def test_a_non_2xx_status_is_never_retried(
                                                                                    logger: logging.Logger, status: int
                                                                                    ) -> None:
                                                                                    """The site answered, so a backoff would only delay the crawl: a status is
                                                                                    raised on the first attempt however large the budget is (review.md item 12).

                                                                                    Args:
                                                                                    logger: The injected logger.
                                                                                    status: The status the client reports, 503 included.
                                                                                    """
                                                                                    factory = FakeSessionFactory([FakeResponse(status, b"no")])
                                                                                    policy = ScriptedRetryPolicy(max_attempts=3)

                                                                                    with pytest.raises(NonRetryableError):
                                                                                        await build(SOCKET_TIMEOUT_SECONDS, logger, factory).fetch(
                                                                                            CustomURL(PAGE_URL), policy
                                                                                            )

                                                                                        assert policy.attempts == 1

                                                                                        @pytest.mark.parametrize("timeout_seconds", [0.01, 0.05])
                                                                                        async def test_a_fetch_over_its_deadline_is_abandoned_and_retried(
                                                                                            logger: logging.Logger, timeout_seconds: float
                                                                                            ) -> None:
                                                                                            """The policy bounds an attempt with `asyncio.wait_for`, so a stalled
                                                                                            response is abandoned and the next attempt's body is the one returned.

                                                                                            Args:
                                                                                            logger: The injected logger.
                                                                                            timeout_seconds: The per-attempt deadline the policy applies.
                                                                                            """
                                                                                            stalled = FakeResponse(200, b"too slow", delay_seconds=0.2)
                                                                                            factory = FakeSessionFactory([stalled, FakeResponse(200, b"in time")])
                                                                                            policy = ScriptedRetryPolicy(max_attempts=2, timeout_seconds=timeout_seconds)

                                                                                            fetched = await build(SOCKET_TIMEOUT_SECONDS, logger, factory).fetch(
                                                                                                CustomURL(PAGE_URL), policy
                                                                                                )

                                                                                            assert fetched == "in time"
                                                                                            assert policy.attempts == 2
                                                                                            assert len(factory.sessions[0].calls) == 2
                                                                                            assert stalled.reads == 1

                                                                                        @pytest.mark.parametrize(
                                                                                            ("outcomes", "max_attempts", "expected_attempts", "expect_failure_record"),
                                                                                            [
                                                                                            pytest.param([FakeResponse(200, b"ok")], 1, 1, False, id="success"),
                                                                                            pytest.param([FakeResponse(404, b"no")], 3, 1, True, id="status_failure"),
                                                                                            pytest.param(
                                                                                            [aiohttp.ClientConnectionError("refused")], 3, 3, True, id="transport"
                                                                                            ),
                                                                                            ])
                                                                                        async def test_every_attempt_emits_a_log_record(
                                                                                            caplog: pytest.LogCaptureFixture,
                                                                                            logger: logging.Logger,
                                                                                            outcomes: list[FakeResponse | BaseException],
                                                                                            max_attempts: int,
                                                                                            expected_attempts: int,
                                                                                            expect_failure_record: bool) -> None:
                                                                                            """`goal.md:141` asks for logging as part of the retry, so each attempt is
                                                                                            recorded before it starts, and a failed one records the cause.

                                                                                            Args:
                                                                                            caplog: pytest's log capture.
                                                                                            logger: The injected logger.
                                                                                            outcomes: What each attempt into the session does.
                                                                                            max_attempts: The policy's attempt budget; the status case spends
                                                                                            one attempt of it, because a status is not retried.
                                                                                            expected_attempts: How many attempt records must be captured.
                                                                                            expect_failure_record: Whether a failure record must be captured too.
                                                                                            """
                                                                                            factory = FakeSessionFactory(outcomes)
                                                                                            policy = ScriptedRetryPolicy(max_attempts=max_attempts)

                                                                                            with caplog.at_level(logging.INFO, logger=LOGGER_NAME):
                                                                                                with contextlib.suppress(aiohttp.ClientError, OSError, NonRetryableError):
                                                                                                    await build(SOCKET_TIMEOUT_SECONDS, logger, factory).fetch(
                                                                                                        CustomURL(PAGE_URL), policy
                                                                                                        )

                                                                                                    attempts = [
                                                                                                        record
                                                                                                        for record in caplog.records
                                                                                                        if ATTEMPT_RECORD in record.getMessage
                                                                                                        ]
                                                                                                    assert len(attempts) == expected_attempts == policy.attempts
                                                                                                    assert all(record.name == LOGGER_NAME for record in attempts)
                                                                                                    assert all(record.levelno == logging.INFO for record in attempts)
                                                                                                    failures = [
                                                                                                        record for record in caplog.records if FAILURE_RECORD in record.getMessage
                                                                                                        ]
                                                                                                    assert bool(failures) is expect_failure_record
                                                                                                    assert all(record.name == LOGGER_NAME for record in failures)

                                                                                                    @pytest.mark.parametrize(
                                                                                                        "raw_url",
                                                                                                        [
                                                                                                        "https://crawlme.monzo.com/",
                                                                                                        "https://crawlme.monzo.com/a%20b",
                                                                                                        "http://crawlme.monzo.com:8080/x",
                                                                                                        "https://crawlme.monzo.com/x?b=2&a=1#frag",
                                                                                                        ])
                                                                                                    async def test_the_request_is_the_canonical_url(
                                                                                                        logger: logging.Logger, raw_url: str
                                                                                                        ) -> None:
                                                                                                        """The client is handed `get_url`, so a dropped port, a re-sorted query,
                                                                                                        and a dropped fragment are already applied.

                                                                                                        Args:
                                                                                                        logger: The injected logger.
                                                                                                        raw_url: The URL the caller crawled, in any of its raw forms.
                                                                                                        """
                                                                                                        factory = FakeSessionFactory([FakeResponse(200, b"ok")])

                                                                                                        await build(SOCKET_TIMEOUT_SECONDS, logger, factory).fetch(
                                                                                                            CustomURL(raw_url), ScriptedRetryPolicy
                                                                                                            )

                                                                                                        assert factory.sessions[0].calls[0].url == CustomURL(raw_url).get_url

                                                                                                    @pytest.mark.parametrize("headers_sent", [{}, {"User-Agent": "crawler/1.0"}])
                                                                                                    async def test_the_middlewares_are_the_only_source_of_request_headers(
                                                                                                        logger: logging.Logger, headers_sent: dict[str, str]
                                                                                                        ) -> None:
                                                                                                        """The fetcher adds no header of its own, so a request carries exactly what
                                                                                                        the caller's middlewares set.

                                                                                                        Args:
                                                                                                        logger: The injected logger.
                                                                                                        headers_sent: The headers the injected middleware sets, or none.
                                                                                                        """
                                                                                                        factory = FakeSessionFactory([FakeResponse(200, b"ok")])

                                                                                                        await build(
                                                                                                            SOCKET_TIMEOUT_SECONDS,
                                                                                                            logger,
                                                                                                            factory,
                                                                                                            middlewares=(HeadersMiddleware(headers_sent))).fetch(CustomURL(PAGE_URL), ScriptedRetryPolicy)

                                                                                                        headers = factory.sessions[0].calls[0].headers
                                                                                                        assert headers == headers_sent

                                                                                                    @pytest.mark.parametrize("fetch_count", [1, 2, 3])
                                                                                                    async def test_the_policy_is_applied_per_call_with_a_fresh_operation(
                                                                                                        logger: logging.Logger, fetch_count: int
                                                                                                        ) -> None:
                                                                                                        """Each `fetch` call is one policy call over one fresh operation and one
                                                                                                        session, and that session is closed when the call ends, so the fetcher
                                                                                                        holds nothing between calls.

                                                                                                        Args:
                                                                                                        logger: The injected logger.
                                                                                                        fetch_count: How many `fetch` calls the test makes.
                                                                                                        """
                                                                                                        factory = FakeSessionFactory([FakeResponse(200, b"ok")])
                                                                                                        policy = ScriptedRetryPolicy()
                                                                                                        fetcher = build(SOCKET_TIMEOUT_SECONDS, logger, factory)

                                                                                                        for _ in range(fetch_count):
                                                                                                            await fetcher.fetch(CustomURL(PAGE_URL), policy)

                                                                                                            assert policy.calls == fetch_count
                                                                                                            assert policy.attempts == fetch_count
                                                                                                            assert len(policy.operations) == fetch_count
                                                                                                            assert len({id(operation) for operation in policy.operations}) == fetch_count
                                                                                                            assert len(factory.sessions) == fetch_count
                                                                                                            assert all(session.closed for session in factory.sessions)

                                                                                                            @pytest.mark.parametrize("timeout_seconds", [0.1, 2.5, 30.0])
                                                                                                            async def test_the_default_factory_bounds_the_socket_with_the_constructor_timeout(
                                                                                                                logger: logging.Logger, timeout_seconds: float
                                                                                                                ) -> None:
                                                                                                                """ requires the socket side to be bounded by
                                                                                                                `aiohttp.ClientTimeout(total=timeout_seconds)`, so a session built by the
                                                                                                                default factory carries that bound; building a session opens no socket.

                                                                                                                Args:
                                                                                                                logger: The injected logger.
                                                                                                                timeout_seconds: The socket-side bound the fetcher was constructed with.
                                                                                                                """
                                                                                                                fetcher = build(timeout_seconds, logger)
                                                                                                                factory = fetcher._session_factory

                                                                                                                async with factory() as session:
                                                                                                                    assert session.timeout.total == timeout_seconds
                                                                                                                    assert session.closed is False

                                                                                                                    assert session.closed is True

                                                                                                                    @pytest.mark.parametrize("middlewares", [(), None])
                                                                                                                    async def test_with_no_middlewares_the_request_carries_no_headers(
                                                                                                                        logger: logging.Logger, middlewares: Sequence[RequestMiddleware] | None
                                                                                                                        ) -> None:
                                                                                                                        """A crawl with no middlewares configured sends no header at all.

                                                                                                                        Args:
                                                                                                                        logger: The injected logger.
                                                                                                                        middlewares: None or empty, both of which must behave identically.
                                                                                                                        """
                                                                                                                        factory = FakeSessionFactory([FakeResponse(200, b"ok")])

                                                                                                                        fetcher = AiohttpWebPageFetcher(
                                                                                                                            SOCKET_TIMEOUT_SECONDS,
                                                                                                                            logger,
                                                                                                                            session_factory=factory,
                                                                                                                            **({} if middlewares is None else {"middlewares": middlewares}))
                                                                                                                        await fetcher.fetch(CustomURL(PAGE_URL), ScriptedRetryPolicy)

                                                                                                                        assert factory.sessions[0].calls[0].headers == {}

                                                                                                                    @pytest.mark.parametrize(
                                                                                                                        "override", ["Mozilla/5.0 (X11) Chrome/124.0.0.0 Safari/537.36", "custom-agent/1.0"]
                                                                                                                        )
                                                                                                                    async def test_a_later_middleware_overrides_an_earlier_user_agent(
                                                                                                                        logger: logging.Logger, override: str
                                                                                                                        ) -> None:
                                                                                                                        """Middlewares run in order, so the last one to set a header wins.

                                                                                                                        Args:
                                                                                                                        logger: The injected logger.
                                                                                                                        override: The `User-Agent` value the last middleware sets.
                                                                                                                        """
                                                                                                                        factory = FakeSessionFactory([FakeResponse(200, b"ok")])

                                                                                                                        await build(
                                                                                                                            SOCKET_TIMEOUT_SECONDS,
                                                                                                                            logger,
                                                                                                                            factory,
                                                                                                                            middlewares=(
                                                                                                                            HeadersMiddleware({"User-Agent": "first/0.1", "Accept": "text/html"}),
                                                                                                                            HeadersMiddleware({"User-Agent": override}))).fetch(CustomURL(PAGE_URL), ScriptedRetryPolicy)

                                                                                                                        headers = factory.sessions[0].calls[0].headers
                                                                                                                        assert headers == {"User-Agent": override, "Accept": "text/html"}

