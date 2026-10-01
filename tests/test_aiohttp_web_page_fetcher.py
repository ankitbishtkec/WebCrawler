"""
Happy-path and failing-status tests for `AiohttpWebPageFetcher`, the real `aiohttp`
transport.
"""

from collections.abc import Awaitable, Callable, Sequence
from typing import NamedTuple
from unittest.mock import AsyncMock, MagicMock

import aiohttp
import pytest

from webcrawler.domain.custom_url import CustomURL
from webcrawler.domain.errors import NonRetryableError, RetryableStatusError
from webcrawler.domain.retry_settings import RetrySettings
from webcrawler.infrastructure.fetch.aiohttp_web_page_fetcher import (
    AiohttpWebPageFetcher)
from webcrawler.infrastructure.fetch.headers_middleware import HeadersMiddleware
from webcrawler.infrastructure.retry.exponential_backoff_retry_policy import (
    ExponentialBackoffRetryPolicy)
from webcrawler.ports.request_middleware import RequestMiddleware

URL: str = "https://crawlme.monzo.com/index.html"

# `CustomURL` is immutable, so one instance is safely shared by every test here.
PAGE: CustomURL = CustomURL(URL)

BODY: str = "<html>a page</html>"

# The status the mocked session answers with, the only one on the happy path.
SUCCESS_STATUS: int = 200

# The fetcher's socket-side bound, inert here because the session is injected.
TIMEOUT_SECONDS: float = 5.0

# Two statuses the shipped `RETRYABLE_STATUS_CODES` names, a throttled site and
# an unavailable one, so the retryable path is not shown for a single code.
THROTTLED_STATUS: int = 429
UNAVAILABLE_STATUS: int = 503

# A non-2xx status outside that set, so the site has given a final answer.
FINAL_STATUS: int = 404

# The budget the real policy is given, and the transport error it is fed when
# the session itself fails rather than the site answering.
ATTEMPTS: int = 3
TRANSPORT_ERROR: aiohttp.ClientError = aiohttp.ClientError(
    "the connection was closed before any answer")

# The real policy over a three attempt budget and no wait at all, so what these
# tests count is the attempts, never the time spent between them.
NO_WAIT: RetrySettings = RetrySettings(
    max_attempts=ATTEMPTS,
    base_delay_seconds=0.0,
    max_delay_seconds=0.0,
    jitter_seconds=0.0,
    timeout_seconds=TIMEOUT_SECONDS,
)


class Rig(NamedTuple):
    """The system under test together with the mocks it was built over."""

    fetcher: AiohttpWebPageFetcher
    session: MagicMock
    session_factory: AsyncMock


async def run_once(operation: Callable[[], Awaitable[str]]) -> str:
    """Run the operation a single time, which is all a success needs."""
    return await operation()


def make_session(body: str = BODY) -> MagicMock:
    """Build a mocked session that answers every request with a 200."""
    response = MagicMock()
    response.status = SUCCESS_STATUS
    response.text = AsyncMock(return_value=body)
    # `async with` must yield the very response the fetcher reads a status from.
    response.__aenter__.return_value = response

    session = MagicMock()
    session.get.return_value = response
    session.close = AsyncMock()
    return session


def make_fetcher(
    middlewares: Sequence[RequestMiddleware] = (),
) -> Rig:
    """Build the real fetcher over a mocked session and a mocked retry policy."""
    session = make_session()
    session_factory = AsyncMock(return_value=session)
    # Mocked, but still running the attempt, so the body the session served is
    # the body the test sees.
    retry_policy = MagicMock()
    retry_policy.execute = AsyncMock(wraps=run_once)

    fetcher = AiohttpWebPageFetcher(
        TIMEOUT_SECONDS,
        retry_policy,
        session_factory=session_factory,
        middlewares=middlewares,
    )
    return Rig(fetcher=fetcher, session=session, session_factory=session_factory)


def make_failing_session(status: int) -> MagicMock:
    """Build a mocked session that answers every request with a non-2xx status."""
    response = MagicMock()
    response.status = status
    response.text = AsyncMock(return_value=BODY)
    # `async with` must yield the very response the fetcher reads a status from.
    response.__aenter__.return_value = response

    session = MagicMock()
    session.get.return_value = response
    session.close = AsyncMock()
    return session


def make_retrying_fetcher(session: MagicMock) -> Rig:
    """Build the real fetcher over a mocked session and the real retry policy."""
    session_factory = AsyncMock(return_value=session)

    fetcher = AiohttpWebPageFetcher(
        TIMEOUT_SECONDS,
        ExponentialBackoffRetryPolicy(NO_WAIT),
        session_factory=session_factory,
    )
    return Rig(fetcher=fetcher, session=session, session_factory=session_factory)


async def test_a_200_answer_is_returned_as_the_page_body() -> None:
    """A 200 is decoded and handed back, and the session was asked for the URL."""
    rig = make_fetcher()

    assert await rig.fetcher.fetch(PAGE) == BODY
    rig.session.get.assert_called_once_with(URL, headers={})


async def test_the_middlewares_reach_the_headers_that_were_sent() -> None:
    """What a middleware adds is what leaves with the request."""
    rig = make_fetcher(middlewares=[HeadersMiddleware({"X-Custom": "1"})])

    await rig.fetcher.fetch(PAGE)

    rig.session.get.assert_called_once_with(URL, headers={"X-Custom": "1"})


async def test_one_session_serves_every_fetch() -> None:
    """The session is built once and pooled, so the factory runs a single time."""
    rig = make_fetcher()

    await rig.fetcher.fetch(PAGE)
    await rig.fetcher.fetch(PAGE)

    assert rig.session_factory.call_count == 1


async def test_close_awaits_the_session_close() -> None:
    """Releasing the fetcher releases the session it opened."""
    rig = make_fetcher()
    await rig.fetcher.fetch(PAGE)

    await rig.fetcher.close()

    rig.session.close.assert_awaited_once()


@pytest.mark.parametrize("status", [THROTTLED_STATUS, UNAVAILABLE_STATUS])
async def test_a_retryable_status_uses_the_whole_attempt_budget(
    status: int,
) -> None:
    """A transient answer is asked again, and the last one still reaches the caller."""
    rig = make_retrying_fetcher(make_failing_session(status))

    with pytest.raises(RetryableStatusError):
        await rig.fetcher.fetch(PAGE)

    assert rig.session.get.call_count == ATTEMPTS


async def test_a_non_retryable_status_spends_a_single_attempt() -> None:
    """A final answer is not backed off, so the budget is left untouched."""
    rig = make_retrying_fetcher(make_failing_session(FINAL_STATUS))

    with pytest.raises(NonRetryableError):
        await rig.fetcher.fetch(PAGE)

    assert rig.session.get.call_count == 1


@pytest.mark.parametrize("status", [UNAVAILABLE_STATUS, FINAL_STATUS])
async def test_a_failing_status_never_reads_the_body(status: int) -> None:
    """The status decides the outcome, so the body of a failed answer is left unread."""
    session = make_failing_session(status)
    response = session.get.return_value
    rig = make_retrying_fetcher(session)

    with pytest.raises((RetryableStatusError, NonRetryableError)):
        await rig.fetcher.fetch(PAGE)

    response.text.assert_not_awaited()


async def test_a_transport_error_uses_the_whole_attempt_budget() -> None:
    """A session that cannot even ask is worth the same number of attempts."""
    session = make_failing_session(UNAVAILABLE_STATUS)
    session.get.side_effect = TRANSPORT_ERROR
    rig = make_retrying_fetcher(session)

    with pytest.raises(aiohttp.ClientError) as caught:
        await rig.fetcher.fetch(PAGE)

    assert rig.session.get.call_count == ATTEMPTS
    assert caught.value is TRANSPORT_ERROR


async def test_a_retryable_status_error_names_the_url_and_the_status() -> None:
    """The error carries the page asked for and the answer that came back."""
    rig = make_retrying_fetcher(make_failing_session(UNAVAILABLE_STATUS))

    with pytest.raises(RetryableStatusError) as caught:
        await rig.fetcher.fetch(PAGE)

    assert caught.value.url == URL
    assert caught.value.reason == f"the site answered with status {UNAVAILABLE_STATUS}"
