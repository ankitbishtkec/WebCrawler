"""Tests for `AiohttpWebPageFetcher`, the real `aiohttp` transport.

The `session_factory` seam is the only thing replaced, so the status mapping, the
retry hand-off, and the pooled-session lifetime are the production code's own; no
socket is opened and no assertion is made about the headers or the logs.

Every test is `async def` and runs on the one event loop `pytest-asyncio` gives
it, so a fetcher that holds a lock is never carried across two loops.
"""

import pytest

from tests.support import FAST_RETRY
from webcrawler.domain.custom_url import CustomURL
from webcrawler.domain.errors import NonRetryableError, RetryableStatusError
from webcrawler.infrastructure.fetch.aiohttp_web_page_fetcher import (
    AiohttpWebPageFetcher)
from webcrawler.infrastructure.retry.exponential_backoff_retry_policy import (
    ExponentialBackoffRetryPolicy)

URL: str = "https://crawlme.monzo.com/index.html"

# `CustomURL` is immutable, so one instance is safely shared by every test here.
PAGE: CustomURL = CustomURL(URL)

BODY: str = "<html>a page</html>"

# The fetcher's socket-side bound, inert here because the session is injected.
TIMEOUT_SECONDS: float = 5.0


class _CannedResponse:
    """One canned answer, in the shape the fetcher enters and reads.

    Args:
    status: The HTTP status every request of the owning session is answered with.
    body: The body that status carries.
    """

    def __init__(self, status: int, body: str) -> None:
        """Hold the status and the body, so no request is ever really made.

        Args:
        status: The HTTP status the fetch must read.
        body: The body a successful fetch must receive.
        """
        self.status = status
        self._body = body

    async def text(self, encoding: str, errors: str) -> str:
        """Return the canned body, since decoding it never changes it.

        Args:
        encoding: The encoding the fetcher asks for, accepted and ignored.
        errors: The error policy the fetcher asks for, accepted and ignored.

        Returns:
        str: The canned body.
        """
        return self._body

    async def __aenter__(self) -> "_CannedResponse":
        """Enter the response context, which needs nothing set up here.

        Returns:
        _CannedResponse: This response, so the fetcher can read `.status`.
        """
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        """Leave the response context, which needs nothing released here.

        Args:
        exc_info: The triple `async with` passes on an error, ignored.

        Returns:
        None
        """
        return None


class _CannedSession:
    """A session double answering every request with one canned response.

    A closed session refuses requests, as a real `aiohttp.ClientSession` does, so
    a fetch that reuses a released session fails loudly instead of passing by
    accident. Not in `support.py` because it is a double for this transport's own
    private seam, which no other test drives, and `FakeFetcher` stands in one
    layer above.

    Args:
    status: The HTTP status every request is answered with.
    body: The body every request carries.
    """

    def __init__(self, status: int, body: str) -> None:
        """Build the one response every request of this session gets.

        Args:
        status: The HTTP status every request is answered with.
        body: The body every request carries.
        """
        self._response = _CannedResponse(status, body)
        self._closed = False

    def get(self, url: str, headers: dict[str, str] | None = None) -> _CannedResponse:
        """Answer one request, since there is no socket to reach.

        Args:
        url: The target the fetcher asked for, ignored.
        headers: The headers the fetcher sent, ignored.

        Returns:
        _CannedResponse: The canned response, which the fetcher enters.

        Raises:
        RuntimeError: If this session was already closed, which is what a real
        client does with a request that arrives after its own release.
        """
        if self._closed:
            raise RuntimeError("the session was closed")
        return self._response

    async def close(self) -> None:
        """Mark the session closed, so a later request on it is refused.

        Returns:
        None
        """
        self._closed = True


def make_fetcher(
    status: int, body: str = BODY
) -> tuple[AiohttpWebPageFetcher, list[int]]:
    """Build the real fetcher over a canned session, counting its session builds.

    Args:
    status: The HTTP status the injected session answers every request with.
    body: The body that status carries.

    Returns:
    tuple[AiohttpWebPageFetcher, list[int]]: The fetcher, and the one-element list
    holding how many times the session factory ran, which is what shows the session
    is pooled rather than rebuilt.
    """
    builds: list[int] = [0]

    async def session_factory() -> _CannedSession:
        """Hand back a fresh canned session and record that one was built.

        Returns:
        _CannedSession: A session double. The seam is annotated for a real
        `aiohttp.ClientSession`, which is the shape this stands in for.
        """
        builds[0] += 1
        return _CannedSession(status, body)

    fetcher = AiohttpWebPageFetcher(
        TIMEOUT_SECONDS,
        ExponentialBackoffRetryPolicy(FAST_RETRY),
        session_factory=session_factory,
    )
    return fetcher, builds


@pytest.mark.parametrize("status", [200, 201], ids=["ok", "created"])
async def test_a_success_status_returns_the_body(status: int) -> None:
    """A 2xx answer is decoded and handed back as the page body.

    Args:
    status: The success status the injected session answers with.

    Returns:
    None
    """
    fetcher, _ = make_fetcher(status)

    assert await fetcher.fetch(PAGE) == BODY


@pytest.mark.parametrize("status", [408, 429, 500, 503])
async def test_a_retryable_status_surfaces_the_error_the_attempts_exhausted(
    status: int,
) -> None:
    """A transient status is retried inside the budget, then raised to the caller.

    Args:
    status: A status in `RETRYABLE_STATUS_CODES`, so the policy backs off on it.

    Returns:
    None
    """
    fetcher, _ = make_fetcher(status)

    with pytest.raises(RetryableStatusError):
        await fetcher.fetch(PAGE)


@pytest.mark.parametrize("status", [400, 403, 404, 501])
async def test_a_non_retryable_status_raises_on_the_first_attempt(status: int) -> None:
    """A final answer ends the fetch at once, so no backoff is spent on it.

    Args:
    status: A status that is neither 2xx nor in `RETRYABLE_STATUS_CODES`.

    Returns:
    None
    """
    fetcher, _ = make_fetcher(status)

    with pytest.raises(NonRetryableError):
        await fetcher.fetch(PAGE)


async def test_a_fetch_after_close_still_returns_the_body() -> None:
    """`close` releases the session, so a later fetch opens a fresh one.

    Returns:
    None
    """
    # Phase one: fetch, on the session the fetcher builds for itself.
    fetcher, _ = make_fetcher(200)
    before = await fetcher.fetch(PAGE)

    # Phase two: release that session, then fetch the same page again.
    await fetcher.close()
    after = await fetcher.fetch(PAGE)

    assert (before, after) == (BODY, BODY)


async def test_close_without_a_fetch_does_not_raise() -> None:
    """A fetcher that opened no session is still closable.

    Returns:
    None
    """
    fetcher, _ = make_fetcher(200)

    assert await fetcher.close() is None


async def test_the_session_is_built_once_and_reused_across_fetches() -> None:
    """One session serves every call, so the connection pool is actually shared.

    Returns:
    None
    """
    fetcher, builds = make_fetcher(200)

    # Both fetches are on the one loop this test runs, so neither forces a new one.
    first = await fetcher.fetch(PAGE)
    second = await fetcher.fetch(PAGE)

    assert (first, second) == (BODY, BODY)
    assert builds == [1]
