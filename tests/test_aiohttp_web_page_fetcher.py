"""Happy-path tests for `AiohttpWebPageFetcher`, the real `aiohttp` transport.

The fetcher's only collaborator is the aiohttp session, so the session is a
`MagicMock` and the retry policy a mock that runs the one attempt it is given.
No socket is opened, and every assertion is on what the fetcher asked the
session for.

Every test is `async def` and runs on the one event loop `pytest-asyncio` gives
it, so a fetcher that holds a lock is never carried across two loops.
"""

from collections.abc import Awaitable, Callable, Sequence
from typing import NamedTuple
from unittest.mock import AsyncMock, MagicMock

from webcrawler.domain.custom_url import CustomURL
from webcrawler.infrastructure.fetch.aiohttp_web_page_fetcher import (
    AiohttpWebPageFetcher)
from webcrawler.infrastructure.fetch.headers_middleware import HeadersMiddleware
from webcrawler.ports.request_middleware import RequestMiddleware

URL: str = "https://crawlme.monzo.com/index.html"

# `CustomURL` is immutable, so one instance is safely shared by every test here.
PAGE: CustomURL = CustomURL(URL)

BODY: str = "<html>a page</html>"

# The status the mocked session answers with, the only one on the happy path.
SUCCESS_STATUS: int = 200

# The fetcher's socket-side bound, inert here because the session is injected.
TIMEOUT_SECONDS: float = 5.0


class Rig(NamedTuple):
    """The system under test together with the mocks it was built over.

    Args:
    fetcher: The real fetcher, wired to the mocks below.
    session: The mocked session every request is answered by.
    session_factory: The mocked seam the fetcher builds its session through.
    """

    fetcher: AiohttpWebPageFetcher
    session: MagicMock
    session_factory: AsyncMock


async def run_once(operation: Callable[[], Awaitable[str]]) -> str:
    """Run the operation a single time, which is all a success needs.

    Args:
    operation: The one attempt the fetcher handed over.

    Returns:
    str: Whatever that attempt returned.
    """
    return await operation()


def make_session(body: str = BODY) -> MagicMock:
    """Build a mocked session that answers every request with a 200.

    Args:
    body: The body the canned 200 carries.

    Returns:
    MagicMock: A session whose `get` hands back a response the fetcher can read.
    """
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
    """Build the real fetcher over a mocked session and a mocked retry policy.

    Args:
    middlewares: The request middlewares applied to every outgoing request.

    Returns:
    Rig: The fetcher, plus the session and the session factory it holds.
    """
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


async def test_a_200_answer_is_returned_as_the_page_body() -> None:
    """A 200 is decoded and handed back, and the session was asked for the URL.

    Returns:
    None
    """
    rig = make_fetcher()

    assert await rig.fetcher.fetch(PAGE) == BODY
    rig.session.get.assert_called_once_with(URL, headers={})


async def test_the_middlewares_reach_the_headers_that_were_sent() -> None:
    """What a middleware adds is what leaves with the request.

    Returns:
    None
    """
    rig = make_fetcher(middlewares=[HeadersMiddleware({"X-Custom": "1"})])

    await rig.fetcher.fetch(PAGE)

    rig.session.get.assert_called_once_with(URL, headers={"X-Custom": "1"})


async def test_one_session_serves_every_fetch() -> None:
    """The session is built once and pooled, so the factory runs a single time.

    Returns:
    None
    """
    rig = make_fetcher()

    await rig.fetcher.fetch(PAGE)
    await rig.fetcher.fetch(PAGE)

    assert rig.session_factory.call_count == 1


async def test_close_awaits_the_session_close() -> None:
    """Releasing the fetcher releases the session it opened.

    Returns:
    None
    """
    rig = make_fetcher()
    await rig.fetcher.fetch(PAGE)

    await rig.fetcher.close()

    rig.session.close.assert_awaited_once()
