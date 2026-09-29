"""The default page fetcher: `aiohttp`, a natively async client.

It is the only transport the worker uses, and it bounds each attempt twice: the session owns the socket side, the
injected `RetryPolicy` the await side. One session is built on the first fetch and shared by every call after it,
so the connection pool is reused and the caller releases it with `close`.
"""

import asyncio
import logging
from collections.abc import Awaitable, Callable, Sequence

import aiohttp

from webcrawler.domain.custom_url import CustomURL
from webcrawler.domain.errors import NonRetryableError, RetryableStatusError
from webcrawler.ports.request_middleware import RequestMiddleware
from webcrawler.ports.retry_policy import RetryPolicy
from webcrawler.ports.web_page_fetcher import WebPageFetcher

# Transient answers: the site is being asked for the same request again, so a backoff can fix it.
RETRYABLE_STATUS_CODES: frozenset[int] = frozenset({408, 425, 429, 500, 502, 503, 504})

BODY_ENCODING: str = "utf-8"
FIRST_SUCCESS_STATUS: int = 200
FIRST_FAILURE_STATUS: int = 300


class AiohttpWebPageFetcher(WebPageFetcher):
    """Fetches one page per call through the native-async `aiohttp` client.

    The class extends the `WebPageFetcher` ABC and pools one session for its whole lifetime, so the
    caller that owns it releases it with `close`.
    """

    def __init__(
        self,
        timeout_seconds: float,
        retry_policy: RetryPolicy,
        *,
        session_factory: Callable[[], Awaitable[aiohttp.ClientSession]] | None = None,
        middlewares: Sequence[RequestMiddleware] = (),
    ) -> None:
        """Hold the timeout, the policy, the lazy session, and the middlewares.

        Args:
        timeout_seconds: The socket-side bound, in seconds.
        retry_policy: Applied to every attempt here, so a caller's transport errors are retried without the caller wrapping the call.
        session_factory: The session seam; the default builds a real `aiohttp.ClientSession` when this is None.
        middlewares: The header middlewares applied to every request, in the order given.
        """
        self._logger = logging.getLogger(__name__)
        self._timeout_seconds = timeout_seconds
        self._retry_policy = retry_policy
        # Applied in order to each request's headers just before the send, so auth and
        # header policies compose without this class knowing them.
        self._middlewares = tuple(middlewares)
        # The default opens a real `aiohttp.ClientSession`; tests inject a fake, so no
        # socket is ever opened.
        self._session_factory: Callable[[], Awaitable[aiohttp.ClientSession]] = (
            session_factory
            if session_factory is not None
            else self._default_session_factory
        )
        # Built on the first fetch and released by `close`, so every call shares one pool.
        self._session: aiohttp.ClientSession | None = None
        # The worker crawls a batch concurrently, so two tasks can find no session at
        # once; the lock keeps the loser from opening a second one nobody will close.
        self._session_lock = asyncio.Lock()

    async def _default_session_factory(self) -> aiohttp.ClientSession:
        """Build one client session bounded by the constructor timeout.

        Returns:
        aiohttp.ClientSession: An open session; the caller releases it through `close`.
        """
        return aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=self._timeout_seconds)
        )

    async def _get_session(self) -> aiohttp.ClientSession:
        """Return the shared session, building it on the first call that needs one.

        Returns:
        aiohttp.ClientSession: The one session that every call reuses.
        """
        if self._session is None:
            async with self._session_lock:
                if self._session is None:
                    self._session = await self._session_factory()
        return self._session

    async def close(self) -> None:
        """Close the pooled session, so a later fetch builds a fresh one.

        Closing a session that was never opened does nothing.

        Raises:
        Exception: A networked implementation may raise while closing its client; the shipped one never does.
        """
        async with self._session_lock:
            session = self._session
            self._session = None
        # Released outside the lock, so a slow close cannot block a fetch that is
        # opening the next session.
        if session is not None:
            await session.close()

    async def fetch(self, url: CustomURL) -> str:
        """Return one page's body, retried by the policy given at construction.

        The policy owns the attempts, so this class never retries or swallows one of its own.

        Args:
        url: The page to retrieve, already canonical.

        Returns:
        str: The decoded body of a 2xx response.

        Raises:
        RetryableStatusError: For a status in `RETRYABLE_STATUS_CODES`, so the policy backs off and asks again.
        NonRetryableError: For any other non-2xx status, since the site has given a final answer.
        Exception: Possibly a aiohttp.ClientError: For a transport failure or OSError.
        """
        target = url.get_url()
        # One session is built on the first call and reused, so the pool is shared.
        session = await self._get_session()

        async def attempt() -> str:
            """Run one fetch, recording it whether it succeeds or not.

            Recorded before the request starts, so a deadline is still logged.

            Returns:
            str: The decoded body of a 2xx response.

            Raises:
            RetryableStatusError: For a status in `RETRYABLE_STATUS_CODES`, so the policy backs off and asks again.
            NonRetryableError: For any other non-2xx status, since the site has given a final answer.
            Exception: Possibly a aiohttp.ClientError: For a transport failure or OSError.
            """
            self._logger.debug("fetch attempt for %s", target)
            headers: dict[str, str] = dict()
            for middleware in self._middlewares:
                middleware.apply(url, headers)
            try:
                # Redirects are followed wherever the server sends them, so
                # a same-host page can lead off-host.
                async with session.get(target, headers=headers) as response:
                    status = response.status
                    if status in RETRYABLE_STATUS_CODES:
                        raise RetryableStatusError(
                            url.get_url(),
                            f"the site answered with status {status}",
                        )
                    if not FIRST_SUCCESS_STATUS <= status < FIRST_FAILURE_STATUS:
                        raise NonRetryableError(
                            url.get_url(),
                            f"the site answered with status {status}",
                        )
                    body = await response.text(
                        encoding=BODY_ENCODING, errors="replace"
                    )
            except Exception as error:  # noqa: BLE001 - logged, then re-raised
                self._logger.error(
                    "fetch of %s failed: %s: %s",
                    target,
                    type(error).__name__,
                    error,
                )
                raise
            self._logger.debug("fetched %s: %d characters", target, len(body))
            return body

        return await self._retry_policy.execute(attempt)
