"""The default page fetcher: `aiohttp`, a natively async client.

It is the only transport the worker uses, and it bounds each attempt twice:
the session owns the socket side, the injected `RetryPolicy` the await side.
"""


import logging
from collections.abc import AsyncIterator, Callable, Sequence
from contextlib import AbstractAsyncContextManager as AsyncContextManager
from contextlib import asynccontextmanager

import aiohttp

from webcrawler.domain.custom_url import CustomURL
from webcrawler.domain.errors import NonRetryableError
from webcrawler.ports.request_middleware import RequestMiddleware
from webcrawler.ports.retry_policy import RetryPolicy
from webcrawler.ports.web_page_fetcher import WebPageFetcher

# A browser User-Agent: several sites answer 503 to a non-browser agent
# (leetcode among them), so the crawler identifies like a normal client.
DEFAULT_USER_AGENT: str = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

BODY_ENCODING: str = "utf-8"
FIRST_SUCCESS_STATUS: int = 200
FIRST_FAILURE_STATUS: int = 300


class AiohttpWebPageFetcher(WebPageFetcher):
    """Fetches one page per call through the native-async `aiohttp` client.

    The class extends the `WebPageFetcher` ABC.
    """

    def __init__(
        self,
        timeout_seconds: float,
        logger: logging.Logger,
        retry_policy: RetryPolicy,
        *,
        session_factory: Callable[[], AsyncContextManager[aiohttp.ClientSession]]
        | None = None,
        middlewares: Sequence[RequestMiddleware] =(),
        ) -> None:
        """Hold the timeout, the policy, the logger, the session, and middlewares.

        Args:
        timeout_seconds: The socket-side bound, in seconds.
        logger: The injected logger, the only one this class writes to.
        retry_policy: Applied to every attempt here, so a caller's transport
        errors are retried without the caller wrapping the call.
        session_factory: The session seam; the default builds a real
        `aiohttp.ClientSession` when this is None.
        middlewares: The header middlewares applied to every request, in
        the order given.
        """
        self._timeout_seconds = timeout_seconds
        self._logger = logger
        self._retry_policy = retry_policy
        # Applied in order to each request's headers just before the send, so
        # auth and header policies compose without this class knowing them.
        # Empty by default, so the user agent below is the only header sent.
        self._middlewares = tuple(middlewares)
        # The default opens a real `aiohttp.ClientSession`; tests inject a fake,
        # so no socket is ever opened.
        self._session_factory: Callable[[], AsyncContextManager[aiohttp.ClientSession]] = (
            session_factory
            if session_factory is not None
            else self._default_session_factory
            )

    @asynccontextmanager
    async def _default_session_factory(self) -> AsyncIterator[aiohttp.ClientSession]:
        """Yield one client session bounded by the constructor timeout.

        Returns:
        AsyncIterator[aiohttp.ClientSession]: One session; leaving the
        context manager closes it, so the fetcher owns no session
        between calls.
        """
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=self._timeout_seconds)
            ) as session:
            yield session

    async def fetch(self, url: CustomURL) -> str:
        """Return one page's body, retried by the policy given at construction.

        The policy owns the attempts, so this class never retries or swallows.

        Args:
        url: The page to retrieve, already canonical.

        Returns:
        str: The decoded body of a 2xx response.

        Raises:
        NonRetryableError: For a non-2xx status. The site has answered, so the policy must not spend a backoff on it, a 503 from a site that refuses this client included.
        Exception: Possibly a aiohttp.ClientError: For a transport failure or OSError.
        """
        target = url.get_url()
        # One session per call keeps this stateless, so it needs no close; a
        # production crawler would reuse a long-lived one and own its lifecycle.
        #ankit: do not create a new session for each fetch call. rather resuse same instance
        async with self._session_factory() as session:

            async def attempt() -> str:
                """Run one fetch, recording it whether it succeeds or not.

                Recorded before the request starts, so a deadline is still logged.

                Returns:
                str: The decoded body of a 2xx response.

                Raises:
                NonRetryableError: For a non-2xx status. The site has answered, so the policy must not spend a backoff on it, a 503 from a site that refuses this client included.
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
                        #ankit:this is too simplistic. list codes which should be retried.
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

