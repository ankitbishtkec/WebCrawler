"""The default page fetcher: `aiohttp`, a natively async client.

`goal.md:11` asks for an interface with a simple default implementation and
`goal.md:16` forbids bypassing it, so this is the only transport the worker
uses. Three decisions are structural rather than incidental:

- `aiohttp` is the client: it awaits its sockets on the
single event loop of `goal.md:13`, so no thread bridge is started here.
- The attempt is bounded in two places: the session is built with
`aiohttp.ClientTimeout(total=RetrySettings.timeout_seconds)`, which
bounds the socket side, and the per-call `RetryPolicy` bounds the await
side with its per-attempt deadline.
- Retry belongs to the caller: this class holds no attempt counter and no
sleep, it hands one attempt to the injected `RetryPolicy` per call, which
keeps the backoff numbers in one place (`goal.md:141`).

A redirect is followed wherever the server sends it, so a same-host page could
in principle lead off-host. Closing that would mean a redirect-blocking
handler, and the crawler scope of `goal.md:1` is stated over links, not over
server responses.
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

    The class extends the `WebPageFetcher` ABC and is extended by nothing, which
    is what keeps the worker's transport substitutable (`goal.md:8`).

    Args:
    timeout_seconds: The socket-side bound, handed to the client as
    `aiohttp.ClientTimeout(total=...)`. It is
    `RetrySettings.timeout_seconds`, so an attempt the policy abandons
    still ends on its own instead of holding a socket for ever.
    logger: The injected logger. Every attempt is recorded at INFO, which is
    what `goal.md:141` asks for.
    session_factory: Builds one session per `fetch` call as an async context
    manager. The default opens a real `aiohttp.ClientSession`; tests
    inject a fake so no socket is ever opened.
    middlewares: Applied, in order, to each request's headers just before
    it is sent, so auth and header policies compose without the fetcher
    knowing any of them. Empty by default, so a request then carries
    no header the caller did not ask for.
    """

    def __init__(
        self,
        timeout_seconds: float,
        logger: logging.Logger,
        *,
        session_factory: Callable[[], AsyncContextManager[aiohttp.ClientSession]]
        | None = None,
        middlewares: Sequence[RequestMiddleware] =(),
        ) -> None:
        """Hold the timeout, the logger, the session factory, and middlewares.

        Args:
        timeout_seconds: The socket-side bound, in seconds.
        logger: The injected logger, the only one this class writes to.
        session_factory: The session seam; the default builds a real
        `aiohttp.ClientSession` when this is None.
        middlewares: The header middlewares applied to every request, in
        the order given.
        """
        self._timeout_seconds = timeout_seconds
        self._logger = logger
        self._middlewares = tuple(middlewares)
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
        # The session timeout bounds the socket side, independent of the retry
        # policy that bounds the await side.
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=self._timeout_seconds)
            ) as session:
            yield session

    async def fetch(self, url: CustomURL, retry_policy: RetryPolicy) -> str:
        """Return one page's body, retried by the policy this call is given.

        The policy is asked for its timeout, backoff, and jitter and is handed a
        single attempt as a callable, so it can bound that attempt and start a
        fresh one; this class therefore cannot retry on its own and cannot
        swallow a failure (`goal.md:141`).

        Args:
        url: The page to retrieve, already canonical.
        retry_policy: The per-call timeout, backoff, and jitter. The same
        instance may be shared with the store and the worker, so every
        I/O in the process retries with the same settings.

        Returns:
        str: The decoded body of a 2xx response.

        Raises:
        aiohttp.ClientResponseError: If a response arrived with a non-2xx
        status, once the policy has spent its attempts.
        aiohttp.ClientError: For a transport failure raised by the client,
        once the policy has spent its attempts.
        OSError: For a DNS, socket, or timeout failure raised below the
        client, once the policy has spent its attempts.
        """
        target = url.get_url()
        # One session per call keeps this stateless, so it needs no close; a
        # production crawler would reuse a long-lived one and own its lifecycle.
        async with self._session_factory() as session:

            async def attempt() -> str:
                """Run one fetch, recording it whether it succeeds or not.

                The record is emitted before the request is started, so an
                attempt the policy abandons on its deadline is still visible in
                the log (`goal.md:141`).

                Returns:
                str: The decoded body of a 2xx response.

                Raises:
                NonRetryableError: For a non-2xx status. The site has
                answered, so the policy must not spend a backoff on
                it — a 503 from a site that refuses this client
                included.
                aiohttp.ClientError: For a transport failure, which is
                transient and so is retried.
                OSError: For a failure raised below the client.
                """
                self._logger.info("fetch attempt for %s", target)
                # The built-in agent goes first so an unauthenticated crawl is
                # still identified; a middleware may override it.
                headers: dict[str, str] = {"User-Agent": DEFAULT_USER_AGENT}
                for middleware in self._middlewares:
                    middleware.apply(url, headers)
                try:
                    async with session.get(target, headers=headers) as response:
                        status = response.status
                        if not FIRST_SUCCESS_STATUS <= status < FIRST_FAILURE_STATUS:
                            raise NonRetryableError(
                                url.get_url(),
                                f"the site answered with status {status}",
                            )
                        body = await response.text(
                            encoding=BODY_ENCODING, errors="replace"
                        )
                except Exception as error:  # noqa: BLE001 - logged, then re-raised
                    self._logger.info(
                        "fetch of %s failed: %s: %s",
                        target,
                        type(error).__name__,
                        error,
                    )
                    raise
                self._logger.info("fetched %s: %d characters", target, len(body))
                return body

            # Inside the session block: the policy runs every attempt against a
            # session that is still open.
            return await retry_policy.execute(attempt)

