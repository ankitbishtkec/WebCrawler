"""The default retry policy: exponential backoff, jitter, and a per-attempt deadline.

Every implementation that performs I/O owns its retry with this one loop, and
the store and the fetcher are handed the same instance, so the numbers in
`RetrySettings` are read in a single place.
"""

import asyncio
import random  # the jitter source, and the only seam a test patches
from collections.abc import Awaitable, Callable

from typing import TypeVar

from webcrawler.domain.errors import NonRetryableError
from webcrawler.domain.retry_settings import RetrySettings
from webcrawler.ports.retry_policy import RetryPolicy

T = TypeVar("T")

NO_ATTEMPTS_MESSAGE: str = "RetrySettings.max_attempts must be at least 1"

class ExponentialBackoffRetryPolicy(RetryPolicy):
    """Runs one fallible operation with a deadline per attempt and a backoff.

    The class extends the `RetryPolicy` ABC and is extended by nothing, which is
    what keeps every collaborator's retry substitutable. The settings are read
    per call, so one instance serves both the store and the fetcher.
    """

    def __init__(self, settings: RetrySettings) -> None:
        """Hold the settings; no clock is read and nothing is scheduled.

        Args:
        settings: The numbers every `execute` call applies, read from the
        frozen value type rather than copied into fields.
        """
        self._settings = settings

    async def execute(self, operation: Callable[[], Awaitable[T]]) -> T:
        """Run `operation` until it succeeds or the attempt budget is spent.

        The wait is taken *between* attempts, never after the last one.

        Args:
        operation: A zero-argument callable returning a new awaitable per
        attempt, so the same callable is safe to call again.

        Returns:
        T: The first successful result, returned without re-running the
        operation.

        Raises:
        Exception: The last error, once `max_attempts` attempts have
        failed, so the caller sees the real cause rather than a wrapper. A deadline abandons only the await, which is why
        a blocking implementation bounds itself separately.
        ValueError: If `max_attempts` is below 1, which would otherwise
        report success for work that was never attempted.
        """
        last_error: BaseException | None = None
        # The deadline is per attempt, not for the whole operation, so one hung
        # attempt cannot consume the budget of the attempts that follow it.
        for attempt in range(self._settings.max_attempts):
            try:
                # `operation` is a zero-argument callable, not a coroutine, so
                # each attempt awaits a fresh awaitable: a coroutine cannot be
                # awaited twice, and reusing one carries the previous state.
                return await asyncio.wait_for(
                    operation(), self._settings.timeout_seconds
                )
            except NonRetryableError:
                # A final answer, such as a 404 or a 410, is not worth a backoff.
                # 408, 425, 429, 500, 502, 503 and 504 are retried, so they reach here.
                raise
            except Exception as error:  # noqa: BLE001 - re-raised below
                last_error = error
                if attempt + 1 >= self._settings.max_attempts:
                    break
                await self._wait_before_retry(attempt)
        if last_error is not None:
            raise last_error
        raise ValueError(NO_ATTEMPTS_MESSAGE)

    async def _wait_before_retry(self, attempt: int) -> None:
                            """Sleep the backoff owed after the attempt that just failed.

                            The cap applies to the exponential term only, so the total wait is at most `max_delay_seconds + jitter_seconds`.

                            Args:
                            attempt: The zero-based number of the attempt that failed, which
                            selects `base_delay_seconds * 2 ** attempt` before the cap.
                            """
                            exponential = min(
                                self._settings.base_delay_seconds * 2**attempt,
                                self._settings.max_delay_seconds)
                            # The cap bounds the growth however many attempts are
                            # configured, and the spread de-synchronises workers
                            # whose attempts failed at the same moment.
                            jitter = random.uniform(0.0, self._settings.jitter_seconds)
                            await asyncio.sleep(exponential + jitter)
