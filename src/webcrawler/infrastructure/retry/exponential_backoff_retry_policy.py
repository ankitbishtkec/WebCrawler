"""The default retry policy: a per-attempt deadline, backoff, and jitter.

`goal.md:17` requires every implementation that performs I/O to own its retry
with exponential backoff, jitter, and a timeout, and this is the one loop all of
them share: the store and the fetcher are handed the same instance, so the
numbers in `RetrySettings` are read in a single place.

Three decisions are structural rather than incidental:

- The wait before attempt `n`, counted from zero, is
`min(base_delay_seconds * 2 ** n, max_delay_seconds)` plus
`random.uniform(0.0, jitter_seconds)`. The cap bounds the growth however
many attempts are configured, and the spread de-synchronises workers whose
attempts failed at the same moment. `random` is the module imported here, so
it is the only seam a test has to patch to make the schedule exact.
- The deadline is per attempt and not for the whole operation, so one hung
attempt cannot consume the budget of the attempts that follow it.
- `operation` is a zero-argument callable rather than a coroutine, so every
attempt awaits a fresh awaitable: a coroutine object cannot be awaited
twice, and reusing one would carry the previous attempt's state into the
next.
"""

import asyncio
import random
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
    what keeps every collaborator's retry substitutable (`goal.md:8`).

    Args:
    settings: The attempt budget, backoff, jitter, and per-attempt deadline
    applied to every operation. The values are read per call, so one
    instance can be shared by the store and the fetcher without either
    copying the numbers.
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

        The wait is taken *between* attempts: a success on the first try never
        sleeps, and an exhausted budget is re-raised without a trailing delay
        the caller would wait out before it could even handle the error.

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
        for attempt in range(self._settings.max_attempts):
            try:
                return await asyncio.wait_for(
                    operation(), self._settings.timeout_seconds
                )
            except NonRetryableError:
                # The operation was given a final answer (a 503, a 404), so
                # a backoff would only delay the crawl: re-raise at once.
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

                            The cap applies to the exponential term only, so the total wait is at
                            most `max_delay_seconds + jitter_seconds`; a test therefore proves the
                            jitter bound rather than assuming it.

                            Args:
                            attempt: The zero-based number of the attempt that failed, which
                            selects `base_delay_seconds * 2 ** attempt` before the cap.
                            """
                            exponential = min(
                                self._settings.base_delay_seconds * 2**attempt,
                                self._settings.max_delay_seconds)
                            jitter = random.uniform(0.0, self._settings.jitter_seconds)
                            await asyncio.sleep(exponential + jitter)
