"""Happy-path and failing-attempt tests for `NoOpPolitenessPolicy` and
`ExponentialBackoffRetryPolicy`.

Both units are control flow over the values they are handed, so the only double
is the operation the retry policy is given. Nothing here measures time: the
delays the failing tests provoke are zero, so what they count is the attempts
and never the wait between them, and no clock is faked and no test can be flaky.

Every test is `async def` and runs on the one event loop `pytest-asyncio` gives
it, so the per-attempt deadline is timed by that loop and never by a fresh one.
"""

from collections.abc import Awaitable, Callable, Sequence
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from webcrawler.domain.base_result import BaseResult
from webcrawler.domain.custom_url import CustomURL
from webcrawler.domain.errors import NonRetryableError
from webcrawler.domain.retry_settings import RetrySettings
from webcrawler.infrastructure.politeness.no_op_politeness_policy import (
    NoOpPolitenessPolicy)
from webcrawler.infrastructure.retry.exponential_backoff_retry_policy import (
    ExponentialBackoffRetryPolicy)

URL: str = "https://example.com/a"

# `CustomURL` is immutable, so one instance is safely shared by every test here.
PAGE: CustomURL = CustomURL(URL)

BODY: str = "<html>a page</html>"

# The one instant handed to `record_fetch`, timezone-aware UTC.
NOW: datetime = datetime(2026, 9, 29, 6, 0, tzinfo=timezone.utc)

# Three attempts with a millisecond backoff, so the numbers are never a cost.
# The happy path below succeeds on the first attempt and never sleeps at all.
FAST: RetrySettings = RetrySettings(
    max_attempts=3,
    base_delay_seconds=0.001,
    max_delay_seconds=0.002,
    jitter_seconds=0.0,
    timeout_seconds=5.0,
)

# The same budget with no wait at all, so the failing tests below count the
# attempts and never the delay spent between them.
ATTEMPTS: int = 3
NO_WAIT: RetrySettings = RetrySettings(
    max_attempts=ATTEMPTS,
    base_delay_seconds=0.0,
    max_delay_seconds=0.0,
    jitter_seconds=0.0,
    timeout_seconds=5.0,
)

# A budget the policy refuses outright, since one attempt is the least that can
# report an honest answer.
NO_ATTEMPTS: RetrySettings = RetrySettings(
    max_attempts=0,
    base_delay_seconds=0.0,
    max_delay_seconds=0.0,
    jitter_seconds=0.0,
    timeout_seconds=5.0,
)


def make_failing_operation(
    error: Exception,
) -> tuple[Callable[[], Awaitable[str]], MagicMock]:
    """Build an operation that always fails, plus the counter that records it.

    The operation is a plain function, not one coroutine, because the policy
    calls it per attempt and a coroutine cannot be awaited twice.

    Args:
    error: The error every attempt raises, so the last one is the real cause.

    Returns:
    tuple: The operation to hand over, and a counter with one call per attempt.
    """
    attempts = MagicMock()

    async def attempt() -> str:
        attempts()
        raise error

    return attempt, attempts


def make_recovering_operation(
    values: Sequence[str],
) -> tuple[Callable[[], Awaitable[str]], MagicMock]:
    """Build an operation that fails until its last value is reached.

    Args:
    values: The value each attempt would return, the first only the last one wins.

    Returns:
    tuple: The operation to hand over, and a counter with one call per attempt.
    """
    attempts = MagicMock()

    async def attempt() -> str:
        served = attempts.call_count
        attempts()
        if served + 1 < len(values):
            raise RuntimeError(f"attempt {served + 1} failed")
        return values[served]

    return attempt, attempts


async def test_before_fetch_reports_no_wait() -> None:
    """The shipped default never delays a crawl, so the answer is a constant 0.

    Returns:
    None
    """
    assert await NoOpPolitenessPolicy().before_fetch(PAGE) == 0


async def test_record_fetch_accepts_a_result_and_records_nothing() -> None:
    """A completed attempt is accepted and dropped, since there is nothing to learn.

    Returns:
    None
    """
    policy = NoOpPolitenessPolicy()
    result = BaseResult(is_success=True)

    assert await policy.record_fetch(NOW, PAGE, result) is None


async def test_an_operation_that_works_runs_once_and_returns_its_value() -> None:
    """The value the operation returned is the value the caller gets back.

    Returns:
    None
    """
    operation = AsyncMock(return_value=BODY)

    result = await ExponentialBackoffRetryPolicy(FAST).execute(operation)

    assert result == BODY
    assert operation.await_count == 1


async def test_a_transient_failure_spends_every_attempt_and_reraises_the_last(
) -> None:
    """An ordinary failure is retried, and the caller sees the real cause.

    Returns:
    None
    """
    operation, attempts = make_failing_operation(RuntimeError("the site hung up"))

    with pytest.raises(RuntimeError) as caught:
        await ExponentialBackoffRetryPolicy(NO_WAIT).execute(operation)

    assert attempts.call_count == ATTEMPTS
    assert str(caught.value) == "the site hung up"


async def test_a_final_answer_spends_a_single_attempt() -> None:
    """`NonRetryableError` is the policy's own signal to stop at once.

    Returns:
    None
    """
    error = NonRetryableError(PAGE.get_url(), "the site answered with status 404")
    operation, attempts = make_failing_operation(error)

    with pytest.raises(NonRetryableError) as caught:
        await ExponentialBackoffRetryPolicy(NO_WAIT).execute(operation)

    assert attempts.call_count == 1
    assert caught.value is error


async def test_an_operation_that_recovers_returns_the_success_it_finished_on(
) -> None:
    """The first success ends the loop, and its value is what the caller gets.

    Returns:
    None
    """
    operation, attempts = make_recovering_operation(["first", "second", "third"])

    result = await ExponentialBackoffRetryPolicy(NO_WAIT).execute(operation)

    assert result == "third"
    assert attempts.call_count == ATTEMPTS


async def test_a_budget_below_one_never_runs_the_operation() -> None:
    """A budget of zero is refused rather than reported as a success.

    Returns:
    None
    """
    operation, attempts = make_failing_operation(RuntimeError("never reached"))

    with pytest.raises(ValueError):
        await ExponentialBackoffRetryPolicy(NO_ATTEMPTS).execute(operation)

    attempts.assert_not_called()
