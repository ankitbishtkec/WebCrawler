"""Happy-path tests for `NoOpPolitenessPolicy` and `ExponentialBackoffRetryPolicy`.

Both units are control flow over the values they are handed, so the only double
is the operation the retry policy is given. Nothing here measures time: an
operation that works first time never reaches the backoff, so no clock is faked
and no test can be flaky.

Every test is `async def` and runs on the one event loop `pytest-asyncio` gives
it, so the per-attempt deadline is timed by that loop and never by a fresh one.
"""

from datetime import datetime, timezone
from unittest.mock import AsyncMock

from webcrawler.domain.base_result import BaseResult
from webcrawler.domain.custom_url import CustomURL
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
