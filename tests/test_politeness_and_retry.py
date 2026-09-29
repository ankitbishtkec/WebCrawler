"""Tests for `NoOpPolitenessPolicy` and `ExponentialBackoffRetryPolicy`.

The retry loop is checked for its outcome and its attempt count, not for the
wall clock: the backoff is a millisecond and the deadline is small, so the
suite stays deterministic and needs no faked clock.

Every test is `async def` and runs on the one event loop `pytest-asyncio` gives
it, so the per-attempt deadline is timed by that loop and never by a fresh one.
"""

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import replace
from functools import partial

import pytest

from tests.support import NOW, FakeFetcher
from webcrawler.domain.base_result import BaseResult
from webcrawler.domain.custom_url import CustomURL
from webcrawler.domain.errors import NonRetryableError, RetryableStatusError
from webcrawler.domain.retry_settings import RetrySettings
from webcrawler.infrastructure.politeness.no_op_politeness_policy import (
    NoOpPolitenessPolicy)
from webcrawler.infrastructure.retry.exponential_backoff_retry_policy import (
    ExponentialBackoffRetryPolicy)

URL: str = "https://example.com/a"

# Three attempts with a millisecond backoff, so a retry costs nothing to wait for.
FAST: RetrySettings = RetrySettings(
    max_attempts=3,
    base_delay_seconds=0.001,
    max_delay_seconds=0.002,
    jitter_seconds=0.0,
    timeout_seconds=5.0,
)


def counting_operation(
    error: BaseException, failures: int, result: str = "body"
) -> tuple[Callable[[], Awaitable[str]], list[int]]:
    """Build an operation that fails a set number of times and counts its calls.

    No shared fake can fail twice and then succeed, so this builds that one.

    Args:
    error: The exception raised by every failing call.
    failures: How many leading calls fail before the operation succeeds.
    result: The value returned once those failures are used up.

    Returns:
    tuple[Callable[[], Awaitable[str]], list[int]]: The zero-argument operation,
    and the one-element list holding its call count, which is the attempt count
    the policy spent.
    """
    calls: list[int] = [0]

    async def operation() -> str:
        calls[0] += 1
        if calls[0] <= failures:
            raise error
        return result

    return operation, calls


def sync_operation() -> Awaitable[str]:
    """Hand back a fresh awaitable from a plain function, as an adapter does.

    Returns:
    Awaitable[str]: A coroutine that resolves to `BODY`.
    """
    return asyncio.sleep(0, result="body")


async def async_operation() -> str:
    """Resolve straight to the body, as a coroutine function does.

    Returns:
    str: `BODY`, which the policy must hand back untouched.
    """
    return "body"


@pytest.mark.parametrize(
    "url",
    [
        "https://example.com/a",
        "https://example.com/b?a=1&a=3",
        "http://example.com:8080/c",
    ],
)
async def test_before_fetch_reports_no_wait_for_any_url(url: str) -> None:
    """The shipped default never delays a crawl, whatever the URL is.

    Args:
    url: The URL text about to be fetched; the wait must not depend on it.

    Returns:
    None
    """
    assert await NoOpPolitenessPolicy().before_fetch(CustomURL(url)) == 0


@pytest.mark.parametrize("is_success", [True, False])
async def test_record_fetch_returns_none_and_leaves_the_wait_unchanged(
    is_success: bool,
) -> None:
    """A completed attempt is accepted and discarded, however it ended.

    Args:
    is_success: How the handed-out fetch ended, which the policy must ignore.

    Returns:
    None
    """
    policy = NoOpPolitenessPolicy()
    result = BaseResult(is_success=is_success)

    # Phase one: the completed attempt is accepted and discarded.
    assert await policy.record_fetch(NOW, CustomURL(URL), result) is None

    # Phase two: the wait it would report is still zero afterwards.
    assert await policy.before_fetch(CustomURL(URL)) == 0


@pytest.mark.parametrize("body", ["<html>ok</html>", "", "no markup"])
async def test_successful_operation_returns_the_body_on_the_first_attempt(
    body: str,
) -> None:
    """An operation that works is returned as it came, with no retry.

    Args:
    body: The body the fetcher serves, which must come back untouched.

    Returns:
    None
    """
    operation = partial(FakeFetcher(bodies={URL: body}).fetch, CustomURL(URL))

    assert await ExponentialBackoffRetryPolicy(FAST).execute(operation) == body


@pytest.mark.parametrize("failures", [1, 2])
async def test_transient_failure_is_retried_until_the_operation_succeeds(
    failures: int,
) -> None:
    """A retryable failure is backed off and asked again inside the budget.

    Args:
    failures: How many attempts fail before one succeeds, under three attempts.

    Returns:
    None
    """
    operation, _ = counting_operation(
        RetryableStatusError(URL, "503"), failures
    )

    assert await ExponentialBackoffRetryPolicy(FAST).execute(operation) == "body"


@pytest.mark.parametrize(
    "error",
    [RetryableStatusError(URL, "503"), ValueError("boom")],
    ids=["retryable_status_error", "plain_error"],
)
async def test_exhausted_attempts_reraise_the_last_error(error: BaseException) -> None:
    """The real cause reaches the caller once the attempt budget is spent.

    Args:
    error: The failure every attempt raises, so the re-raised one is checkable.

    Returns:
    None
    """
    operation, _ = counting_operation(error, failures=FAST.max_attempts)

    with pytest.raises(type(error)) as raised:
        await ExponentialBackoffRetryPolicy(FAST).execute(operation)

    assert raised.value is error


async def test_non_retryable_error_is_raised_on_the_first_attempt() -> None:
    """A final answer ends the operation at once, spending no backoff on it.

    Returns:
    None
    """
    operation, calls = counting_operation(
        NonRetryableError(URL, "404"), failures=FAST.max_attempts
    )

    with pytest.raises(NonRetryableError):
        await ExponentialBackoffRetryPolicy(FAST).execute(operation)

    assert calls == [1]


@pytest.mark.parametrize(
    "operation",
    [sync_operation, async_operation],
    ids=["sync_callable", "async_callable"],
)
async def test_value_returned_is_whichever_the_operation_returned(
    operation: Callable[[], Awaitable[str]]
) -> None:
    """The policy hands back the operation's own value, for either callable.

    Args:
    operation: A plain function returning an awaitable, and a coroutine
    function; both are the shape the port accepts.

    Returns:
    None
    """
    assert await ExponentialBackoffRetryPolicy(FAST).execute(operation) == "body"


@pytest.mark.parametrize("timeout_seconds", [0.01, 0.05])
async def test_operation_overrunning_the_deadline_raises_timeout(
    timeout_seconds: float,
) -> None:
    """A hung attempt is abandoned by the per-attempt deadline.

    Args:
    timeout_seconds: The deadline in force, small enough to stay cheap.

    Returns:
    None
    """
    settings = replace(FAST, max_attempts=1, timeout_seconds=timeout_seconds)

    async def operation() -> str:
        # Waits on an event nobody sets, so the attempt cannot finish and the
        # deadline is what ends it; a fixed sleep would only be a number that
        # has to be kept larger than the deadline by hand.
        await asyncio.Event().wait()
        return "too late"

    with pytest.raises(asyncio.TimeoutError):
        await ExponentialBackoffRetryPolicy(settings).execute(operation)
