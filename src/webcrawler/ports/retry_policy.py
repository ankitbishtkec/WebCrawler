"""The retry port (goal.md:17).

`goal.md:17` requires every implementation that performs I/O to own its retry
with exponential backoff, jitter, and a timeout. This port is that ownership in
one place: the backoff numbers live in `RetrySettings`, the loop lives in the
single implementation injected into the state store and the worker that hands
it to the fetcher.
"""

from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from typing import TypeVar

T = TypeVar("T")

class RetryPolicy(ABC):
    """Runs one fallible operation with a deadline and exponential backoff.

    Whether a failure is worth retrying is the operation's call, not the
    policy's: an operation that has been given a final answer — a 503 from a
    site that refuses this client, a 404 — raises `NonRetryableError` and the
    policy re-raises it on the first attempt without spending a backoff.
    Everything else, a transport error or a timeout, is retried.
    """

@abstractmethod
async def execute(self, operation: Callable[[], Awaitable[T]]) -> T:
    """Await the operation, retrying it according to the policy.

    `operation` is a zero-argument callable rather than a coroutine so each
    attempt is a fresh awaitable, and so the policy can bound one attempt
    with a timeout while the operation itself knows nothing about retries.

    Args:
    operation: A callable returning a new awaitable per attempt. The
    same callable is reused for every attempt, so it must be safe
    to call more than once. It signals a final failure by raising
    `NonRetryableError`.

    Returns:
    T: The first successful result, returned without re-running the
    operation.

    Raises:
    NonRetryableError: Whatever the operation raised, re-raised on the
    first attempt without retrying.
    Exception: The last error, re-raised once the attempts are
    exhausted, so the caller sees the real cause rather than a
    wrapper. A timeout abandons only the await, which is why a
    blocking implementation must bound itself with a socket
    timeout.
    """
