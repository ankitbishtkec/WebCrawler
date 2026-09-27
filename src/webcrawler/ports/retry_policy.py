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
    policy's: an operation that has been given a final answer, such as a 503
    from a site that refuses this client, raises `NonRetryableError` and the
    policy re-raises it on the first attempt rather than spending backoff on a
    decision that will not change.
    """

    @abstractmethod
    async def execute(self, operation: Callable[[], Awaitable[T]]) -> T:
        """Run operation, retrying a transient failure with growing delays.

        Each attempt is bounded by the per-attempt deadline, so an operation
        that hangs is abandoned rather than holding the attempt budget open
        for ever.

        Args:
            operation: A callable returning a fresh awaitable per attempt, so
                nothing is reused between attempts.

        Returns:
            T: Whatever the successful attempt returned.

        Raises:
            NonRetryableError: The first attempt's final answer, re-raised at
                once with no backoff spent on it.
            Exception: The last failure, once the attempt budget is spent.
            asyncio.TimeoutError: If the final attempt exceeded the deadline.
        """
