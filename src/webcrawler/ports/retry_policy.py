"""The retry port.

Every implementation that performs I/O must own its retry with exponential backoff, jitter,
and a timeout. This port is that ownership in one place: the numbers live in
`RetrySettings`, the loop in the single implementation injected into the store and worker.
"""

from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from typing import TypeVar

T = TypeVar("T")


class RetryPolicy(ABC):
    """Runs one fallible operation with a deadline and exponential backoff.

Whether a failure is worth retrying is the operation's call, not the policy's: an operation
given a final answer, such as a 503 from a site that refuses this client, raises
    `NonRetryableError`, which the policy re-raises at once.
    """


    @abstractmethod
    async def execute(self, operation: Callable[[], Awaitable[T]]) -> T:
        """Run operation, retrying a transient failure with growing delays.

        Each attempt is bounded by the per-attempt deadline, so a hanging operation is abandoned rather than holding the attempt budget open.

        Args:
            operation: A callable returning a fresh awaitable per attempt, so nothing is reused between attempts.

        Returns:
            T: Whatever the successful attempt returned.

        Raises:
            NonRetryableError: The first attempt's final answer, re-raised at once with no backoff spent on it.
            Exception: The last failure, once the attempt budget is spent.
            asyncio.TimeoutError: If the final attempt exceeded the deadline.
        """
