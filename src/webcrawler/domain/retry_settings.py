"""The retry knobs shared by every implementation that performs I/O.

Each I/O implementation owns its retry with exponential backoff, jitter, and a
timeout. The values live here as one frozen value type so the policy, the store,
and the fetcher's socket timeout all read the same numbers.
"""

from dataclasses import dataclass

@dataclass(frozen=True)
class RetrySettings:
    """Immutable backoff, jitter, and timeout knobs for one retry policy.

    Args:
    max_attempts: Total attempts per operation, the first try included.
    Once they are used up, the last error is re-raised.
    base_delay_seconds: The delay before attempt `n`, doubled per attempt
    and capped by `max_delay_seconds`.
    max_delay_seconds: The ceiling on that exponential term, so growth
    stays bounded however many attempts are configured.
    jitter_seconds: Width of the uniform random spread added on top of each
    delay, which de-synchronises concurrent workers retrying in step.
    timeout_seconds: The per-attempt deadline. An attempt overrunning it is
    abandoned and retried, and the blocking HTTP client reuses it as its
    socket timeout.
    """

    max_attempts: int
    base_delay_seconds: float
    max_delay_seconds: float
    jitter_seconds: float
    timeout_seconds: float
