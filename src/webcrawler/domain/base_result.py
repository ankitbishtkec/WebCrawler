"""The outcome of a single fetch, as handed to the politeness policy."""

from dataclasses import dataclass


@dataclass(frozen=True)
class BaseResult:
    """How one fetch ended, given back to the policy so it can learn.

    Args:
    is_success: True when the fetch succeeded, False when it failed.
    """

    is_success: bool
