"""The queue's message type and its overflow signal.

`BaseMessage` is the only thing that crosses the queue boundary, so it carries
both the payload and the routing key. Partitioning is by `hash(url)`
(goal.md:113), which the poller supplies and the producer routes with
`partition_key % partition_count`.
"""

from dataclasses import dataclass

from webcrawler.domain.custom_url import CustomURL

class QueueOverflowError(RuntimeError):
    """Raised when an enqueue would exceed the deque max size (goal.md:117).

    A `RuntimeError` and not a bespoke base class: running out of capacity is a
    property of the live queue, not a fault in the caller's arguments, and the
    bulk API reports the identical condition as a `False` result instead of
    raising.

    Args:
    message: The reason the enqueue was rejected.
    """

@dataclass(frozen=True)
class BaseMessage:
    """One queued unit of work: a URL to crawl and the key that routes it.

    A frozen value type with no project base, so the poller, the producer, and
    the worker can all hold and compare the same object without any of them
    being able to alter it after the fact.

    Args:
    url: The URL to crawl. Its identity is the canonical form, so the
    message compares and hashes stably across producers.
    partition_key: The routing key, `hash(url)` (goal.md:113), stored
    verbatim. A Python `%` against a positive modulus is already
    non-negative, so routing needs no normalization here.
    """

    url: CustomURL
    partition_key: int
