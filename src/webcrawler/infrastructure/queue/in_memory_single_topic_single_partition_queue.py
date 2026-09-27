"""The one bounded deque both queue views are composed over.

`InMemorySingleTopicSinglePartitionQueue` is the single object injected into
BOTH `InMemoryTopicProducer` and `InMemoryTopicReader`: if the two hold
different instances the poller fills a queue the worker never reads, so the
composition root constructs exactly one and hands it to both (plan.md:817).

One deque and nothing else: no topics, no partitions, no connected-reader
list, no reader id. The prod-queue params (topic, consumer group id, message
`partition_key`) are accepted by the adapters and ignored here, so a real prod
queue can replace them without touching a caller (goal.md:118).

No lock, deliberately: the work is CPU-bound on the single event loop and the
shipped run path has exactly one reader, so there is no concurrent mutation
to serialize (goal.md:117).
"""

from collections import deque
from itertools import islice
from typing import Final

from webcrawler.domain.messages import BaseMessage, QueueOverflowError

# The capacity of goal.md:116.
DEFAULT_MAX_SIZE: Final = 10_000


class InMemorySingleTopicSinglePartitionQueue:
    """The single bounded FIFO the crawl is served from.

    The two adapters are composed over one instance of this and never
    subclass it, so neither view can hold a message the other cannot see
    (plan.md:798, goal.md:8).

    Args:
        max_size: The capacity, checked on append rather than via
            `deque(maxlen=...)`, because a `maxlen` deque discards from the
            opposite end and would drop uncommitted head messages instead of
            rejecting the new one (goal.md:116).
    """

    def __init__(self, *, max_size: int = DEFAULT_MAX_SIZE) -> None:
        """Record the capacity and start empty.

        Args:
            max_size: The capacity, in messages.
        """
        self._max_size = max_size
        self._messages: deque[BaseMessage] = deque()

    def enqueue(self, message: BaseMessage) -> None:
        """Append one message to the tail.

        The message is stored verbatim; its `partition_key` is accepted for a
        future prod queue and ignored here, there is no routing to do.

        Args:
            message: The message to append, carrying the `partition_key` the
                poller sets to `hash(url)`.

        Raises:
            QueueOverflowError: If the deque already holds `max_size`
                messages, leaving it unchanged (goal.md:116).
        """
        if len(self._messages) >= self._max_size:
            raise QueueOverflowError(
                f"the queue already holds {len(self._messages)} "
                f"of {self._max_size} messages"
            )
        self._messages.append(message)

    def peek(self, n: int) -> list[BaseMessage]:
        """Read the head without removing anything.

        Non-reserving, because the shipped run path has exactly one reader,
        so there is nothing to hold a claim against (plan.md:92).

        Args:
            n: The largest number of items wanted.

        Returns:
            list[BaseMessage]: Up to `n` items, leaving the deque unchanged.
                Empty when `n` is not positive.
        """
        if n <= 0:
            return []
        return list(islice(self._messages, n))

    def commit(self, count: int) -> None:
        """Remove up to `count` messages from the head.

        Head-based and non-idempotent: a second call with the same count
        removes a second time. That is safe only because the shipped run
        path has exactly one reader (plan.md:299).

        Args:
            count: How many messages to remove, bounded by the messages that
                are actually there, so an over-sized batch cannot underflow.
        """
        for _ in range(min(count, len(self._messages))):
            self._messages.popleft()
