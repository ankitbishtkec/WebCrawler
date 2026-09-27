"""The one bounded deque both queue views are composed over.

`InMemorySingleTopicSinglePartitionQueue` is the single object injected into
BOTH `InMemoryTopicProducer` and `InMemoryTopicReader`: if the two hold
different instances the poller fills a queue the worker never reads, so the
composition root constructs exactly one and hands it to both (plan.md:817).

Two deques and nothing else: the crawl topic and its deadletter topic, and no
topics, no partitions, no connected-reader list, no reader id. The prod-queue
params (topic, consumer group id, message `partition_key`) are accepted by the
adapters and ignored here, so a real prod queue can replace them without
touching a caller (goal.md:118).

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

# The deadletter capacity, bounded the same way but independently, so a full
# crawl topic still has somewhere to park a message that must not be retried.
DEFAULT_MAX_DEADLETTER_SIZE: Final = 10_000


class InMemorySingleTopicSinglePartitionQueue:
    """The bounded FIFO the crawl is served from, and its deadletter FIFO.

    The two adapters are composed over one instance of this and never
    subclass it, so neither view can hold a message the other cannot see
    (plan.md:798, goal.md:8).

    Args:
        max_size: The crawl queue's capacity, checked on append rather than via
            `deque(maxlen=...)`, because a `maxlen` deque discards from the
            opposite end and would drop uncommitted head messages instead of
            rejecting the new one (goal.md:116).
        max_deadletter_size: The deadletter queue's capacity, checked the same
            way for the same reason.
    """

    def __init__(
        self,
        *,
        max_size: int = DEFAULT_MAX_SIZE,
        max_deadletter_size: int = DEFAULT_MAX_DEADLETTER_SIZE,
    ) -> None:
        """Record both capacities and start both deques empty.

        Args:
            max_size: The crawl queue's capacity, in messages.
            max_deadletter_size: The deadletter queue's capacity, in messages.
        """
        self._max_size = max_size
        self._messages: deque[BaseMessage] = deque()
        self._max_deadletter_size = max_deadletter_size
        self._deadletters: deque[BaseMessage] = deque()

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

    def enqueue_deadletter(self, message: BaseMessage) -> None:
        """Append one message to the tail of the deadletter deque.

        The message is stored verbatim, exactly as the crawl deque stores one,
        so a parked message is still the same object a reader could have peeked.

        Args:
            message: The message to park, carrying the URL it failed on.

        Raises:
            QueueOverflowError: If the deadletter deque already holds
                `max_deadletter_size` messages, leaving it unchanged.
        """
        if len(self._deadletters) >= self._max_deadletter_size:
            raise QueueOverflowError(
                f"the deadletter queue already holds {len(self._deadletters)} "
                f"of {self._max_deadletter_size} messages"
            )
        self._deadletters.append(message)

    def peek_deadletter(self, n: int) -> list[BaseMessage]:
        """Read the deadletter head without removing anything.

        Args:
            n: The largest number of items wanted.

        Returns:
            list[BaseMessage]: Up to `n` parked messages, leaving the deque
                unchanged. Empty when `n` is not positive.
        """
        if n <= 0:
            return []
        return list(islice(self._deadletters, n))

    @property
    def deadletter_count(self) -> int:
        """int: How many messages are parked, without removing any."""
        return len(self._deadletters)
