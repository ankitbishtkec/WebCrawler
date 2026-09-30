"""The one bounded deque both queue views are composed over.

The same instance is injected into BOTH `InMemoryTopicProducer` and
`InMemoryTopicReader`; two instances would let the poller fill a queue the
worker never reads. Two deques and nothing else.
"""

from collections import deque
from itertools import islice
from typing import Final

from webcrawler.domain.messages import BaseMessage, QueueOverflowError

# The crawl topic's capacity, in messages.
DEFAULT_MAX_SIZE: Final = 1000000

# The deadletter capacity, bounded the same way but independently, so a full
# crawl topic still has somewhere to park a message that must not be retried.
DEFAULT_MAX_DEADLETTER_SIZE: Final = 1000000


class InMemorySingleTopicSinglePartitionQueue:
    """The bounded FIFO the crawl is served from, and its deadletter FIFO.

    Both adapters compose over one instance and never subclass it, so neither
    view can hide a message from the other. No lock: the work is CPU-bound on
    the single event loop with one reader, so nothing is left to serialise.
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
        # Two deques and nothing else: no topics, no partitions, no connected
        # reader list, no reader id. The prod-queue params live in the adapters,
        # accepted there and ignored, so a real queue can replace them.
        self._messages: deque[BaseMessage] = deque()
        self._max_deadletter_size = max_deadletter_size
        self._deadletters: deque[BaseMessage] = deque()

    def enqueue(self, message: BaseMessage) -> None:
        """Append one message to the tail.

        Stored verbatim; `partition_key` is accepted for a prod queue and ignored.

        Args:
            message: The message to append, carrying the `partition_key` the
                poller sets to `hash(url)`.

        Raises:
            QueueOverflowError: If the deque already holds `max_size`
                messages, leaving it unchanged.
        """
        # Checked on append, not via `deque(maxlen=...)`: a maxlen deque
        # discards from the opposite end, dropping uncommitted head messages
        # instead of rejecting the new one.
        if len(self._messages) >= self._max_size:
            raise QueueOverflowError(
                f"the queue already holds {len(self._messages)} "
                f"of {self._max_size} messages"
            )
        self._messages.append(message)

    def peek(self, n: int) -> list[BaseMessage]:
        """Read the head without removing anything.

        Non-reserving: one reader in the shipped run path, so nothing to hold.

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

        Head-based and non-idempotent, safe only with the shipped one reader.

        Args:
            count: How many messages to remove, bounded by the messages that
                are actually there, so an over-sized batch cannot underflow.
        """
        for _ in range(min(count, len(self._messages))):
            self._messages.popleft()

    def enqueue_deadletter(self, message: BaseMessage) -> None:
        """Append one message to the tail of the deadletter deque.

        Stored verbatim, exactly as the crawl deque stores one.

        Args:
            message: The message to park, carrying the URL it failed on.

        Raises:
            QueueOverflowError: If the deadletter deque already holds
                `max_deadletter_size` messages, leaving it unchanged.
        """
        # Checked the same way, for the same reason as the crawl deque.
        if len(self._deadletters) >= self._max_deadletter_size:
            raise QueueOverflowError(
                f"the deadletter queue already holds {len(self._deadletters)} "
                f"of {self._max_deadletter_size} messages"
            )
        self._deadletters.append(message)
