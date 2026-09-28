"""The queue's read port.

The consumer half of the queue boundary; `ports/topic_producer.py` is the producer half.
Deliberately a queue, not partitions: one deque and one reader in the shipped implementation, so
no partition assignment to report, no `connect` step, and a memory-ignored `consumer_group_id`.
"""


from abc import ABC, abstractmethod

from webcrawler.domain.messages import BaseMessage


class TopicReader(ABC):
    """The read side of an in-process topic.

    Constructed with `(topic, consumer_group_id, queue, logger)`, of which only
    the first two are behaviourally meaningful. The queue must be the same
    instance the producer writes to.
    """

    @abstractmethod
    async def peek(self, n: int) -> list[BaseMessage]:
        """Read the head of the queue without removing anything.

        Non-reserving, because the shipped run path has exactly one reader, so there is nothing to hold a claim against.

        Args:
            n: The largest number of items wanted.

        Returns:
            list[BaseMessage]: `min(n, available)` items, leaving the deque unchanged.

        Raises:
            Exception: A networked reader's read can fail, so an implementation may raise; the shipped in-memory reader never does.
        """

    @abstractmethod
    async def commit(self, messages: list[BaseMessage], request_id: str | None = None) -> None:
        """Remove `min(len(messages), available)` items from the head.

        Head-based, so committing a batch twice removes twice as many. Nothing is deduplicated today because the shipped in-memory queue is not retried, so `request_id` is only the extension point for a networked broker.

        Args:
            messages: The batch being acknowledged; only its count is used, so a batch larger than the deque cannot underflow it.
            request_id: Optional id making the call idempotent, unused today for the reason given above.

        Raises:
            Exception: A networked commit is a remote call, so an implementation may raise; the shipped in-memory reader never does.
        """
