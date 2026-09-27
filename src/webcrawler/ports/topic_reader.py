"""The queue's read port (goal.md:115-128, plan.md:277-305).

The consumer half of the queue boundary; `ports/topic_producer.py` is the
producer half. The port is deliberately a queue, not a set of partitions: the
shipped implementation has one deque and a single reader, so there is
no partition assignment to report and no `connect` step, the constructor takes
the queue and `peek`/`commit` work straight against it.

`consumer_group_id` stays in the constructor because `goal.md:121` requires the
interface to carry it, and it is a no-op for the in-memory implementation. The
README's Extensions section describes what a real prod queue, Kafka and the
like, would use these same two ports for, and how a consumer group, partition
assignment and rebalancing would appear there.
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

        Non-reserving, because the shipped run path has exactly one reader, so
        there is nothing to hold a claim against (plan.md:92).

        Args:
            n: The largest number of items wanted.

        Returns:
            list[BaseMessage]: `min(n, available)` items, leaving the deque
                unchanged.

        Raises:
            Exception: A networked reader's read can fail, so an implementation
                may raise. The shipped in-memory reader never does.
        """

    @abstractmethod
    async def commit(self, messages: list[BaseMessage], request_id: str | None = None) -> None:
        """Remove `min(len(messages), available)` items from the head.

        Head-based: committing the same batch twice removes twice as many, so
        a retried commit takes more than it should. The shipped queue is
        in-memory and its operations are not retried, so nothing is
        deduplicated today. `request_id` is the extension point for a networked
        broker, where that retried removal would matter.

        Args:
            messages: The batch being acknowledged. Only its count is used, so
                a batch larger than the deque cannot underflow it.
            request_id: Optional id making the call idempotent, unused today
                for the reason given above.

        Raises:
            Exception: A networked commit is a remote call, so an
                implementation may raise. The shipped in-memory reader never
                does.
        """
