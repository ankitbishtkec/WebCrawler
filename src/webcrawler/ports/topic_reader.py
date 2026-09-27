"""The queue's read port (goal.md:115-128).

The consumer half of the queue boundary; `ports/topic_producer.py` is the
producer half. The port is deliberately a queue, not a set of partitions: the
shipped implementation has one deque and a single reader, so there is no
partition assignment to report and no `connect` step — the constructor takes
the queue and `peek`/`commit` work straight against it.

`consumer_group_id` stays in the constructor because `goal.md:121` requires the
interface to carry it, and it is a no-op for the in-memory implementation. The
README's Extensions section describes what a real prod queue (Kafka and the
like) would use these same two ports for, and how a consumer group, partition
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
    """Read the head of the topic without removing anything.

    Non-reserving, because the shipped run path has exactly one reader per
    topic, so there is nothing to hold a claim against.

    Args:
    n: The largest number of items wanted.

    Returns:
    list[BaseMessage]: At most `n` items, leaving the deque unchanged.
    Fewer than `n` means the topic is drained.

    Raises:
    Exception: Whatever reading the topic raises, propagated
    unchanged.
    """

@abstractmethod
async def commit(self, items: list[BaseMessage]) -> None:
    """Remove the given count of items from the head of the topic.

    Head-based rather than identity-based, and non-idempotent: committing
    the same batch twice removes twice as many. That is safe only because
    the shipped run path has exactly one reader per topic.

    Args:
    items: The items being acknowledged. Only their count is used, and
    `min(len(items), len(topic))` items are removed, so a batch
    larger than the deque cannot underflow it.

    Raises:
    Exception: Whatever removing the items raises, propagated
    unchanged.
    """
