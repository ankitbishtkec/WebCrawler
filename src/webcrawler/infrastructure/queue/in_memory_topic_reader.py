"""The read side of the in-memory topic: peek and commit.

A thin view over the shared queue. It owns nothing, so what it peeks and
commits is exactly what the producer over the same queue enqueued.
"""

import logging

from webcrawler.domain.messages import BaseMessage
from webcrawler.infrastructure.queue.in_memory_single_topic_single_partition_queue import (
    InMemorySingleTopicSinglePartitionQueue)
from webcrawler.ports.topic_reader import TopicReader

class InMemoryTopicReader(TopicReader):
    """Read the shared queue's single deque.

    The topic and the `consumer_group_id` are recorded and never consulted, and
    no reader id is kept: one deque, no connected-reader list. Extends the
    `TopicReader` ABC and nothing extends it.

    Args:
    topic: The topic name from the port, recorded and never consulted.
    consumer_group_id: The group this reader belongs to. A no-op for
    this implementation, kept because the interface carries it.
    queue: The shared queue. It must be the same instance the
    producer writes to, or the reader reads an empty queue.
    logger: The injected logger, which records the join at INFO.
    """

    def __init__(
        self,
        topic: str,
        consumer_group_id: str,
        queue: InMemorySingleTopicSinglePartitionQueue,
        logger: logging.Logger) -> None:
        """Hold the topic, group, queue, and logger.

        Args:
        topic: The topic to read, recorded and never consulted.
        consumer_group_id: The group this reader belongs to; recorded and
        never used.
        queue: The shared queue, which must be the producer's queue.
        logger: The injected logger.
        """
        self._topic = topic
        self._consumer_group_id = consumer_group_id
        self._queue = queue
        self._logger = logger
        self._logger.debug(
            "reading topic %s as group %s", topic, consumer_group_id
            )

    async def peek(self, n: int) -> list[BaseMessage]:
        """Read the head of the queue without removing anything.

        Args:
        n: The largest number of items wanted.

        Returns:
        list[BaseMessage]: `min(n, available)` items, leaving the deque
        unchanged.
        """
        return self._queue.peek(n)

    async def commit(
        self, messages: list[BaseMessage], request_id: str | None = None
    ) -> None:
        """Remove `min(len(messages), available)` items from the head.

        Head-based and non-idempotent, safe only with the shipped one reader.

        Args:
        messages: The batch being acknowledged. Only its count is used, so
        a batch larger than the deque cannot underflow it.
        request_id: Accepted and ignored: this in-memory queue is never
        retried, so no commit is repeated and nothing is deduplicated.
        """
        self._queue.commit(len(messages))
