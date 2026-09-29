"""The write side of the in-memory queue: capacity, bulk enqueue, and parking.

A thin view over the shared queue. It holds the port's topic name and a logger and
owns no storage of its own, so the messages it enqueues are the same objects
the reader over the same queue peeks.
"""

import logging

from webcrawler.domain.messages import BaseMessage, QueueOverflowError
from webcrawler.infrastructure.queue.in_memory_single_topic_single_partition_queue import (
    InMemorySingleTopicSinglePartitionQueue)
from webcrawler.ports.topic_producer import TopicProducer

class InMemoryTopicProducer(TopicProducer):
    """Append messages into the shared queue.

    The topic and each message's `partition_key` are accepted because the port
    carries them and ignored because the queue holds one deque. Extends the
    `TopicProducer` ABC and nothing extends it.

    Args:
    topic: The topic name from the port, recorded and never consulted.
    queue: The shared queue. It must be the same instance the
    reader reads from, or the poller fills a queue nobody reads.
    """

    def __init__(
        self, topic: str, queue: InMemorySingleTopicSinglePartitionQueue
    ) -> None:
        """Hold the topic name and the shared queue.

        Args:
        topic: The topic to produce into, recorded and never consulted.
        queue: The shared queue, which must be the reader's queue.
        """
        self._logger = logging.getLogger(__name__)
        self._topic = topic
        self._queue = queue
        self._logger.debug("using topic %s", topic)

    async def enqueue(
        self, message: BaseMessage, request_id: str | None = None
    ) -> bool:
        """Append one message to the tail of the queue.

        The poller uses `enqueue_many`; this API is for tests and direct callers.

        Args:
        message: The message to enqueue, carrying the URL and the
        `partition_key`, which is stored verbatim and not routed.
        request_id: Accepted and ignored: this in-memory queue is never
        retried, so no send is repeated and nothing is deduplicated.

        Returns:
        bool: True, because an overflow is raised rather than reported here.

        Raises:
        QueueOverflowError: If the queue already holds `max_size`
        messages; the deque is left unchanged.
        """
        self._queue.enqueue(message)
        return True

    async def enqueue_many(
        self, messages: list[BaseMessage], request_id: str | None = None
    ) -> list[bool]:
        """Enqueue a batch, reporting one outcome per input message.

        No dedupe: only the caller knows if a repeat is a retry or new work.

        Args:
        messages: The batch to enqueue, in the order given.
        request_id: Accepted and ignored, for the reason given in `enqueue`.

        Returns:
        list[bool]: One result per input message, in the same order;
        True where the message was enqueued, False where it
        overflowed.
        """
        # An overflow is `False` and the rest still enqueued, so one full topic
        # cannot discard a poll's claim: its rows stay `queued` for the next poll.
        results: list[bool] = []
        for message in messages:
            try:
                self._queue.enqueue(message)
            except QueueOverflowError:
                results.append(False)
                continue
            results.append(True)
        overflowed = results.count(False)
        if overflowed:
            self._logger.debug(
                "%d of %d message(s) overflowed topic %s",
                overflowed,
                len(results),
                self._topic)
        return results

    async def enqueue_to_deadletter(
        self, message: BaseMessage, request_id: str | None = None
    ) -> bool:
        """Park one message that failed and must not be retried.

        Args:
        message: The message to park, carrying the URL it failed on.
        request_id: Accepted and ignored, for the reason given in `enqueue`.

        Returns:
        bool: True when the message was parked, False when the deadletter
        queue is full and the message is dropped.
        """
        try:
            self._queue.enqueue_deadletter(message)
        except QueueOverflowError:
            self._logger.debug(
                "the deadletter queue of topic %s is full, dropping the message",
                self._topic)
            return False
        return True


