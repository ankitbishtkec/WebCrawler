"""The write side of the in-memory queue: capacity and bulk enqueue.

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

    The topic name is accepted because the port carries it and ignored
    because the queue holds one deque; the `partition_key` each message
    carries is ignored the same way (goal.md:118).

    It extends the `TopicProducer` ABC and is extended by nothing, which is
    what keeps the write side substitutable (goal.md:8).

    Args:
    topic: The topic name from the port, recorded and never consulted.
    queue: The shared queue. It must be the same instance the
    reader reads from, or the poller fills a queue nobody reads.
    logger: The injected logger. Joining the topic is INFO, and an
    overflowed batch is DEBUG.
    """

    def __init__(
        self, topic: str, queue: InMemorySingleTopicSinglePartitionQueue, logger: logging.Logger
        ) -> None:
        """Hold the topic name, the shared queue, and the logger.

        Args:
        topic: The topic to produce into, recorded and never consulted.
        queue: The shared queue, which must be the reader's queue.
        logger: The injected logger.
        """
        self._topic = topic
        self._queue = queue
        self._logger = logger
        self._logger.info("using topic %s", topic)

    async def enqueue(self, message: BaseMessage) -> None:
        """Append one message to the tail of the queue.

        The poller uses `enqueue_many`, so this single-message API is for tests
        and callers outside the crawl loop.

        Args:
        message: The message to enqueue, carrying the URL and the
        `partition_key`, which is stored verbatim and not routed.

        Raises:
        QueueOverflowError: If the queue already holds `max_size`
        messages; the deque is left unchanged.
        """
        self._queue.enqueue(message)

    async def enqueue_many(self, messages: list[BaseMessage]) -> list[bool]:
        """Enqueue a batch, reporting one outcome per input message.

        No dedupe: the caller owns that, because only the caller knows
        whether a repeated URL is a retry or new work.
        An overflowed message is reported as `False` and the rest of the
        batch is still enqueued, so one full topic cannot discard a poll's
        whole claim; the row stays `queued` in the database and the first
        poll after `queue_timeout` re-claims it.

        Args:
        messages: The batch to enqueue, in the order given.

        Returns:
        list[bool]: One result per input message, in the same order;
        True where the message was enqueued, False where it
        overflowed.
        """
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
