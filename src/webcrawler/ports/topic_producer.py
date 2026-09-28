"""The queue's write port.

The poller produces into a topic the worker consumes from; this is the producer half and
`ports/topic_reader.py` the consumer half. The shipped topic is one deque with no
connected-reader bookkeeping, so both sides must be given the same queue instance.
"""

from abc import ABC, abstractmethod

from webcrawler.domain.messages import BaseMessage


class TopicProducer(ABC):
    """The write side of an in-process topic.

    Constructed with `(topic, queue, logger)`, where the queue must be the same instance the reader
    reads from. There is no `consumer_group_id` because only readers join a group, no `connect`
    step because the constructor takes the queue; `topic`/`partition_key` are only future-proofing.
    """

    @abstractmethod
    async def enqueue(self, message: BaseMessage, request_id: str | None = None) -> bool:
        """Append one message to the tail of the queue.

        Args:
        message: The message to enqueue, carrying the URL and the `partition_key`, which is stored verbatim and not routed.
        request_id: Optional id making the call idempotent, the extension point for a networked broker where a retried send would duplicate; the shipped in-memory queue is not retried, so nothing is deduplicated today.

        Returns:
            bool: True when the message was enqueued.

        Raises:
            QueueOverflowError: If the queue already holds `max_size` messages; the deque is left unchanged.
        """

    @abstractmethod
    async def enqueue_many(
        self, messages: list[BaseMessage], request_id: str | None = None
    ) -> list[bool]:
        """Enqueue a batch, reporting one outcome per input message.

        No dedupe, because only the caller knows if a repeated URL is a retry or new work. An overflowed message is reported as `False` and the rest of the batch still enqueued, so one full queue cannot discard a poll's whole claim; its row stays `queued` and the first poll after `queue_timeout` re-claims it.

        Args:
            messages: The batch to enqueue, in the order given.
            request_id: Optional id making the whole call idempotent, unused today for the same reason as in `enqueue`.

        Returns:
            list[bool]: One result per input message in the same order; True where it was enqueued, False where it overflowed.

        Raises:
            Exception: A networked producer's send can fail, so an implementation may raise; the shipped in-memory producer never does and reports overflow as `False`.
        """

    @abstractmethod
    async def enqueue_to_deadletter(
        self, message: BaseMessage, request_id: str | None = None
    ) -> bool:
        """Park a message that failed and must not be retried.

        Args:
        message: The message to park, carrying the URL it was for.
        request_id: Optional id making the call idempotent, unused today as in `enqueue`.

        Returns:
            bool: True when the message was parked, False when the deadletter queue is full and the message is dropped.

        Raises:
            Exception: A networked producer's send can fail, so an implementation may raise; the shipped in-memory producer never does and reports a full deadletter queue as `False`.
        """
