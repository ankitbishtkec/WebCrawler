"""The queue's write port (goal.md:115-128).

The poller produces into a topic and the worker consumes from it, and
`goal.md:119` asks for an interface on each side of that boundary. This module
is the producer half; `ports/topic_reader.py` is the consumer half.

The topic is a queue, not a set of partitions, in the default implementation:
one deque, no connected-reader bookkeeping, and the prod-queue parameters
(`topic`, the message `partition_key`) kept for future-proofing
(goal.md:118). The port therefore states the connection requirement, the
producer and the reader must be given the same queue instance, without
naming the storage, because how a topic is stored is neither side's business.

Every method is a coroutine, even the bulk enqueue, so a caller never has to
know whether an implementation is in-memory or networked.
"""

from abc import ABC, abstractmethod

from webcrawler.domain.messages import BaseMessage


class TopicProducer(ABC):
    """The write side of an in-process topic.

    Constructed with `(topic, queue, logger)`, where the queue must be the same
    instance the reader reads from, so both sides see the same topic. There is
    no `consumer_group_id` because only readers join a group (goal.md:124).
    The constructor takes the queue, so there is no `connect` step.
    """

    @abstractmethod
    async def enqueue(self, message: BaseMessage, request_id: str | None = None) -> bool:
        """Append one message to the tail of the queue.

        Args:
        message: The message to enqueue, carrying the URL and the
            `partition_key`, which is stored verbatim and not routed.
        request_id: Optional id making the call idempotent. The shipped queue
            is in-memory and its operations are not retried, so no id is ever
            sent twice and nothing is deduplicated today. It is here as the
            extension point for a networked broker, where a retried send would
            otherwise duplicate the message.

        Returns:
            bool: True when the message was enqueued.

        Raises:
            QueueOverflowError: If the queue already holds `max_size`
                messages; the deque is left unchanged.
        """

    @abstractmethod
    async def enqueue_many(
        self, messages: list[BaseMessage], request_id: str | None = None
    ) -> list[bool]:
        """Enqueue a batch, reporting one outcome per input message.

        No dedupe: the caller owns that, because only the caller knows
        whether a repeated URL is a retry or new work (goal.md:125-126).
        An overflowed message is reported as `False` and the rest of the
        batch is still enqueued, so one full queue cannot discard a poll's
        whole claim; the row stays `queued` in the database and the first
        poll after `queue_timeout` re-claims it (goal.md:140).

        Args:
            messages: The batch to enqueue, in the order given.
            request_id: Optional id making the whole call idempotent. Not used
                today, for the same reason as in `enqueue`; on a networked
                broker a retried send would otherwise duplicate the batch.

        Returns:
            list[bool]: One result per input message, in the same order;
                True where the message was enqueued, False where it
                overflowed.

        Raises:
            Exception: A networked producer's send can fail, so an
                implementation may raise. The shipped in-memory producer never
                does; it reports overflow as `False` instead.
        """

    @abstractmethod
    async def enqueue_to_deadletter(
        self, message: BaseMessage, request_id: str | None = None
    ) -> bool:
        """Park a message that failed and must not be retried.

        Args:
        message: The message to park, carrying the URL it was for.
        request_id: Optional id making the call idempotent, unused today as
            in `enqueue`.

        Returns:
            bool: True when the message was parked, False when the deadletter
                queue is full and the message is dropped.

        Raises:
            Exception: A networked producer's send can fail, so an
                implementation may raise. The shipped in-memory producer never
                does; it reports a full deadletter queue as `False` instead.
        """
