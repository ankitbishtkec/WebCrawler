"""Tests for the simple in-memory topic queue.

One deque: no partitions, no reader registration, no reader id.
The prod-queue params (`topic`, `consumer_group_id`, message
`partition_key`) are accepted and ignored, so their presence — not their
value — is what these tests pin.
"""

import logging

import pytest

from webcrawler.domain.custom_url import CustomURL
from webcrawler.domain.messages import BaseMessage, QueueOverflowError
from webcrawler.infrastructure.queue.in_memory_topic_producer import (
    InMemoryTopicProducer)
from webcrawler.infrastructure.queue.in_memory_topic_reader import (
    InMemoryTopicReader)
from webcrawler.infrastructure.queue.in_memory_single_topic_single_partition_queue import (
    InMemorySingleTopicSinglePartitionQueue)

LOGGER = logging.getLogger("tests.webcrawler.queue")

def build(
    *, max_size: int = 10_000, topic: str = "crawl"
    ) -> tuple[InMemorySingleTopicSinglePartitionQueue, InMemoryTopicProducer, InMemoryTopicReader]:
    """Build one shared queue with a producer and reader over it.

    Args:
    max_size: The per-topic capacity to configure.
    topic: The topic name both sides share.

    Returns:
    tuple[InMemorySingleTopicSinglePartitionQueue, InMemoryTopicProducer, InMemoryTopicReader]:
    The queue, the producer, and the reader, sharing one
    queue instance exactly as the composition root wires them.
    """
    queue = InMemorySingleTopicSinglePartitionQueue(max_size=max_size)
    producer = InMemoryTopicProducer(topic, queue, LOGGER)
    reader = InMemoryTopicReader(topic, "crawler", queue, LOGGER)
    return queue, producer, reader

def message(url: str, partition_key: int = 0) -> BaseMessage:
    """Build one message for the given url and key.

    Args:
    url: The URL to carry.
    partition_key: The routing key, kept for a prod queue.

    Returns:
    BaseMessage: The message carrying the URL and the key.
    """
    return BaseMessage(CustomURL(url), partition_key)

@pytest.mark.parametrize("max_size", [1, 2, 3, 10])
async def test_the_topic_accepts_exactly_max_size_messages(max_size: int) -> None:
    """Enqueue up to the cap succeeds and one more raises.

    Args:
    max_size: The capacity to fill exactly and then exceed.
    """
    _, producer, _ = build(max_size=max_size)
    for index in range(max_size):
        await producer.enqueue(message(f"https://h.test/{index}"))
        with pytest.raises(QueueOverflowError):
            await producer.enqueue(message("https://h.test/one-more"))

            @pytest.mark.parametrize("max_size", [1, 2, 3])
            async def test_an_overflowed_bulk_message_reports_false_and_the_rest_are_enqueued(
                max_size: int) -> None:
                """The overflowed message reports False; later messages still land.

                Args:
                max_size: The capacity the first part of the batch fills.
                """
                _, producer, reader = build(max_size=max_size)
                batch = [message(f"https://h.test/{index}") for index in range(max_size + 2)]
                results = await producer.enqueue_many(batch)
                assert results == [True] * max_size + [False, False]
                assert len(await reader.peek(max_size + 2)) == max_size

            @pytest.mark.parametrize("constructed_first", ["producer", "reader"])
            async def test_either_adapter_may_be_built_first_because_nothing_is_wired(
                constructed_first: str) -> None:
                """The queue needs no creation step, so construction order cannot matter
                and no `connect` call exists (review.md items 14 and 16).

                Args:
                constructed_first: Which adapter is constructed first on a fresh queue.
                """
                fresh = InMemorySingleTopicSinglePartitionQueue()
                if constructed_first == "producer":
                    InMemoryTopicProducer("crawl", fresh, LOGGER)
                else:
                    InMemoryTopicReader("crawl", "crawler", fresh, LOGGER)
                    producer = InMemoryTopicProducer("crawl", fresh, LOGGER)
                    reader = InMemoryTopicReader("crawl", "crawler", fresh, LOGGER)

                    first = message("https://h.test/first")
                    await producer.enqueue(first)

                    assert await reader.peek(1) == [first]

                    @pytest.mark.parametrize("partition_key", [-9223372036854775808, -7, -1, 0, 1, 7])
                    async def test_any_partition_key_is_accepted_and_ignored(partition_key: int) -> None:
                        """One deque: every key lands in it, in order.

                        Args:
                        partition_key: The key carried on the message, which must not
                        change where or whether the message is enqueued.
                        """
                        _, producer, reader = build
                        first, second = (
                            message("https://h.test/first", partition_key),
                            message("https://h.test/second", partition_key))
                        assert await producer.enqueue_many([first, second]) == [True, True]
                        assert await reader.peek(2) == [first, second]

                    @pytest.mark.parametrize("url_text", ["https://h.test/a", "https://h.test/b"])
                    async def test_enqueue_many_does_not_dedupe(url_text: str) -> None:
                        """A repeated message is enqueued twice: dedupe is the caller's job.

                        Args:
                        url_text: The URL repeated in one bulk call.
                        """
                        _, producer, reader = build
                        repeated = message(url_text)
                        assert await producer.enqueue_many([repeated, repeated]) == [True, True]
                        assert await reader.peek(2) == [repeated, repeated]

                    @pytest.mark.parametrize(
                        ("enqueued", "n", "expected"),
                        [
                        (0, 1, 0),
                        (1, 1, 1),
                        (3, 2, 2),
                        (3, 10, 3),
                        ])
                    async def test_peek_returns_min_of_n_and_len_and_removes_nothing(
                        enqueued: int, n: int, expected: int
                        ) -> None:
                        """Peek reads the head without reserving or consuming it.

                        Args:
                        enqueued: How many messages to enqueue first.
                        n: The requested peek size.
                        expected: The peeked count, always `min(n, len)`.
                        """
                        _, producer, reader = build
                        batch = [message(f"https://h.test/{index}") for index in range(enqueued)]
                        await producer.enqueue_many(batch)
                        assert await reader.peek(n) == batch[:expected]
                        assert await reader.peek(n) == batch[:expected]

                    @pytest.mark.parametrize(
                        ("enqueued", "committed", "removed"),
                        [
                        (3, 3, 3),
                        (3, 10, 3),
                        (5, 2, 2),
                        (5, 0, 0),
                        ])
                    async def test_commit_removes_min_of_items_and_len_from_the_head(
                        enqueued: int, committed: int, removed: int
                        ) -> None:
                        """Commit drops exactly the first `min(len(items), len)` messages.

                        Args:
                        enqueued: How many messages to enqueue first.
                        committed: How many items the caller acknowledges.
                        removed: How many head messages must disappear.
                        """
                        _, producer, reader = build
                        batch = [message(f"https://h.test/{index}") for index in range(enqueued)]
                        await producer.enqueue_many(batch)
                        await reader.commit(batch[:committed])
                        assert await reader.peek(enqueued) == batch[removed:]

                    @pytest.mark.parametrize(
                        ("enqueued", "acknowledged", "first_removed", "second_removed"),
                        [
                        (4, 2, 2, 4),
                        (5, 1, 1, 2),
                        ])
                    async def test_commit_is_not_idempotent(
                        enqueued: int, acknowledged: int, first_removed: int, second_removed: int
                        ) -> None:
                        """Committing the same batch twice removes a second time.

                        Args:
                        enqueued: How many messages to enqueue first.
                        acknowledged: How many items each commit call acknowledges.
                        first_removed: The head count gone after the first commit.
                        second_removed: The head count gone after the second commit.
                        """
                        _, producer, reader = build
                        batch = [message(f"https://h.test/{index}") for index in range(enqueued)]
                        await producer.enqueue_many(batch)
                        await reader.commit(batch[:acknowledged])
                        assert await reader.peek(enqueued) == batch[first_removed:]
                        await reader.commit(batch[:acknowledged])
                        assert await reader.peek(enqueued) == batch[second_removed:]

                    @pytest.mark.parametrize("consumer_group_id", ["crawler", "other", ""])
                    async def test_a_consumer_group_is_accepted_and_is_a_no_op(
                        consumer_group_id: str) -> None:
                        """The group is recorded and never consulted.

                        Args:
                        consumer_group_id: The group id to construct the reader with.
                        """
                        queue = InMemorySingleTopicSinglePartitionQueue()
                        producer = InMemoryTopicProducer("crawl", queue, LOGGER)
                        reader = InMemoryTopicReader("crawl", consumer_group_id, queue, LOGGER)
                        await producer.enqueue(message("https://h.test/a"))
                        assert len(await reader.peek(1)) == 1

                    @pytest.mark.parametrize(("peeked", "appended"), [(1, 1), (2, 3), (3, 1)])
                    async def test_appending_during_processing_does_not_disturb_the_head(
                        peeked: int, appended: int
                        ) -> None:
                        """Messages peeked stay at the head while new ones land at the tail.

                        Args:
                        peeked: How many head messages are held in flight.
                        appended: How many tail messages land during processing.
                        """
                        _, producer, reader = build
                        batch = [message(f"https://h.test/{index}") for index in range(peeked)]
                        await producer.enqueue_many(batch)
                        in_flight = await reader.peek(peeked)
                        late = [message(f"https://h.test/late-{index}") for index in range(appended)]
                        await producer.enqueue_many(late)
                        assert await reader.peek(peeked) == in_flight
                        await reader.commit(in_flight)
                        assert await reader.peek(peeked + appended) == late

                    @pytest.mark.parametrize("acked", [1, 5])
                    async def test_a_fresh_queue_peeks_empty_and_an_oversized_commit_is_harmless(
                        acked: int) -> None:
                        """There is nothing to wire, so an empty queue is a valid state and a
                        commit larger than it removes nothing.

                        Args:
                        acked: How many items the caller acknowledges against an empty queue.
                        """
                        queue = InMemorySingleTopicSinglePartitionQueue()
                        reader = InMemoryTopicReader("crawl", "crawler", queue, LOGGER)
                        assert await reader.peek(5) == []
                        await reader.commit([message(f"https://h.test/{i}") for i in range(acked)])
                        assert await reader.peek(5) == []
