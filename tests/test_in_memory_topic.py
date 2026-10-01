"""Unit tests for the in-memory topic: one bounded deque behind two views."""

import pytest

from webcrawler.domain.custom_url import CustomURL
from webcrawler.domain.messages import BaseMessage
from webcrawler.infrastructure.queue.in_memory_single_topic_single_partition_queue import (
    InMemorySingleTopicSinglePartitionQueue)
from webcrawler.infrastructure.queue.in_memory_topic_producer import (
    InMemoryTopicProducer)
from webcrawler.infrastructure.queue.in_memory_topic_reader import (
    InMemoryTopicReader)

# The topic name both views are given; recorded and never consulted.
TOPIC: str = "crawl"

# The group the reader is given; recorded and never consulted.
GROUP: str = "crawler"

# The host every URL in this file is built on; nothing here is ever fetched.
HOST: str = "https://site.test"


def views() -> tuple[InMemoryTopicProducer, InMemoryTopicReader]:
    """Build the producer and the reader over one shared empty queue."""
    queue = InMemorySingleTopicSinglePartitionQueue()
    return (
        InMemoryTopicProducer(TOPIC, queue),
        InMemoryTopicReader(TOPIC, GROUP, queue),
    )


def messages(count: int) -> list[BaseMessage]:
    """Build one message per page, in order."""
    made: list[BaseMessage] = []
    for index in range(1, count + 1):
        url = CustomURL(f"{HOST}/page-{index}.html")
        # The routing key is the second field, the hash the poller sets.
        made.append(BaseMessage(url, hash(url)))
    return made


async def test_enqueue_then_peek_returns_the_messages_in_order() -> None:
    """A peek reports what was enqueued, head first, and removes nothing."""
    producer, reader = views()
    sent = messages(3)
    for message in sent:
        await producer.enqueue(message)

    assert await reader.peek(10) == sent


@pytest.mark.parametrize("count", [1, 3])
async def test_enqueue_many_reports_one_true_per_message(count: int) -> None:
    """A batch reports one True per message, and the reader then sees all of them."""
    producer, reader = views()
    sent = messages(count)

    reported = await producer.enqueue_many(sent)

    assert reported == [True] * count
    assert await reader.peek(10) == sent


@pytest.mark.parametrize("count", [1, 3])
async def test_commit_removes_the_batch_and_leaves_the_rest(count: int) -> None:
    """A commit takes exactly the batch the reader peeked, and no more."""
    producer, reader = views()
    sent = messages(count + 1)
    for message in sent:
        await producer.enqueue(message)

    peeked = await reader.peek(count)
    await reader.commit(peeked)

    assert await reader.peek(10) == sent[count:]


async def test_enqueue_to_deadletter_parks_the_message_out_of_sight() -> None:
    """A parked message reports True, and no peek of the crawl topic can see it."""
    producer, reader = views()
    message = messages(1)[0]

    parked = await producer.enqueue_to_deadletter(message)

    assert parked is True
    assert await reader.peek(10) == []
