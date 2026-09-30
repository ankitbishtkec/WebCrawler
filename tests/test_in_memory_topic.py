"""Unit tests for the in-memory topic: one bounded deque behind two views.

The queue classes hold no collaborator, so the object under test is the unit and
there is nothing to mock. The producer and the reader are built over one shared
queue, as the composition root builds them, and every assertion is on a return
value: what `peek` hands back and what `enqueue_many` reports.
"""

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
    """Build the producer and the reader over one shared empty queue.

    Returns:
        tuple[InMemoryTopicProducer, InMemoryTopicReader]: The two views of the
        same queue, so what the producer writes is what the reader reads. The
        capacities are the shipped ones, which no test here fills.
    """
    queue = InMemorySingleTopicSinglePartitionQueue()
    return (
        InMemoryTopicProducer(TOPIC, queue),
        InMemoryTopicReader(TOPIC, GROUP, queue),
    )


def messages(count: int) -> list[BaseMessage]:
    """Build one message per page, in order.

    Args:
        count: How many pages to name.

    Returns:
        list[BaseMessage]: The messages of pages 1 through `count`, in that order.
    """
    made: list[BaseMessage] = []
    for index in range(1, count + 1):
        url = CustomURL(f"{HOST}/page-{index}.html")
        # The routing key is the second field, the hash the poller sets.
        made.append(BaseMessage(url, hash(url)))
    return made


async def test_enqueue_then_peek_returns_the_messages_in_order() -> None:
    """A peek reports what was enqueued, head first, and removes nothing.

    Returns:
        None
    """
    producer, reader = views()
    sent = messages(3)
    for message in sent:
        await producer.enqueue(message)

    assert await reader.peek(10) == sent


@pytest.mark.parametrize("count", [1, 3])
async def test_enqueue_many_reports_one_true_per_message(count: int) -> None:
    """A batch reports one True per message, and the reader then sees all of them.

    Args:
        count: How many messages the batch holds.

    Returns:
        None
    """
    producer, reader = views()
    sent = messages(count)

    reported = await producer.enqueue_many(sent)

    assert reported == [True] * count
    assert await reader.peek(10) == sent


@pytest.mark.parametrize("count", [1, 3])
async def test_commit_removes_the_batch_and_leaves_the_rest(count: int) -> None:
    """A commit takes exactly the batch the reader peeked, and no more.

    Args:
        count: How many messages the peeked batch holds.

    Returns:
        None
    """
    producer, reader = views()
    sent = messages(count + 1)
    for message in sent:
        await producer.enqueue(message)

    peeked = await reader.peek(count)
    await reader.commit(peeked)

    assert await reader.peek(10) == sent[count:]


async def test_enqueue_to_deadletter_parks_the_message_out_of_sight() -> None:
    """A parked message reports True, and no peek of the crawl topic can see it.

    Returns:
        None
    """
    producer, reader = views()
    message = messages(1)[0]

    parked = await producer.enqueue_to_deadletter(message)

    assert parked is True
    assert await reader.peek(10) == []
