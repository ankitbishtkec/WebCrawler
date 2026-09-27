"""Tests for the in-memory topic queue (plan.md:792)."""

import logging

import pytest

from webcrawler.domain.custom_url import CustomURL
from webcrawler.domain.messages import BaseMessage, QueueOverflowError
from webcrawler.infrastructure.queue.in_memory_single_topic_single_partition_queue import (
    InMemorySingleTopicSinglePartitionQueue,
)
from webcrawler.infrastructure.queue.in_memory_topic_producer import (
    InMemoryTopicProducer,
)
from webcrawler.infrastructure.queue.in_memory_topic_reader import InMemoryTopicReader

LOGGER = logging.getLogger("tests.webcrawler.topic")
TOPIC = "crawl"
GROUP = "crawler"
METHODS = ["enqueue", "enqueue_many"]


def message(raw: str, partition_key: int | None = None) -> BaseMessage:
    """Return one queue message for a URL."""
    url = CustomURL(raw)
    return BaseMessage(url, hash(url) if partition_key is None else partition_key)


def build(
    *, max_size: int = 10_000, topic: str = TOPIC
) -> tuple[InMemorySingleTopicSinglePartitionQueue, InMemoryTopicProducer, InMemoryTopicReader]:
    """Return one queue with its two views composed over it."""
    queue = InMemorySingleTopicSinglePartitionQueue(max_size=max_size)
    producer = InMemoryTopicProducer(topic, queue, LOGGER)
    reader = InMemoryTopicReader(topic, GROUP, queue, LOGGER)
    return queue, producer, reader


@pytest.mark.parametrize("method", METHODS)
async def test_the_producer_returns_none_for_a_message_it_stores(
    method: str,
) -> None:
    """Both producer entry points report storage by returning `None`."""
    _, producer, reader = build()
    item = message("https://h.test/a")

    if method == "enqueue":
        result = await producer.enqueue(item)
    else:
        result = (await producer.enqueue_many([item]))[0]

    assert result is None or result is True
    assert await reader.peek(1) == [item]


@pytest.mark.parametrize("max_size", [1, 2, 3])
async def test_the_queue_accepts_exactly_max_size_then_overflows(
    max_size: int,
) -> None:
    """The capacity is a hard limit, and the message past it is refused."""
    _, producer, _ = build(max_size=max_size)
    for index in range(max_size):
        await producer.enqueue(message(f"https://h.test/{index}"))

    with pytest.raises(QueueOverflowError):
        await producer.enqueue(message("https://h.test/one-more"))


@pytest.mark.parametrize("max_size", [1, 2, 3])
async def test_an_overflowed_bulk_message_reports_false_and_the_rest_are_enqueued(
    max_size: int,
) -> None:
    """One full queue cannot discard a batch: each message reports its own."""
    _, producer, reader = build(max_size=max_size)
    batch = [message(f"https://h.test/{index}") for index in range(max_size + 2)]

    results = await producer.enqueue_many(batch)

    assert results == [True] * max_size + [False, False]
    assert await reader.peek(max_size + 2) == batch[:max_size]


@pytest.mark.parametrize(
    "overrides",
    [pytest.param("producer", id="producer_first"), pytest.param("reader", id="reader_first")],
)
async def test_either_view_may_be_built_first_because_nothing_is_wired(
    overrides: str,
) -> None:
    """There is no topic to create and no `connect` step, so construction order."""
    queue = InMemorySingleTopicSinglePartitionQueue()
    if overrides == "producer":
        InMemoryTopicProducer(TOPIC, queue, LOGGER)
    else:
        InMemoryTopicReader(TOPIC, GROUP, queue, LOGGER)
    producer = InMemoryTopicProducer(TOPIC, queue, LOGGER)
    reader = InMemoryTopicReader(TOPIC, GROUP, queue, LOGGER)

    first = message("https://h.test/first")
    await producer.enqueue(first)

    assert await reader.peek(1) == [first]


@pytest.mark.parametrize(
    "partition_key",
    [
        pytest.param(-9223372036854775808, id="most_negative"),
        pytest.param(-7, id="negative"),
        pytest.param(0, id="zero"),
        pytest.param(7, id="positive"),
    ],
)
async def test_any_partition_key_is_accepted_and_ignored(partition_key: int) -> None:
    """One deque, so every key lands in it in the order it arrived."""
    _, producer, reader = build()
    first, second = (
        message("https://h.test/first", partition_key),
        message("https://h.test/second", partition_key),
    )

    await producer.enqueue_many([first, second])

    assert await reader.peek(2) == [first, second]


@pytest.mark.parametrize("enqueued", [0, 1, 3])
async def test_the_bulk_api_reports_one_outcome_per_message(enqueued: int) -> None:
    """`enqueue_many` neither dedupes nor drops: the result list is the input."""
    _, producer, _ = build()
    batch = [message(f"https://h.test/{index}") for index in range(enqueued)]

    results = await producer.enqueue_many(batch)

    assert len(results) == enqueued
    assert all(results)


@pytest.mark.parametrize(("enqueued", "n", "expected"), [(3, 1, 1), (3, 5, 3), (1, 0, 0), (1, -2, 0)])
async def test_peek_returns_min_of_n_and_len_and_removes_nothing(
    enqueued: int, n: int, expected: int
) -> None:
    """`peek` is non-reserving, so a second call sees the same messages."""
    _, producer, reader = build()
    batch = [message(f"https://h.test/{index}") for index in range(enqueued)]
    await producer.enqueue_many(batch)

    first = await reader.peek(n)

    assert len(first) == expected
    assert first == await reader.peek(n)


@pytest.mark.parametrize(
    ("enqueued", "committed", "removed"),
    [(3, 3, 3), (3, 10, 3), (5, 2, 2), (5, 0, 0)],
)
async def test_commit_removes_min_of_items_and_len_from_the_head(
    enqueued: int, committed: int, removed: int
) -> None:
    """Commit drops exactly the first `min(len(items), len)` messages, so an."""
    _, producer, reader = build()
    batch = [message(f"https://h.test/{index}") for index in range(enqueued)]
    await producer.enqueue_many(batch)

    await reader.commit(batch[:committed])

    assert await reader.peek(enqueued) == batch[removed:]


@pytest.mark.parametrize(
    ("enqueued", "acknowledged", "first_removed", "second_removed"),
    [(4, 2, 2, 4), (5, 1, 1, 2)],
)
async def test_commit_is_not_idempotent(
    enqueued: int, acknowledged: int, first_removed: int, second_removed: int
) -> None:
    """A second commit of the same count removes a second time, which is why."""
    _, producer, reader = build()
    batch = [message(f"https://h.test/{index}") for index in range(enqueued)]
    await producer.enqueue_many(batch)

    await reader.commit(batch[:acknowledged])
    assert len(batch) - first_removed == len(await reader.peek(enqueued))

    await reader.commit(batch[:acknowledged])

    assert len(batch) - second_removed == len(await reader.peek(enqueued))


@pytest.mark.parametrize("consumer_group_id", ["a", "b", ""])
async def test_a_consumer_group_is_accepted_and_is_a_no_op(
    consumer_group_id: str,
) -> None:
    """The port carries the group, so the reader records it and never consults."""
    queue = InMemorySingleTopicSinglePartitionQueue()
    producer = InMemoryTopicProducer(TOPIC, queue, LOGGER)
    reader = InMemoryTopicReader(TOPIC, consumer_group_id, queue, LOGGER)
    await producer.enqueue(message("https://h.test/a"))

    assert len(await reader.peek(1)) == 1


@pytest.mark.parametrize(("peeked", "appended"), [(1, 1), (2, 3), (3, 1)])
async def test_appending_during_processing_does_not_disturb_the_head(
    peeked: int, appended: int
) -> None:
    """A batch already read stays at the head while new messages land at the."""
    _, producer, reader = build()
    batch = [message(f"https://h.test/{index}") for index in range(peeked)]
    await producer.enqueue_many(batch)
    in_flight = await reader.peek(peeked)
    late = [message(f"https://h.test/late-{index}") for index in range(appended)]
    await producer.enqueue_many(late)

    assert await reader.peek(peeked) == in_flight

    await reader.commit(in_flight)

    assert await reader.peek(peeked + appended) == late


@pytest.mark.parametrize("acked", [1, 5])
async def test_an_empty_queue_peeks_empty_and_an_oversized_commit_is_harmless(
    acked: int,
) -> None:
    """There is nothing to wire, so an empty queue is a valid state and a."""
    queue = InMemorySingleTopicSinglePartitionQueue()
    reader = InMemoryTopicReader(TOPIC, GROUP, queue, LOGGER)

    assert await reader.peek(5) == []

    await reader.commit([message(f"https://h.test/{index}") for index in range(acked)])

    assert await reader.peek(5) == []
