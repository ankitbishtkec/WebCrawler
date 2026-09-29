"""Tests for the in-memory topic: one bounded deque behind two views.

The queue is shared by construction, so these tests cover the FIFO order, the
non-destructive peek, the head-based commit that cannot underflow, and the two
capacities being independent of each other.
"""

import pytest

from tests.support import HOST, make_producer, make_queue, make_reader, page_urls, texts
from webcrawler.domain.custom_url import CustomURL
from webcrawler.domain.messages import BaseMessage, QueueOverflowError


def _message(index: int) -> BaseMessage:
    """Build the message for one page index.

    Args:
    index: The page number, which makes the URL and the partition key distinct.

    Returns:
    BaseMessage: A message carrying that page's URL.
    """
    url = CustomURL(f"{HOST}/page-{index}.html")
    return BaseMessage(url, partition_key=hash(url))


def test_enqueue_then_peek_returns_the_messages_in_order() -> None:
    """The deque is FIFO, so a peek reports the enqueue order.

    Returns:
    None
    """
    queue = make_queue()
    for index in range(1, 4):
        queue.enqueue(_message(index))

    assert texts(queue.peek(10)) == page_urls(3)


@pytest.mark.parametrize("n", [1, 2, 5])
def test_two_peeks_return_the_same_messages(n: int) -> None:
    """`peek` is non-reserving, so reading twice loses nothing.

    Args:
    n: How many messages to enqueue and how many each peek asks for.

    Returns:
    None
    """
    queue = make_queue()
    for index in range(1, n + 1):
        queue.enqueue(_message(index))

    assert texts(queue.peek(n)) == texts(queue.peek(n))
    assert texts(queue.peek(n)) == page_urls(n)


def test_commit_removes_exactly_the_batch_it_is_given() -> None:
    """Only the committed head is gone, and the tail is still there to read.

    Returns:
    None
    """
    queue = make_queue()
    for index in range(1, 5):
        queue.enqueue(_message(index))

    queue.commit(2)

    assert texts(queue.peek(10)) == page_urls(4)[2:]


@pytest.mark.parametrize(
    ("committed", "remaining"), [(0, 2), (1, 1), (5, 0)]
)
def test_commit_beyond_what_remains_takes_only_what_is_there(
    committed: int, remaining: int
) -> None:
    """The deque cannot underflow, so an over-sized commit removes what is there.

    Args:
    committed: The batch size handed to `commit`, against a queue holding two.
    remaining: How many messages must survive.

    Returns:
    None
    """
    queue = make_queue()
    for index in range(1, 3):
        queue.enqueue(_message(index))

    queue.commit(committed)

    assert texts(queue.peek(10)) == page_urls(2)[2 - remaining :]


@pytest.mark.parametrize(
    ("max_size", "enqueued", "overflows"),
    [(3, 2, False), (3, 3, False), (3, 4, True)],
)
def test_enqueue_past_the_capacity_raises(
    max_size: int, enqueued: int, overflows: bool
) -> None:
    """The capacity rejects the new message instead of dropping an uncommitted one.

    Args:
    max_size: The crawl queue's capacity.
    enqueued: How many messages to append.
    overflows: Whether the last append must raise `QueueOverflowError`.

    Returns:
    None
    """
    queue = make_queue(max_size=max_size, max_deadletter_size=4)
    messages = [_message(index) for index in range(1, enqueued + 1)]

    if overflows:
        with pytest.raises(QueueOverflowError):
            for message in messages:
                queue.enqueue(message)
        return

    for message in messages:
        queue.enqueue(message)
    assert len(queue.peek(enqueued)) == enqueued


def test_a_full_crawl_queue_does_not_fill_the_deadletter_queue() -> None:
    """The two deques have separate capacities, so a full topic still has room.

    Returns:
    None
    """
    queue = make_queue(max_size=2, max_deadletter_size=2)
    for index in range(1, 3):
        queue.enqueue(_message(index))
    with pytest.raises(QueueOverflowError):
        queue.enqueue(_message(3))

    queue.enqueue_deadletter(_message(4))
    queue.enqueue_deadletter(_message(5))

    assert texts(queue.peek(10)) == page_urls(2)


@pytest.mark.parametrize(
    ("max_deadletter_size", "parked", "accepted"),
    [(1, 0, True), (1, 1, False), (2, 1, True), (2, 2, False)],
)
async def test_a_full_deadletter_queue_is_reported_not_raised(
    max_deadletter_size: int, parked: int, accepted: bool
) -> None:
    """`enqueue_to_deadletter` answers False on overflow, so the caller decides.

    Args:
    max_deadletter_size: The deadletter queue's capacity.
    parked: How many messages are parked before the one under test.
    accepted: Whether that last park must report True.

    Returns:
    None
    """
    producer = make_producer(make_queue(max_deadletter_size=max_deadletter_size))

    for index in range(1, parked + 1):
        await producer.enqueue_to_deadletter(_message(index))

    assert await producer.enqueue_to_deadletter(_message(parked + 1)) is accepted


async def test_the_producer_and_reader_share_one_queue() -> None:
    """What the producer enqueues is what the reader peeks, and peek removes nothing.

    Returns:
    None
    """
    queue = make_queue()
    producer = make_producer(queue)
    reader = make_reader(queue)

    for index in range(1, 3):
        await producer.enqueue(_message(index))

    first = texts(await reader.peek(10))
    second = texts(await reader.peek(10))

    assert first == page_urls(2)
    assert second == first
