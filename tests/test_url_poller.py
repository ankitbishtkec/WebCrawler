"""Unit tests for `URLPoller`, the producer half of the crawl loop.

The store and the producer are mocks, so each test only asks which claim the
poller made and which messages it fed. `run` never returns, so the happy path
is driven through `queue_candidates`, the one public call that polls.
"""

from datetime import datetime, timedelta, timezone
from typing import NamedTuple
from unittest.mock import MagicMock

import pytest

from webcrawler.application.url_poller import URLPoller
from webcrawler.domain.custom_url import CustomURL
from webcrawler.domain.messages import BaseMessage
from webcrawler.ports.time_provider import TimeProviderFactory
from webcrawler.ports.topic_producer import TopicProducer
from webcrawler.ports.url_state_repository import URLStateRepository

HOST: str = "https://crawlme.monzo.com"

# The instant the pinned clock hands the poller, so the claim is predictable.
NOW: datetime = datetime(2026, 9, 29, 6, 0, tzinfo=timezone.utc)

# The two staleness windows the poller was configured with.
JOB_TIMEOUT: timedelta = timedelta(minutes=1)
QUEUE_TIMEOUT: timedelta = timedelta(seconds=30)

# The caller's dedupe key, forwarded to the queue untouched.
REQUEST_ID: str = "test-request"

# The row limit one call is given, checked against the store's own claim.
MAX_ITEMS: int = 7


def _claimed(count: int) -> set[CustomURL]:
    """Return the `count` URLs the mocked store hands back from a claim.

    Args:
    count: How many rows the claim returned.

    Returns:
    set[CustomURL]: The claimed URLs, the one set the poller and the test both read.
    """
    return {CustomURL(f"{HOST}/page-{index}.html") for index in range(1, count + 1)}


class _Built(NamedTuple):
    """The poller under test, with the mocked store and producer behind it.

    Args:
    poller: The poller, built over a mock store and a mock producer.
    repository: The mocked crawl state store.
    producer: The mocked queue write side.
    claimed: The rows the mocked store returns from every claim.
    """

    poller: URLPoller
    repository: MagicMock
    producer: MagicMock
    claimed: set[CustomURL]


def _build_poller(claimed: set[CustomURL]) -> _Built:
    """Build the poller over mocks, with `claimed` waiting to be fed.

    Args:
    claimed: The rows every claim returns, an empty set for a poll that found nothing.

    Returns:
    _Built: The poller and the two mocks to assert on.
    """
    repository = MagicMock(spec=URLStateRepository)
    repository.claim_candidates.return_value = claimed
    producer = MagicMock(spec=TopicProducer)
    producer.enqueue_many.return_value = [True] * len(claimed)
    clock = MagicMock(spec=TimeProviderFactory)
    clock.now.return_value = NOW

    poller = URLPoller(
        repository,
        producer,
        job_timeout=JOB_TIMEOUT,
        queue_timeout=QUEUE_TIMEOUT,
        time_provider=clock,
    )
    return _Built(poller, repository, producer, claimed)


@pytest.mark.parametrize("count", [0, 2])
async def test_the_claimed_urls_are_fed_as_one_message_each(count: int) -> None:
    """One bulk call carries one message per claimed URL, under the caller's key.

    Args:
    count: How many rows the claim returned.

    Returns:
    None
    """
    claimed = _claimed(count)
    built = _build_poller(claimed)

    await built.poller.queue_candidates(NOW, REQUEST_ID)

    built.producer.enqueue_many.assert_awaited_once_with(
        [BaseMessage(url, partition_key=hash(url)) for url in claimed], REQUEST_ID
    )


async def test_every_message_is_routed_by_the_hash_of_its_own_url() -> None:
    """`partition_key` is `hash(url)`, the value the producer stores verbatim.

    Returns:
    None
    """
    claimed = _claimed(2)
    built = _build_poller(claimed)

    await built.poller.queue_candidates(NOW, REQUEST_ID)

    built.producer.enqueue_many.assert_awaited_once()
    messages = built.producer.enqueue_many.await_args.args[0]
    assert [message.partition_key for message in messages] == [
        hash(url) for url in claimed
    ]


async def test_the_claim_is_asked_for_with_the_configured_timeouts_and_limit() -> None:
    """The store's claim gets the instant, the row limit, and both windows.

    Returns:
    None
    """
    built = _build_poller(_claimed(1))

    await built.poller.queue_candidates(NOW, REQUEST_ID, MAX_ITEMS)

    built.repository.claim_candidates.assert_awaited_once_with(
        NOW, MAX_ITEMS, job_timeout=JOB_TIMEOUT, queue_timeout=QUEUE_TIMEOUT
    )


async def test_a_failed_claim_feeds_the_queue_nothing() -> None:
    """A claim that raises ends the poll, so the feed is never reached.

    Returns:
    None
    """
    built = _build_poller(_claimed(2))
    built.repository.claim_candidates.side_effect = RuntimeError("the claim failed")

    await built.poller.queue_candidates(NOW, REQUEST_ID)

    built.producer.enqueue_many.assert_not_awaited()


async def test_a_failed_caller_url_claim_feeds_the_queue_nothing() -> None:
    """The one guard covers the other claim too, so the caller's URLs are not fed.

    Returns:
    None
    """
    built = _build_poller(set())
    built.repository.claim_urls.side_effect = RuntimeError("the claim failed")

    await built.poller.enqueue_urls([CustomURL(f"{HOST}/page-1.html")], REQUEST_ID)

    built.producer.enqueue_many.assert_not_awaited()


async def test_a_failed_feed_is_raised_to_the_caller() -> None:
    """The feed is unguarded, so a networked producer's failure ends the poll.

    Returns:
    None
    """
    built = _build_poller(_claimed(1))
    built.producer.enqueue_many.side_effect = RuntimeError("the send failed")

    with pytest.raises(RuntimeError, match="the send failed"):
        await built.poller.queue_candidates(NOW, REQUEST_ID)

    built.repository.claim_candidates.assert_awaited_once()


async def test_a_rejected_message_does_not_stop_the_claim_being_fed() -> None:
    """One overflowed message is only reported, so the whole claim is still sent.

    Returns:
    None
    """
    built = _build_poller(_claimed(2))
    built.producer.enqueue_many.return_value = [False, True]

    await built.poller.queue_candidates(NOW, REQUEST_ID)

    built.producer.enqueue_many.assert_awaited_once_with(
        [BaseMessage(url, partition_key=hash(url)) for url in built.claimed],
        REQUEST_ID,
    )

