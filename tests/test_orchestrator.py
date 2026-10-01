"""Unit tests for `Orchestrator`, the seed gate and the one owner of the loop."""

from typing import NamedTuple
from unittest.mock import MagicMock, call

from webcrawler.application.orchestrator import Orchestrator
from webcrawler.application.url_poller import URLPoller
from webcrawler.application.worker import CrawlerWorker
from webcrawler.domain.custom_url import CustomURL
from webcrawler.ports.url_state_repository import URLStateRepository

SEED: str = "https://crawlme.monzo.com/index.html"

# A seed string that names no crawlable URL, so the session ends before the store.
REJECTED: str = "not a url"


class _Built(NamedTuple):
    """The orchestrator under test, with the mocks it was built with."""

    orchestrator: Orchestrator
    repository: MagicMock
    poller: MagicMock
    worker: MagicMock
    seed_calls: MagicMock


def _build_orchestrator(seed_line: str) -> _Built:
    """Build the orchestrator over mocks, with both loops stubbed to return at once."""
    repository = MagicMock(spec=URLStateRepository)
    poller = MagicMock(spec=URLPoller)
    poller.run.return_value = None
    worker = MagicMock(spec=CrawlerWorker)
    worker.run.return_value = None
    # One manager over both seed calls, so a test can read the order they happened in.
    seed_calls = MagicMock()
    seed_calls.attach_mock(repository.create_urls, "create_urls")
    seed_calls.attach_mock(poller.enqueue_urls, "enqueue_urls")

    orchestrator = Orchestrator(repository, poller, worker, seed_line)
    return _Built(orchestrator, repository, poller, worker, seed_calls)


async def test_a_valid_seed_is_created_in_the_store() -> None:
    """The one seed becomes one row, created in a single store call."""
    built = _build_orchestrator(SEED)

    await built.orchestrator.run()

    built.repository.create_urls.assert_awaited_once_with({CustomURL(SEED)})


async def test_a_valid_seed_is_queued_by_the_poller() -> None:
    """The same seed is then handed to the poller as a one URL list."""
    built = _build_orchestrator(SEED)

    await built.orchestrator.run()

    built.poller.enqueue_urls.assert_awaited_once_with([CustomURL(SEED)])


async def test_the_seed_is_created_before_it_is_queued() -> None:
    """The row is written first, since the poller only queues rows that exist."""
    built = _build_orchestrator(SEED)

    await built.orchestrator.run()

    assert built.seed_calls.mock_calls == [
        call.create_urls({CustomURL(SEED)}),
        call.enqueue_urls([CustomURL(SEED)]),
    ]


async def test_run_starts_the_poller_and_the_worker() -> None:
    """One poll loop and one worker loop, started together once the seed is in."""
    built = _build_orchestrator(SEED)

    await built.orchestrator.run()

    built.poller.run.assert_awaited_once_with()
    built.worker.run.assert_awaited_once_with()


async def test_a_seed_that_is_not_a_url_creates_and_queues_nothing() -> None:
    """A rejected seed ends the session, so the store and the queue are untouched."""
    built = _build_orchestrator(REJECTED)

    await built.orchestrator.run()

    built.repository.create_urls.assert_not_awaited()
    built.poller.enqueue_urls.assert_not_awaited()


async def test_an_empty_seed_creates_and_queues_nothing() -> None:
    """An empty seed ends the session before the seed is even parsed as a URL."""
    built = _build_orchestrator("")

    await built.orchestrator.run()

    built.repository.create_urls.assert_not_awaited()
    built.poller.run.assert_not_awaited()
    built.worker.run.assert_not_awaited()


async def test_a_seed_the_store_cannot_create_queues_and_crawls_nothing() -> None:
    """A failed insert ends the session, so the queue and both loops are untouched."""
    built = _build_orchestrator(SEED)
    built.repository.create_urls.side_effect = RuntimeError("the insert failed")

    await built.orchestrator.run()

    built.poller.enqueue_urls.assert_not_awaited()
    built.poller.run.assert_not_awaited()
    built.worker.run.assert_not_awaited()


async def test_a_seed_the_poller_cannot_queue_is_still_a_durable_row() -> None:
    """The row outlives the failure, so only the two loops are never started."""
    built = _build_orchestrator(SEED)
    built.poller.enqueue_urls.side_effect = RuntimeError("the claim failed")

    await built.orchestrator.run()

    built.repository.create_urls.assert_awaited_once_with({CustomURL(SEED)})
    built.poller.run.assert_not_awaited()
    built.worker.run.assert_not_awaited()


async def test_a_crawl_task_that_fails_returns_instead_of_raising() -> None:
    """One failed task is logged, so `run` returns after both loops were started."""
    built = _build_orchestrator(SEED)
    built.worker.run.side_effect = RuntimeError("the worker died")

    assert await built.orchestrator.run() is None

    built.poller.run.assert_awaited_once_with()
    built.worker.run.assert_awaited_once_with()

