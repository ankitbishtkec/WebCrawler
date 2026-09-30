"""Unit tests for `Orchestrator`, the seed gate and the one owner of the loop.

The store, the poller, and the worker are mocks, and both `run` methods return
at once, so a test reads the seed step and stops instead of driving a crawl
that never ends.
"""

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
    """The orchestrator under test, with the mocks it was built with.

    Args:
    orchestrator: The orchestrator, built over three mocked collaborators.
    repository: The mocked crawl state store.
    poller: The mocked poller, whose `run` returns at once.
    worker: The mocked worker, whose `run` returns at once.
    seed_calls: A manager recording the seed's two calls, so their order is visible.
    """

    orchestrator: Orchestrator
    repository: MagicMock
    poller: MagicMock
    worker: MagicMock
    seed_calls: MagicMock


def _build_orchestrator(seed_line: str) -> _Built:
    """Build the orchestrator over mocks, with both loops stubbed to return at once.

    Args:
    seed_line: The operator's single seed URL, valid or not.

    Returns:
    _Built: The orchestrator and the mocks to assert on.
    """
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
    """The one seed becomes one row, created in a single store call.

    Returns:
    None
    """
    built = _build_orchestrator(SEED)

    await built.orchestrator.run()

    built.repository.create_urls.assert_awaited_once_with({CustomURL(SEED)})


async def test_a_valid_seed_is_queued_by_the_poller() -> None:
    """The same seed is then handed to the poller as a one URL list.

    Returns:
    None
    """
    built = _build_orchestrator(SEED)

    await built.orchestrator.run()

    built.poller.enqueue_urls.assert_awaited_once_with([CustomURL(SEED)])


async def test_the_seed_is_created_before_it_is_queued() -> None:
    """The row is written first, since the poller only queues rows that exist.

    Returns:
    None
    """
    built = _build_orchestrator(SEED)

    await built.orchestrator.run()

    assert built.seed_calls.mock_calls == [
        call.create_urls({CustomURL(SEED)}),
        call.enqueue_urls([CustomURL(SEED)]),
    ]


async def test_run_starts_the_poller_and_the_worker() -> None:
    """One poll loop and one worker loop, started together once the seed is in.

    Returns:
    None
    """
    built = _build_orchestrator(SEED)

    await built.orchestrator.run()

    built.poller.run.assert_awaited_once_with()
    built.worker.run.assert_awaited_once_with()


async def test_a_seed_that_is_not_a_url_creates_and_queues_nothing() -> None:
    """A rejected seed ends the session, so the store and the queue are untouched.

    Returns:
    None
    """
    built = _build_orchestrator(REJECTED)

    await built.orchestrator.run()

    built.repository.create_urls.assert_not_awaited()
    built.poller.enqueue_urls.assert_not_awaited()
