"""Tests for `Orchestrator`, the seed gate and the one owner of the loop.

The release paths are the composition root's, so they are driven through
`main.main()` with an empty seed, which returns before either loop starts.
"""

import asyncio
import contextlib
import inspect
from collections.abc import Callable
from pathlib import Path

import pytest

from tests.support import (
    HOST,
    FakeFetcher,
    make_poller,
    make_producer,
    make_queue,
    make_reader,
    make_repository,
    make_worker,
    queued_texts,
    rows_by_state,
)
from webcrawler import main
from webcrawler.application.orchestrator import Orchestrator
from webcrawler.domain.crawl_state import CrawlState

SEED: str = f"{HOST}/index.html"

# Short enough that a seeded crawl has already started, long enough not to spin.
POLL_SECONDS: float = 0.005

# How long a drive keeps yielding before it cancels the loop anyway.
DEADLINE_SECONDS: float = 2.0


class _CountingStore:
    """A store double that only counts its own release.

    Not in `support.py` because it exists solely to observe the composition
    root's release path, which no other test drives.

    Args:
    close_fails: Whether `close` raises, so a test can see which release failed.
    """

    def __init__(self, close_fails: bool) -> None:
        """Record whether the release raises and start with no release.

        Args:
        close_fails: Whether `close` raises instead of returning.
        """
        self._close_fails = close_fails
        self.closed = 0

    async def initialize(self) -> None:
        """Accept the schema creation, which the release path does not care about.

        Returns:
        None
        """
        return None

    async def close(self) -> None:
        """Count the call, then raise when this double is configured to fail.

        Returns:
        None

        Raises:
        RuntimeError: When this double was built with `close_fails`.
        """
        self.closed += 1
        if self._close_fails:
            raise RuntimeError("the store could not be released")


class _CountingFetcher:
    """A fetcher double that only counts its own release.

    Not in `support.py` because it exists solely to observe the composition
    root's release path; `FakeFetcher` always releases cleanly.

    Args:
    close_fails: Whether `close` raises, so a test can see which release failed.
    """

    def __init__(self, close_fails: bool) -> None:
        """Record whether the release raises and start with no release.

        Args:
        close_fails: Whether `close` raises instead of returning.
        """
        self._close_fails = close_fails
        self.closed = 0

    async def close(self) -> None:
        """Count the call, then raise when this double is configured to fail.

        Returns:
        None

        Raises:
        RuntimeError: When this double was built with `close_fails`.
        """
        self.closed += 1
        if self._close_fails:
            raise RuntimeError("the session could not be released")


async def run_until(orchestrator: Orchestrator, done: Callable[[], bool]) -> None:
    """Run the orchestrator until the condition holds, then cancel it.

    Args:
    orchestrator: The orchestrator whose `run` is driven.
    done: The condition that ends the drive, checked between yields.

    Returns:
    None
    """
    task = asyncio.create_task(orchestrator.run())
    deadline = asyncio.get_running_loop().time() + DEADLINE_SECONDS
    try:
        while not done() and asyncio.get_running_loop().time() < deadline:
            await asyncio.sleep(POLL_SECONDS)
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


@pytest.mark.parametrize(
    "seed_line", ["", "not a url", "/relative.html", "mailto:someone@monzo.com"]
)
async def test_a_seed_that_is_not_crawlable_queues_nothing(
    seed_line: str, tmp_path: Path
) -> None:
    """An empty or rejected seed ends the session instead of raising.

    Args:
    seed_line: The operator's entry that names no crawlable URL.
    tmp_path: This test's private directory, holding its own database file.

    Returns:
    None
    """
    database = str(tmp_path / "crawl.db")
    repository = make_repository(database)
    await repository.initialize()
    queue = make_queue()
    producer = make_producer(queue)
    try:
        orchestrator = Orchestrator(
            repository,
            make_poller(repository, producer),
            make_worker(
                repository,
                queue=queue,
                reader=make_reader(queue),
                producer=producer,
                fetcher=FakeFetcher(),
            ),
            seed_line,
        )

        assert await orchestrator.run() is None

        assert rows_by_state(database) == {}
        assert queued_texts(queue) == []
    finally:
        await repository.close()


async def test_a_valid_seed_is_inserted_and_queued(tmp_path: Path) -> None:
    """The one seed becomes a row and one message, and the crawl then runs.

    Args:
    tmp_path: This test's private directory, holding its own database file.

    Returns:
    None
    """
    database = str(tmp_path / "crawl.db")
    repository = make_repository(database)
    await repository.initialize()
    queue = make_queue()
    producer = make_producer(queue)
    try:
        orchestrator = Orchestrator(
            repository,
            make_poller(repository, producer),
            # A batch of zero messages idles the worker, so the seed step is
            # observed on its own rather than racing the crawl it starts.
            make_worker(
                repository,
                queue=queue,
                reader=make_reader(queue),
                producer=producer,
                fetcher=FakeFetcher(bodies={SEED: "<html>a page</html>"}),
                batch_size=0,
            ),
            SEED,
        )

        await run_until(orchestrator, lambda: len(queued_texts(queue)) == 1)

        assert rows_by_state(database) == {CrawlState.QUEUED.value: 1}
        assert queued_texts(queue) == [SEED]
    finally:
        await repository.close()


@pytest.mark.parametrize(
    ("worker_close_fails", "repository_close_fails", "raises"),
    [(False, False, False), (True, False, False), (False, True, True)],
    ids=["both_released", "session_release_fails", "store_release_fails"],
)
async def test_the_release_path_always_releases_the_store(
    worker_close_fails: bool,
    repository_close_fails: bool,
    raises: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Both are released exactly once, and only the store's failure is propagated.

    Args:
    worker_close_fails: Whether the fetcher's release raises.
    repository_close_fails: Whether the store's release raises.
    raises: Whether `main` itself must raise.
    monkeypatch: The test's patch of the composition root's collaborators.

    Returns:
    None
    """
    fetcher = _CountingFetcher(worker_close_fails)
    store = _CountingStore(repository_close_fails)
    monkeypatch.setattr(main, "AiohttpWebPageFetcher", lambda *_, **__: fetcher)
    monkeypatch.setattr(main, "SQLiteURLStateRepository", lambda *_, **__: store)
    monkeypatch.setattr("builtins.input", lambda _prompt="": "")

    if raises:
        with pytest.raises(RuntimeError):
            await main.main()
    else:
        await main.main()

    assert fetcher.closed == 1
    assert store.closed == 1


def test_the_orchestrator_takes_no_logger() -> None:
    """Every class logs for itself, so none is handed one.

    Returns:
    None
    """
    assert "logger" not in inspect.signature(Orchestrator.__init__).parameters
