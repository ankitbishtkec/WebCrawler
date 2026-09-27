"""The orchestrator: the one owner of the crawl loop's lifetime (goal.md:13).

Creates exactly one poller task (goal.md:107) and one worker task, and is the
only place either is created or stopped. Every collaborator is injected, so
`main.py` picks the concrete classes and this class only picks when things run.

`run` is wrapped in `try/finally`, so a `Ctrl+C` during the seed read still
closes the store, which joins the aiosqlite thread.
"""

import asyncio
import logging

from webcrawler.application.url_poller import URLPoller
from webcrawler.application.worker import CrawlerWorker
from webcrawler.domain.custom_url import CustomURL, InvalidURLError
from webcrawler.ports.url_state_repository import URLStateRepository


class Orchestrator:
    """Runs the poller and the worker on one event loop (goal.md:13).

    Args:
    repository: The crawl state store, closed on every exit of `run`.
    poller: The queuer and the one poll loop this class runs.
    worker: The crawl consumer, started once the seed is queued.
    seed_line: The operator's single seed URL, read before any task starts.
    logger: The injected logger; DEBUG for no seed, WARNING for a rejected
        one, ERROR for an unrecordable one or a failed task.
    """

    def __init__(
        self,
        repository: URLStateRepository,
        poller: URLPoller,
        worker: CrawlerWorker,
        seed_line: str,
        logger: logging.Logger,
    ) -> None:
        """Hold the collaborators; nothing is started here.

        Args:
        repository: The crawl state store.
        poller: The queuer and the one poll loop this class runs.
        worker: The crawl consumer.
        seed_line: The operator's single seed URL.
        logger: The injected logger.

        Returns:
        None
        """
        self._repository = repository
        self._poller = poller
        self._worker = worker
        self._seed_line = seed_line
        self._logger = logger

    async def run(self) -> None:
        """Seed the crawl once, then run the poller and the worker together.

        `main.py` read the seed before this was called, so it is validated as a
        single `CustomURL`, inserted with `create_urls`, then queued with
        `enqueue_urls`. The insert must precede the enqueue, because the queuer
        claims rows that must already exist. Both tasks start only after that,
        so exactly one poller task exists and none runs with nothing to crawl.

        `asyncio.TaskGroup` runs the two tasks, so one failing takes the other
        with it. The group awaits every task before it returns, so the `finally`
        needs only to close the store.

        Args:
        None.

        Returns:
        None: Always. A failed task is logged at ERROR and ends the session.
        """
        try:
            if not self._seed_line:
                self._logger.debug(
                    "no seed URL was entered, so there is nothing to crawl "
                    "and the session ends"
                )
                return
            try:
                seed = CustomURL(self._seed_line)
            except InvalidURLError as error:
                self._logger.warning(
                    "the seed %r was rejected as a single URL, so there "
                    "is nothing to crawl and the session ends: %s",
                    self._seed_line,
                    error,
                )
                return
            try:
                await self._repository.create_urls([seed])
                await self._poller.enqueue_urls([seed])
            except Exception as error:
                self._logger.error(
                    "the seed %s could not be recorded or queued, so there "
                    "is nothing to crawl and the session ends: %s",
                    seed.get_url(),
                    error,
                )
                return
            self._logger.debug(
                "the crawl is running from the seed %s; interrupt it to stop",
                seed.get_url(),
            )
            try:
                async with asyncio.TaskGroup() as group:
                    group.create_task(self._poller.run())
                    group.create_task(self._worker.run())
            except Exception as error:
                self._logger.error(
                    "a crawl task failed, so the crawl is over: %s", error
                )
        finally:
            await self._repository.close()
