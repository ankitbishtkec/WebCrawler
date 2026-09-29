"""The orchestrator: the one owner of the crawl loop's lifetime.

It creates exactly one poller task and one worker task and is the only place
either is created or stopped; every collaborator is injected, so `main.py` picks
the concrete classes and this class only picks when things run.
"""

import asyncio
import logging

from webcrawler.application.url_poller import URLPoller
from webcrawler.application.worker import CrawlerWorker
from webcrawler.domain.custom_url import CustomURL, InvalidURLError
from webcrawler.ports.url_state_repository import URLStateRepository


class Orchestrator:
    """Runs the poller and the worker on one event loop."""

    def __init__(
        self,
        repository: URLStateRepository,
        poller: URLPoller,
        worker: CrawlerWorker,
        seed_line: str,
    ) -> None:
        """Hold the collaborators; nothing is started here.

        Args:
        repository: The crawl state store.
        poller: The queuer and the one poll loop this class runs.
        worker: The crawl consumer.
        seed_line: The operator's single seed URL.

        Returns:
        None
        """
        self._logger = logging.getLogger(__name__)
        self._repository = repository
        self._poller = poller
        self._worker = worker
        self._seed_line = seed_line

    async def run(self) -> None:
        """Seed the crawl once, then run the poller and the worker together.

        The seed is inserted before it is queued, since the queuer claims rows.

        Args:
        None.

        Returns:
        None: Always. A failed task is logged at ERROR and ends the session.
        """
        # `try/finally` so a Ctrl+C during the seed read still closes the store,
        # which joins the aiosqlite thread, and so it closes on every exit.
        try:
            # Levels mark how far the seed got: DEBUG for none entered, WARNING
            # for one rejected as a URL, ERROR for one that could not be stored.
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
            # A group takes both tasks down if one fails and awaits each before it
            # returns, so the `finally` needs only to release the collaborators.
            try:
                async with asyncio.TaskGroup() as group:
                    group.create_task(self._poller.run())
                    group.create_task(self._worker.run())
            except Exception as error:
                self._logger.error(
                    "a crawl task failed, so the crawl is over: %s", error
                )
        finally:
            await self._close_collaborators()

    async def _close_collaborators(self) -> None:
        """Release the worker and the store, on every exit of `run`.

        The two closes are independent, and only the store's can hang a shutdown.
        """
        # A failing session close is logged, never raised, so the store below
        # still closes and joins its aiosqlite thread.
        try:
            await self._worker.close()
        except Exception as error:
            self._logger.error(
                "releasing the pooled http session failed, so it is left to "
                "the garbage collector: %s", error
            )
        # Propagated, because a store that did not close is the one failure
        # this method cannot absorb.
        await self._repository.close()
