"""The orchestrator: the one owner of the crawl loop's lifetime (goal.md:13).

`goal.md:13` asks for one module that runs all the others on a single asyncio
event loop. This class creates exactly one poller task (`goal.md:107`) and one
worker task, and is the only place either is created or stopped. Every
collaborator arrives injected, so the choice of concrete classes is the
composition root's (`main.py`) and this class decides only when
things run.

Two decisions carry the design:

- The seed is read once by `main.py` before any task exists and arrives as a
string, so no second seed can be set. It is validated as one `CustomURL`,
never split on commas, then inserted and queued.
- The tasks are created only after the seed is queued, which makes "exactly
one poller task" (`goal.md:107`) structural rather than conventional, and
`run` is wrapped in `try/finally` so a `Ctrl+C` during the seed read
still closes the store — which joins the aiosqlite thread.
"""

import asyncio
import contextlib
import logging

from webcrawler.application.url_poller import URLPoller
from webcrawler.application.worker import CrawlerWorker
from webcrawler.domain.custom_url import CustomURL, InvalidURLError
from webcrawler.ports.url_state_repository import URLStateRepository

class Orchestrator:
    """Runs the crawl's two long-lived tasks on one event loop (goal.md:13).

    No project base: this class composes ports and owns a lifetime, and
    nothing is shared with it by inheritance ( goal.md:8). The
    seed-once flow is the whole of its behaviour: read one line, make it a
    row, hand it to the queue, then run the poller and the worker until the
    run is interrupted.

    Args:
    repository: The crawl state store, whose connection this class
    closes on every exit of `run`.
    poller: The queuer, both the seed's enqueue and the one poll-loop
    task this class starts.
    worker: The crawl consumer, started once the seed is queued.
    seed_line: The operator's single seed URL, read before any task
    starts, so the crawl never runs without one.
    logger: The injected logger. A seed that cannot start the crawl is
    INFO, because an ended session is the operator's next decision,
    not an error condition.
    """

    def __init__(
        self,
        repository: URLStateRepository,
        poller: URLPoller,
        worker: CrawlerWorker,
        seed_line: str,
        logger: logging.Logger) -> None:
        """Hold the collaborators; nothing is started here.

        Args:
        repository: The crawl state store.
        poller: The queuer and the one poll loop this class runs.
        worker: The crawl consumer.
        seed_line: The operator's single seed URL.
        logger: The injected logger.
        """
        self._repository = repository
        self._poller = poller
        self._worker = worker
        self._seed_line = seed_line
        self._logger = logger

    async def run(self) -> None:
        """Own the single poller and worker instances (goal.md:13).
        
        `main.py` read the seed before this was called — one blocking
        `input` — so no seed is read here and none can be set again. It is
        validated as a single `CustomURL`, inserted with
        `repository.create_urls([seed])`, then queued with
        `poller.enqueue_urls([seed])`: the insert must precede the enqueue
        because the queuer claims rows that must already exist.
        
        The two tasks are created only after the seed is queued, so exactly
        one poller task can exist (`goal.md:107`) and no task runs with
        nothing to crawl. The crawl then runs until `Ctrl+C`: the await is
        wrapped in `try/finally`, which cancels and awaits both tasks and
        then awaits `repository.close`, so no task or connection thread is
        left running.
        
        An invalid seed, or an exception from `create_urls` or `enqueue_urls`
        (an exhausted DB retry), is logged at INFO and the session ends,
        because with no seed there is nothing to crawl.
        
        One `asyncio.gather` awaits both tasks, so either ending — cancelled
        or failed — takes the other with it rather than leaving a dead task
        and a silently stopped crawl.
        
        Raises:
        asyncio.CancelledError: When the run is interrupted; both tasks
        and the store are already closed by then.
        Exception: Whatever a task raises outside the handled seed
        paths, propagated unchanged after the shutdown has run.
        """
        poller_task: asyncio.Task[None] | None = None
        worker_task: asyncio.Task[None] | None = None
        try:
            seed_line = self._seed_line
            if not seed_line:
                self._logger.info(
                    "no seed URL was entered, so there is nothing to crawl "
                    "and the session ends"
                    )
                return
            try:
                seed = CustomURL(seed_line)
            except InvalidURLError as error:
                self._logger.info(
                    "the seed %r was rejected as a single URL, so there "
                    "is nothing to crawl and the session ends: %s",
                    seed_line,
                    error)
                return
            try:
                await self._repository.create_urls([seed])
                # The insert precedes the enqueue: the queuer claims rows that
                # must already exist.
                await self._poller.enqueue_urls([seed])
            except Exception as error:
                self._logger.info(
                    "the seed %s could not be recorded or queued, so there "
                    "is nothing to crawl and the session ends: %s",
                    seed.get_url(),
                    error)
                return
            self._logger.info(
                "the crawl is running from the seed %s; interrupt it to stop",
                seed.get_url())
            poller_task = asyncio.create_task(self._poller.run())
            worker_task = asyncio.create_task(self._worker.run())
            await asyncio.gather(poller_task, worker_task)
        finally:
            for task in (poller_task, worker_task):
                if task is not None and not task.done:
                    task.cancel()
            tasks = [task for task in (poller_task, worker_task) if task is not None]
            if tasks:
                # return_exceptions: a task re-delivering the error this try
                # is already propagating must not skip the close below.
                await asyncio.gather(*tasks, return_exceptions=True)
            await self._repository.close()
