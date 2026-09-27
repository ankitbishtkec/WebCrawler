"""The orchestrator: the one owner of the crawl loop's lifetime (goal.md:13).

One module that runs all the others on a single asyncio
event loop. This class creates exactly one poller task (`goal.md:107`) and one
worker task, and is the only place either is created or stopped. Every
collaborator arrives injected, so the choice of concrete classes is the
composition root's (`main.py`) and this class decides only when
things run.

`run` is wrapped in `try/finally` so a `Ctrl+C` during the seed read
still closes the store — which joins the aiosqlite thread.
"""

import asyncio
import logging

from webcrawler.application.url_poller import URLPoller
from webcrawler.application.worker import CrawlerWorker
from webcrawler.domain.custom_url import CustomURL, InvalidURLError
from webcrawler.ports.url_state_repository import URLStateRepository


async def _settle(task: asyncio.Task[None]) -> None:
    """Wait for one task this class created, absorbing whatever it raised.

    Args:
    task: The task to wait for, which may be cancelled, failed, or finished.

    Raises:
    Exception: Nothing is raised
    """
    # `asyncio.wait` never re-raises what the task raised, so a task
    # re-delivering an error the caller is already propagating cannot skip
    # the shutdown that follows.
    await asyncio.wait([task])
    if not task.cancelled():
        # Retrieved, so asyncio does not warn about a failure nobody looked at.
        task.exception()


def _unwrapped(failures: ExceptionGroup) -> BaseException:
    """Return the one failure a task group reports, so it can be re-raised.

    Args:
    failures: The group the task group raised, holding one or more failures.

    Returns:
    BaseException: The single underlying failure, or the group itself when a
    task really did fail more than once.
    """
    if len(failures.exceptions) == 1:
        return failures.exceptions[0]
    return failures

class Orchestrator:
    """Runs the crawl's two long-lived tasks on one event loop (goal.md).

    No project base: this class composes ports and owns a lifetime, and
    nothing is shared with it by inheritance ( goal.md). The
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
    logger: The injected logger. A missing seed is DEBUG, a rejected one is
    WARNING, and an unrecordable one is ERROR.
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
        one poller task can exist (`goal.md`) and no task runs with
        nothing to crawl. The crawl then runs until `Ctrl+C`: the await is
        wrapped in `try/finally`, which cancels and awaits both tasks and
        then awaits `repository.close`, so no task or connection thread is
        left running.
        
        An invalid seed, or an exception from `create_urls` or `enqueue_urls`
        (an exhausted DB retry), is logged as WARNING or ERROR and the session
        ends, because with no seed there is nothing to crawl.

        One `asyncio.TaskGroup` awaits both tasks, so either ending — cancelled
        or failed — takes the other with it rather than leaving a dead task and
        a silently stopped crawl. The group re-raises a task's failure wrapped
        in an `ExceptionGroup`, so a lone failure is unwrapped and re-raised as
        itself, which keeps `KeyboardInterrupt` and `CancelledError` on their
        own unwrapped path.

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
                self._logger.debug(
                    "no seed URL was entered, so there is nothing to crawl "
                    "and the session ends"
                    )
                return
            try:
                seed = CustomURL(seed_line)
            except InvalidURLError as error:
                self._logger.warning(
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
                self._logger.error(
                    "the seed %s could not be recorded or queued, so there "
                    "is nothing to crawl and the session ends: %s",
                    seed.get_url(),
                    error)
                return
            self._logger.debug(
                "the crawl is running from the seed %s; interrupt it to stop",
                seed.get_url())
            try:
                async with asyncio.TaskGroup() as group:
                    poller_task = group.create_task(self._poller.run())
                    worker_task = group.create_task(self._worker.run())
            except ExceptionGroup as failures:
                error = _unwrapped(failures)
                self._logger.error(
                    "a crawl task failed, so the crawl is over: %s", error)
                # `from None`: the group only wrapped the failure just logged,
                # so the traceback shows the cause rather than the wrapper too.
                raise error from None
        finally:
            for task in (poller_task, worker_task):
                if task is not None and not task.done():
                    task.cancel()
            tasks = [task for task in (poller_task, worker_task) if task is not None]
            if tasks:
                # A task re-delivering the error this try is already propagating
                # must not skip the close below, so `_settle` absorbs it.
                async with asyncio.TaskGroup() as group:
                    for task in tasks:
                        group.create_task(_settle(task))
            await self._repository.close()
