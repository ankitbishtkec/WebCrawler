"""The crawl worker's port.

The consumer half of the crawl loop, stated as an interface;
`ports/crawl_queuer.py` is the producer half. Deliberately two methods and
nothing else: the queue, the store, the fetcher and every schedule are
constructor arguments of the implementation, so a worker is anything that
consumes the queue and releases what it owns.
"""

from abc import ABC, abstractmethod


class CrawlWorker(ABC):
    """Consumes the crawl queue until the crawl is stopped, then releases itself.

    `run` is the loop a caller starts as one task and stops by cancelling, and
    `close` is what that caller runs afterwards, so a worker owns everything it
    opened and nothing it was handed.
    """

    @abstractmethod
    async def run(self) -> None:
        """Consume the crawl queue until cancelled.

        Never returns on its own: a crawl ends when the operator interrupts it,
        so the loop's only exit is the cancellation its caller sends.

        Returns:
        None: Never, on its own; the crawl ends on cancellation.

        Raises:
        asyncio.CancelledError: When cancelled, which is how the orchestrator stops the crawl.
        Exception: Whatever a failed iteration raises, which ends the crawl rather than one URL.
        """

    @abstractmethod
    async def close(self) -> None:
        """Release what this worker owns, on the way out.

        Called once by whoever built the worker, after `run` has stopped, so an
        implementation can free what it holds before it goes: the resources it
        owns, or a buffer whose contents are written out first.

        Raises:
        Exception: A networked implementation may raise while closing what it owns; the shipped ones only close a pooled session.
        """
