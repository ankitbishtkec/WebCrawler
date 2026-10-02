"""The composition root: the one module that names concrete classes.

Every other module sees a port, so this is where the concrete classes are
chosen and connected: one queue behind both queue views, one retry policy each
for the store and the fetches, one politeness policy that never waits, and the
operator's choice between the two `CrawlWorker` implementations.
"""


import asyncio
import logging
import sys
from datetime import timedelta

from webcrawler.application.orchestrator import Orchestrator
from webcrawler.application.url_poller import URLPoller
from webcrawler.application.worker import CrawlerWorker
from webcrawler.application.worker_v1 import CrawlerWorkerV1
from webcrawler.domain.retry_settings import RetrySettings
from webcrawler.infrastructure.db.sqlite_url_state_repository import (
    SQLiteURLStateRepository)
from webcrawler.infrastructure.fetch.aiohttp_web_page_fetcher import (
    AiohttpWebPageFetcher)
from webcrawler.infrastructure.fetch.headers_middleware import (
    DEFAULT_HEADERS,
    HeadersMiddleware,
)
from webcrawler.infrastructure.html.html_link_extractor import HtmlLinkExtractor
from webcrawler.infrastructure.politeness.no_op_politeness_policy import (
    NoOpPolitenessPolicy)
from webcrawler.infrastructure.queue.in_memory_topic_producer import (
    InMemoryTopicProducer)
from webcrawler.infrastructure.queue.in_memory_topic_reader import (
    InMemoryTopicReader)
from webcrawler.infrastructure.queue.in_memory_single_topic_single_partition_queue import (
    InMemorySingleTopicSinglePartitionQueue)
from webcrawler.infrastructure.retry.exponential_backoff_retry_policy import (
    ExponentialBackoffRetryPolicy)
from webcrawler.infrastructure.time.system_time_provider import SystemTimeProvider
from webcrawler.ports.crawl_worker import CrawlWorker
from webcrawler.ports.request_middleware import RequestMiddleware
from webcrawler.utils.logger import configure_logging

# The values a deployment overrides; each one is a constructor argument below.
LOG_LEVEL: int = logging.INFO
DB_FILE: str = "webcrawler.db"
BATCH_SIZE: int = 50
# Must exceed the fetcher's whole retry budget, three attempts of 12s plus up
# to 25s of backoff, or a URL still retrying is re-claimed and fetched twice.
JOB_TIMEOUT: timedelta = timedelta(minutes=2)
# A `queued` row older than this is re-claimed and re-queued, which is how a
# message lost between the claim and the enqueue is recovered. It must outlast
# a real backlog, or every waiting row is re-fetched while it still waits.
QUEUE_TIMEOUT: timedelta = timedelta(minutes=60)
# A failed URL waits this long before it is due again, so a site that is
# rate limiting the crawl gets a quiet window instead of one burst a minute.
RESCHEDULE_DELAY: timedelta = timedelta(minutes=5)
TOPIC: str = "crawl"
CONSUMER_GROUP_ID: str = "crawler"
# The store and the fetcher need opposite retry shapes, so they get their own
# settings: a locked database comes free in milliseconds, while a 429 is only
# cleared on a seconds-scale window.
STORE_RETRY_SETTINGS: RetrySettings = RetrySettings(
    max_attempts=3,
    base_delay_seconds=0.2,
    max_delay_seconds=1.0,
    jitter_seconds=0.1,
    timeout_seconds=12.0)
FETCH_RETRY_SETTINGS: RetrySettings = RetrySettings(
    max_attempts=3,
    base_delay_seconds=5.0,
    max_delay_seconds=60.0,
    jitter_seconds=5.0,
    timeout_seconds=12.0)
#a bit lower timeout for http call is used compared to the task object timeout
HTTP_CALL_TIMEOUT_SECONDS: int = FETCH_RETRY_SETTINGS.timeout_seconds - 2.0
# The seed used when the operator just presses Enter at the prompt.
DEFAULT_SEED_URL: str = "https://crawlme.monzo.com"
# The workers the operator picks at the prompt, keyed by what they type. The
# default is the non-blocking one, since it measured faster on both sites tried:
# the batch worker idles behind the slowest page of the batch it is holding.
WORKER_CHOICES: dict[str, type[CrawlWorker]] = {
    "1": CrawlerWorker,
    "2": CrawlerWorkerV1,
}
DEFAULT_WORKER_CHOICE: str = "2"


async def main() -> None:
    """Wire every concrete class to its port and run one seed-once crawl.

    The seed and the worker choice are read before any task exists, so the
    loop is never blocked.

    Raises:
    asyncio.CancelledError: When the run is interrupted; the
    orchestrator's shutdown has already stopped both tasks and closed
    the store, and `asyncio.run` then presents the interrupt to this
    file's guard as KeyboardInterrupt.
    Exception: Whatever a collaborator raises outside the orchestrator's
    handled seed paths, propagated unchanged.
    """
    # Logging first: everything that logs is constructed after it. `--debug`
    # lowers the level only; a fetched url and its links stay at INFO either way.
    logger = configure_logging(
        level=logging.DEBUG if "--debug" in sys.argv[1:] else LOG_LEVEL
    )
    print(
        f"Enter the seed URL to crawl, then press Ctrl+C at any time to "
        f"stop. Press Enter to crawl default seed url {DEFAULT_SEED_URL}."
        )
    seed_line = input("seed url> ").strip() or DEFAULT_SEED_URL
    # The worker's behaviour is the operator's choice. Only the difference the
    # operator has to act on goes in the prompt; the rest is in the README. The
    # default is named from the key, so the two cannot drift apart.
    print(
        f"Pick the worker: 1 = {WORKER_CHOICES['1'].__name__}, which waits for "
        f"each batch to finish. 2 = {WORKER_CHOICES['2'].__name__}, which keeps "
        f"fetching while earlier pages are still in flight, up to 1000 at a "
        f"time. Press Enter for "
        f"{WORKER_CHOICES[DEFAULT_WORKER_CHOICE].__name__}."
    )
    worker_choice = input("worker> ").strip() or DEFAULT_WORKER_CHOICE
    if worker_choice not in WORKER_CHOICES:
        logger.warning(
            "%r is not a worker choice, so %s is used",
            worker_choice,
            WORKER_CHOICES[DEFAULT_WORKER_CHOICE].__name__,
        )
        worker_choice = DEFAULT_WORKER_CHOICE
    worker_class = WORKER_CHOICES[worker_choice]
    logger.info("crawling with %s", worker_class.__name__)
    time_provider = SystemTimeProvider()
    # One queue behind both views: the poller must fill the queue the worker
    # reads, or the crawl stops after the seed.
    queue = InMemorySingleTopicSinglePartitionQueue()
    producer = InMemoryTopicProducer(TOPIC, queue)
    reader = InMemoryTopicReader(TOPIC, CONSUMER_GROUP_ID, queue)
    # One retry policy per I/O owner; the store is quick, the fetches are slow.
    store_retry_policy = ExponentialBackoffRetryPolicy(STORE_RETRY_SETTINGS)
    repository = SQLiteURLStateRepository(
        DB_FILE, store_retry_policy, time_provider)
    worker: CrawlWorker | None = None
    # This file builds what the run needs, so this is where they are released.
    try:
        await repository.initialize()
        # The only source of request headers, applied in order; an auth middleware
        # belongs here too (README's Extensions section).
        request_middlewares: tuple[RequestMiddleware, ...] = (
            HeadersMiddleware(DEFAULT_HEADERS),)
        fetch_retry_policy = ExponentialBackoffRetryPolicy(FETCH_RETRY_SETTINGS)
        fetcher = AiohttpWebPageFetcher(
            HTTP_CALL_TIMEOUT_SECONDS,
            fetch_retry_policy,
            middlewares=request_middlewares)
        link_extractor = HtmlLinkExtractor()
        # No wait is ever requested, so a crawl is not throttled; a delaying policy
        # is an extension (README) and the worker already drives any of them.
        politeness_policy = NoOpPolitenessPolicy()
        poller = URLPoller(
            repository,
            producer,
            job_timeout=JOB_TIMEOUT,
            queue_timeout=QUEUE_TIMEOUT,
            time_provider=time_provider)
        # Both workers take the same ports in the same order, so the choice
        # made above is the only thing that differs between the two.
        worker = worker_class(
            repository,
            reader,
            fetcher,
            link_extractor,
            politeness_policy,
            poller,
            producer=producer,
            batch_size=BATCH_SIZE,
            reschedule_delay=RESCHEDULE_DELAY,
            time_provider=time_provider)
        orchestrator = Orchestrator(repository, poller, worker, seed_line)
        await orchestrator.run()
    finally:
        if worker is not None:
            try:
                await worker.close()
            except Exception as error:
                logger.error(
                    "releasing the pooled http session failed, so it is left to "
                    "the garbage collector: %s", error
                )
        await repository.close()


if __name__ == "__main__":  # importing this module has no side effects
    asyncio.run(main())
