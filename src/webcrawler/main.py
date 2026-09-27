"""The composition root: the one module that names concrete classes.

Every other module sees a port; this is where the implementations of `plan.md`
are chosen and connected. Three decisions matter:

- One `InMemorySingleTopicSinglePartitionQueue` behind both views, so the
poller fills the queue the worker reads.
- One `ExponentialBackoffRetryPolicy` shared by the store and the fetches,
so every I/O owner retries with the same settings (`goal.md:17`).
- One `NoOpPolitenessPolicy`: it reports no wait, so a crawl is not
throttled. A delaying policy is an extension (README).

Importing this module has no side effects: the console loop starts only under
the `__main__` guard.
"""

import asyncio
import logging
import sys
from datetime import timedelta

from webcrawler.application.orchestrator import Orchestrator
from webcrawler.application.url_poller import URLPoller
from webcrawler.application.worker import CrawlerWorker
from webcrawler.domain.retry_settings import RetrySettings
from webcrawler.infrastructure.db.sqlite_url_state_repository import (
    SQLiteURLStateRepository)
from webcrawler.infrastructure.fetch.aiohttp_web_page_fetcher import (
    AiohttpWebPageFetcher)
from webcrawler.infrastructure.fetch.headers_middleware import HeadersMiddleware
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
from webcrawler.ports.request_middleware import RequestMiddleware
from webcrawler.utils.logger import configure_logging

# The values a deployment overrides; each one is a constructor argument below.
DB_FILE: str = "webcrawler.db"
BATCH_SIZE: int = 30
JOB_TIMEOUT: timedelta = timedelta(minutes=1)
QUEUE_TIMEOUT: timedelta = timedelta(seconds=30)
RESCHEDULE_DELAY: timedelta = timedelta(minutes=1)
TOPIC: str = "crawl"
CONSUMER_GROUP_ID: str = "crawler"
RETRY_SETTINGS: RetrySettings = RetrySettings(
    max_attempts=3,
    base_delay_seconds=0.5,
    max_delay_seconds=8.0,
    jitter_seconds=0.5,
    timeout_seconds=10.0)
SLEEP_THRESHOLD_MS: int = 2_000 # the largest wait the worker sleeps through
# Several sites answer 503 to a non-browser agent, so the crawler presents as
# a normal browser.
USER_AGENT: str = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
    )

async def main() -> None:
    """Wire every concrete class to its port and run one seed-once crawl.

    The order is the plan's: logging is configured before
    anything that logs is constructed, the store is initialized before the
    crawl starts, the worker's queuer is the poller itself, and the one
    blocking console read happens here — before any task exists, so the
    event loop is never blocked and no second seed can be entered.

    Raises:
    asyncio.CancelledError: When the run is interrupted; the
    orchestrator's shutdown has already stopped both tasks and closed
    the store, and `asyncio.run` then presents the interrupt to this
    file's guard as KeyboardInterrupt.
    Exception: Whatever a collaborator raises outside the orchestrator's
    handled seed paths, propagated unchanged.
    """
    logger = configure_logging(level=logging.INFO)
    print(
        "Enter the seed URL to crawl, then press Ctrl+C at any time to "
        "stop."
        )
    seed_line = input("seed url> ").strip()
    time_provider = SystemTimeProvider()
    # One queue behind both views: the poller must fill the queue the worker
    # reads, or the crawl stops after the seed.
    queue = InMemorySingleTopicSinglePartitionQueue()
    producer = InMemoryTopicProducer(TOPIC, queue, logger)
    reader = InMemoryTopicReader(TOPIC, CONSUMER_GROUP_ID, queue, logger)
    # One retry policy, two owners: the store and the fetches (goal.md:17).
    retry_policy = ExponentialBackoffRetryPolicy(RETRY_SETTINGS)
    repository = SQLiteURLStateRepository(DB_FILE, retry_policy, time_provider, logger)
    await repository.initialize()
    # The only source of request headers, applied in order; an auth middleware
    # belongs here too (README's Extensions section).
    request_middlewares: tuple[RequestMiddleware, ...] = (
        HeadersMiddleware({"User-Agent": USER_AGENT}),)
    fetcher = AiohttpWebPageFetcher(
        RETRY_SETTINGS.timeout_seconds,
        logger,
        middlewares=request_middlewares)
    link_extractor = HtmlLinkExtractor(logger)
    # No wait is ever requested, so a crawl is not throttled; a delaying policy
    # is an extension (README) and the worker already drives any of them.
    politeness_policy = NoOpPolitenessPolicy()
    poller = URLPoller(
        repository,
        producer,
        job_timeout=JOB_TIMEOUT,
        queue_timeout=QUEUE_TIMEOUT,
        time_provider=time_provider,
        logger=logger)
    worker = CrawlerWorker(
        repository,
        reader,
        fetcher,
        link_extractor,
        politeness_policy,
        poller,
        retry_policy,
        producer=producer,
        batch_size=BATCH_SIZE,
        reschedule_delay=RESCHEDULE_DELAY,
        sleep_threshold_ms=SLEEP_THRESHOLD_MS,
        time_provider=time_provider,
        logger=logger)
    orchestrator = Orchestrator(repository, poller, worker, seed_line, logger)
    await orchestrator.run()


if __name__ == "__main__":
    asyncio.run(main())
