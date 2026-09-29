"""The composition root: the one module that names concrete classes.

Every other module sees a port, so this is where the concrete classes are
chosen and connected: one queue behind both queue views, one retry policy
for the store and the fetches, one politeness policy that never waits.
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
from webcrawler.ports.request_middleware import RequestMiddleware
from webcrawler.utils.logger import configure_logging

# The values a deployment overrides; each one is a constructor argument below.
LOG_LEVEL: int = logging.INFO
DB_FILE: str = "webcrawler.db"
BATCH_SIZE: int = 50
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
    timeout_seconds=12.0)
#a bit lower timeout for http call is used compared to the task object timeout
HTTP_CALL_TIMEOUT_SECONDS: int = RETRY_SETTINGS.timeout_seconds - 2.0
# The seed used when the operator just presses Enter at the prompt.
DEFAULT_SEED_URL: str = "https://crawlme.monzo.com"
# Several sites answer 503 to a non-browser agent, so the crawler presents as
# a normal browser.
async def main() -> None:
    """Wire every concrete class to its port and run one seed-once crawl.

    The seed is read before any task exists, so the loop is never blocked.

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
    time_provider = SystemTimeProvider()
    # One queue behind both views: the poller must fill the queue the worker
    # reads, or the crawl stops after the seed.
    queue = InMemorySingleTopicSinglePartitionQueue()
    producer = InMemoryTopicProducer(TOPIC, queue)
    reader = InMemoryTopicReader(TOPIC, CONSUMER_GROUP_ID, queue)
    # One retry policy, two owners: the store and the fetches.
    retry_policy = ExponentialBackoffRetryPolicy(RETRY_SETTINGS)
    repository = SQLiteURLStateRepository(DB_FILE, retry_policy, time_provider)
    await repository.initialize()
    # The only source of request headers, applied in order; an auth middleware
    # belongs here too (README's Extensions section).
    request_middlewares: tuple[RequestMiddleware, ...] = (
        HeadersMiddleware(DEFAULT_HEADERS),)
    fetcher = AiohttpWebPageFetcher(
        HTTP_CALL_TIMEOUT_SECONDS,
        retry_policy,
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
    worker = CrawlerWorker(
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


if __name__ == "__main__":  # importing this module has no side effects
    asyncio.run(main())
