"""End-to-end smoke test: one live check per layer of the crawler.

This is deliberately not a contract suite. Each test names the layer and does
the smallest real thing that layer must be able to do, so a failure here points
straight at the broken layer instead of at a detail.

Run with `-m smoke` to execute just these.
"""

import asyncio
import inspect
import logging
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from webcrawler.domain.custom_url import CustomURL
from webcrawler.domain.messages import BaseMessage
from webcrawler.domain.retry_settings import RetrySettings
from webcrawler.infrastructure.db.sqlite_url_state_repository import (
    SQLiteURLStateRepository,
)
from webcrawler.infrastructure.html.html_link_extractor import HtmlLinkExtractor
from webcrawler.infrastructure.politeness.no_op_politeness_policy import (
    NoOpPolitenessPolicy,
)
from webcrawler.infrastructure.queue.in_memory_single_topic_single_partition_queue import (
    InMemorySingleTopicSinglePartitionQueue,
)
from webcrawler.infrastructure.queue.in_memory_topic_producer import (
    InMemoryTopicProducer,
)
from webcrawler.infrastructure.queue.in_memory_topic_reader import InMemoryTopicReader
from webcrawler.infrastructure.retry.exponential_backoff_retry_policy import (
    ExponentialBackoffRetryPolicy,
)
from webcrawler.infrastructure.time.system_time_provider import SystemTimeProvider
from webcrawler.ports.url_state_repository import URLStateRepository

pytestmark = pytest.mark.smoke

NOW = datetime(2026, 9, 26, 11, 28, tzinfo=timezone.utc)
JOB_TIMEOUT = timedelta(minutes=1)
QUEUE_TIMEOUT = timedelta(seconds=30)


def _policy() -> ExponentialBackoffRetryPolicy:
    """Return a policy that retries fast, so a test never waits on backoff."""
    return ExponentialBackoffRetryPolicy(
        RetrySettings(
            max_attempts=2,
            base_delay_seconds=0.001,
            max_delay_seconds=0.002,
            jitter_seconds=0.0,
            timeout_seconds=5.0,
        )
    )


def test_domain_builds_a_canonical_url() -> None:
    """A URL keeps its path and gains a sorted query."""
    assert CustomURL("https://h.test/a?z=1&a=2").get_url() == "https://h.test/a?a=2&z=1"


def test_html_layer_keeps_same_domain_links_only() -> None:
    """Extraction follows the goal: same domain, no subdomains, no externals."""
    links = HtmlLinkExtractor(logging.getLogger("smoke")).extract(
        '<a href="/a">1</a><a href="https://h.test/b">2</a>'
        '<a href="https://other.test/c">3</a>',
        CustomURL("https://h.test/"),
    )

    assert [link.get_url() for link in links] == ["https://h.test/a", "https://h.test/b"]


async def test_queue_peek_does_not_consume() -> None:
    """A peek is repeatable, and only a commit removes the messages."""
    queue = InMemorySingleTopicSinglePartitionQueue(max_size=4)
    reader = InMemoryTopicReader("urls", "smoke", queue, logging.getLogger("smoke"))
    await InMemoryTopicProducer("urls", queue, logging.getLogger("smoke")).enqueue(
        BaseMessage(CustomURL("https://h.test/a"), 0)
    )

    first = [message.url.get_url() for message in await reader.peek(5)]

    assert first == ["https://h.test/a"]
    assert [message.url.get_url() for message in await reader.peek(5)] == ["https://h.test/a"]

    await reader.commit(await reader.peek(5))

    assert await reader.peek(5) == []


def test_store_round_trips_a_url() -> None:
    """The store creates a row, offers it, claims it, and records the finish."""
    store = SQLiteURLStateRepository(
        str(Path(tempfile.mkdtemp()) / "smoke.db"),
        _policy(),
        SystemTimeProvider(),
        logging.getLogger("smoke"),
    )

    async def run() -> list[str]:
        """Drive the store through one full cycle.

        Args:
            None.

        Returns:
            list[str]: The URLs the store offered for crawling.
        """
        await store.initialize()
        url = CustomURL("https://h.test/a")
        await store.create_urls([url])
        offered = await store.get_crawlable_urls(
            NOW, -1, job_timeout=JOB_TIMEOUT, queue_timeout=QUEUE_TIMEOUT
        )
        await store.claim_urls(
            [url], NOW, -1, job_timeout=JOB_TIMEOUT, queue_timeout=QUEUE_TIMEOUT
        )
        await store.mark_started(url, NOW)
        await store.complete_crawl([(url, None)], [CustomURL("https://h.test/b")], NOW)
        await store.close()
        return [item.get_url() for item in offered]

    assert asyncio.run(run()) == ["https://h.test/a"]


@pytest.mark.parametrize(
    ("protocol", "method", "expected"),
    [
        (URLStateRepository, "get_crawlable_urls", "job_timeout"),
        (URLStateRepository, "claim_candidates", "queue_timeout"),
        (URLStateRepository, "complete_crawl", "discovered"),
    ],
)
def test_ports_expose_their_contract(
    protocol: type, method: str, expected: str
) -> None:
    """Each port still names the arguments the application depends on."""
    assert expected in str(inspect.signature(getattr(protocol, method)))


def test_application_layers_import() -> None:
    """The worker, poller, and orchestrator are constructible modules."""
    from webcrawler.application.orchestrator import Orchestrator
    from webcrawler.application.url_poller import URLPoller
    from webcrawler.application.worker import CrawlerWorker

    for component in (CrawlerWorker, URLPoller, Orchestrator):
        assert hasattr(component, "run")


def test_infrastructure_policies_are_usable() -> None:
    """The shared policies answer the questions the application asks of them."""
    policy = _policy()

    assert NoOpPolitenessPolicy().before_fetch() == 0
    assert callable(policy.execute)
