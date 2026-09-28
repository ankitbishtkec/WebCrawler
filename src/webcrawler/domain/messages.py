from dataclasses import dataclass

from webcrawler.domain.custom_url import CustomURL

class QueueOverflowError(RuntimeError):
    """Raised when an enqueue would exceed the deque max size.

    Args:
    message: The reason the enqueue was rejected.
    """

@dataclass(frozen=True)
class BaseMessage:
    """One queued unit of work: a URL to crawl and the key that routes it.

    A frozen value type with no project base, so the poller, the producer, and
    the worker can all hold and compare the same object without any of them
    being able to alter it after the fact.

    Args:
    url: The URL to crawl. Its identity is the canonical form, so the
    message compares and hashes stably across producers.
    partition_key: The routing key, `hash(url)`, stored verbatim.
    """

    url: CustomURL
    partition_key: int
