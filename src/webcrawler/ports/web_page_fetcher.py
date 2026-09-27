"""The page-fetch port (goal.md:141).

`goal.md:141` requires the fetch to go through an interface and to be given a
retry policy, so the transport stays replaceable and the retry knobs live with
the caller instead of being hard-coded per implementation.
"""

from abc import ABC, abstractmethod

from webcrawler.domain.custom_url import CustomURL
from webcrawler.ports.retry_policy import RetryPolicy

class WebPageFetcher(ABC):
    """Retrieves one page body for a URL, retrying under a given policy."""

@abstractmethod
async def fetch(self, url: CustomURL, retry_policy: RetryPolicy) -> str:
    """Return the response body for one URL.

    The policy is passed per call rather than held by the fetcher, so one
    policy instance can be shared with the store and the worker and every
    I/O in the process then retries with the same settings
    (goal.md:17).

    Args:
    url: The page to retrieve.
    retry_policy: The timeout, backoff, and jitter to apply to every
    attempt. The fetcher does not retry by itself; it hands its
    single attempt to this policy.

    Returns:
    str: The decoded response body of a successful (2xx) response.

    Raises:
    Exception: The last transport or status error, after the policy
    has exhausted its attempts. A non-2xx status and a transport
    failure are both raised rather than returned, so no caller
    can mistake an error page for a page.
    """
