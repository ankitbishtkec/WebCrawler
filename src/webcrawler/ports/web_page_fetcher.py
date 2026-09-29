"""The page-fetch port.

The fetch goes through an interface and is given a retry policy, so the
transport stays replaceable and the retry knobs live with the caller instead of
being hard-coded per implementation.
"""

from abc import ABC, abstractmethod

from webcrawler.domain.custom_url import CustomURL


class WebPageFetcher(ABC):
    """Retrieves one page body for a URL, retrying under a given policy.

    The fetcher owns its transport lifetime: `fetch` reuses one pooled session
    and `close` releases it.
    """

    @abstractmethod
    async def fetch(self, url: CustomURL) -> str:
        """Return the response body for one URL, from a session shared by all calls.

        The implementation retries its own transport, so the caller passes no policy.

        Args:
            url: The page to retrieve.

        Returns:
            str: The decoded response body of a successful (2xx) response.

        Raises:
            Exception: The last transport or status error, after the policy has exhausted its attempts. A non-2xx status and a transport failure are both raised rather than returned, so no caller can mistake an error page for a page.
        """

    @abstractmethod
    async def close(self) -> None:
        """Release the pooled session this fetcher opened.

        Raises:
            Exception: A networked implementation may raise while closing its client; the shipped one never does.
        """
