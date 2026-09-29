"""The link-extraction port.

Pure CPU work over a body already in memory, so this is the one crawl port that
is deliberately synchronous: a coroutine would add an await point and move no
I/O off the loop.
"""

from abc import ABC, abstractmethod

from webcrawler.domain.custom_url import CustomURL


class LinkExtractor(ABC):
    """Finds the next crawl targets in one page body."""

    @abstractmethod
    def extract(self, html: str, base_url: CustomURL) -> set[CustomURL]:
        """Return the unique same-host links found in a page body.

        Relative hrefs resolve against `base_url`, the page just fetched and not the seed, so a nested relative link resolves as a browser would. Off-host links are dropped here, the one place the exact-hostname scope is applied.

        Args:
            html: The page body to parse.
            base_url: The URL the body came from, used to resolve relative hrefs and to decide which host is in scope.

        Returns:
            set[CustomURL]: The unique in-scope links, unordered, with fragments already dropped by `CustomURL`.

        Raises:
            Exception: If the body cannot be parsed; the worker treats that as one failed URL.

        Synchronous: pure CPU parsing, no I/O.
        """
