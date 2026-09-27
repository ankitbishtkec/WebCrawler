"""The link-extraction port (goal.md:142).

Extraction is pure CPU work over a body that is already in memory, so this is
the one crawl port that is deliberately synchronous: making it a coroutine
would add an await point without moving any I/O off the loop.
"""

from abc import ABC, abstractmethod

from webcrawler.domain.custom_url import CustomURL


class LinkExtractor(ABC):
    """Finds the next crawl targets in one page body."""

    @abstractmethod
    def extract(self, html: str, base_url: CustomURL) -> list[CustomURL]:
        """Return the unique same-host links found in a page body.

        Relative hrefs are resolved against `base_url`, which is the page just
        fetched and not the seed, so a nested relative link resolves the way a
        browser would (goal.md:142). Off-host links are dropped here, the one
        place that applies the exact-hostname scope of `goal.md:1`.

        Args:
            html: The page body to parse.
            base_url: The URL the body was fetched from, used to resolve
                relative hrefs and to decide which host is in scope.

        Returns:
            list[CustomURL]: The unique in-scope links in first-seen order,
                with duplicates collapsed and fragments already dropped by
                `CustomURL`.

        Synchronous: pure CPU parsing, no I/O.
        """
