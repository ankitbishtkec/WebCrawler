"""Same-host link extraction: every anchor href resolved against its own page.

`goal.md:142` is the rule this module exists for: read the hrefs, build full
URLs, and drop everything that is not on the page's own hostname. `goal.md:1`
fixes what "own" means, the exact hostname, so a sibling subdomain is a
different host and a suffix match is never acceptable: `notcrawlme.monzo.com`
ends with `crawlme.monzo.com` yet is somebody else's site.

A page that links to itself is not dropped. The extractor is a pure function
over one document, the crawl state DB already refuses to insert a URL it holds,
and hiding a self-reference here would lose the fact that the page links to
itself rather than remove a duplicate.
"""

import logging
from urllib.parse import urljoin

from webcrawler.domain.custom_url import CustomURL, InvalidURLError
from webcrawler.ports.link_extractor import LinkExtractor
from webcrawler.utils.html_parser import collect_hrefs


class HtmlLinkExtractor(LinkExtractor):
    """Turns one page body into the unique same-host links it points at.

    The class extends the `LinkExtractor` ABC and is extended by nothing, so the
    worker's parsing step stays substitutable (`goal.md:8`).

    Args:
        logger: The injected logger. The per-page summary is DEBUG; a rejected
            href is DEBUG too, because a page full of `mailto:` links is
            ordinary rather than an error.
    """

    def __init__(self, logger: logging.Logger) -> None:
        """Hold the logger; nothing is parsed until `extract` is called.

        Args:
            logger: The injected logger, the only one this class writes to.
        """
        self._logger = logger

    def extract(self, html: str, base_url: CustomURL) -> list[CustomURL]:
        """Return the page's unique in-scope links, resolved against the page.

        Order is first seen, so the crawl's own log lists a page's links the
        way the document did and a test can assert on it; duplicates collapse
        because `CustomURL` hashes its canonical form, which also drops the
        fragment and therefore folds `#one` and `#two` of one page together.

        Args:
            html: The page body to parse.
            base_url: The URL the body was fetched from. It resolves the
                relative hrefs and it names the one host in scope, which is why
                a nested page resolves against itself and not against the seed.

        Returns:
            list[CustomURL]: The in-scope links in first-seen order, with
                duplicates collapsed and every fragment already dropped.

        Synchronous: pure CPU parsing, no I/O.
        """
        page_url = base_url.get_url()
        hrefs = collect_hrefs(html)
        # A dict keyed by CustomURL is a set that still remembers insertion
        # order, so dedupe and determinism come from one structure.
        unique: dict[CustomURL, None] = {}
        for href in hrefs:
            target = self._to_url(href, page_url)
            if target is None or target.hostname != base_url.hostname:
                continue
            unique.setdefault(target, None)
        links = list(unique)
        self._logger.debug(
            "extracted %d same-host link(s) from %d href(s) on %s",
            len(links),
            len(hrefs),
            page_url,
        )
        return links

    def _to_url(self, href: str, page_url: str) -> CustomURL | None:
        """Resolve one href against the page and parse the result.

        Returns None for anything that names no crawlable page, which is a blank
        href, a non-http scheme such as `mailto:`, or a malformed URL. Those are
        skipped rather than raised, because one bad link must not cost a page
        the rest of its links (`goal.md:142`).

        Args:
            href: The raw attribute value, as the markup carried it.
            page_url: The canonical URL of the page the href was read from.

        Returns:
            CustomURL | None: The resolved link, or None when it is not a
                crawlable http(s) URL.
        """
        candidate = href.strip()
        if not candidate:
            return None
        try:
            return CustomURL(urljoin(page_url, candidate))
        except InvalidURLError as error:
            self._logger.debug("skipping %r on %s: %s", candidate, page_url, error)
            return None
