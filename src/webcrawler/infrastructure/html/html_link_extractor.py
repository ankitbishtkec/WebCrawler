"""Same-host link extraction: every anchor href resolved against its own page.

Read the hrefs, build full URLs, and drop everything not on the page's own host.
"""

import logging
from urllib.parse import urljoin

from webcrawler.domain.custom_url import CustomURL, InvalidURLError
from webcrawler.ports.link_extractor import LinkExtractor
from webcrawler.utils.html_parser import collect_hrefs


class HtmlLinkExtractor(LinkExtractor):
    """Turns one page body into the unique same-host links it points at.

    The class extends the `LinkExtractor` ABC and is extended by nothing, so the
    worker's parsing step stays substitutable.
    """

    def __init__(self) -> None:
        """Hold nothing; nothing is parsed until `extract` is called."""
        self._logger = logging.getLogger(__name__)

    def extract(self, html: str, base_url: CustomURL) -> set[CustomURL]:
        """Return the page's unique in-scope links, resolved against the page.

        `CustomURL` hashing collapses duplicates and fragments.

        Args:
            html: The page body to parse.
            base_url: The URL the body was fetched from, used to resolve relative hrefs.

        Returns:
            set[CustomURL]: The unique in-scope links, unordered, with fragments already dropped.

        Synchronous: pure CPU parsing, no I/O.
        """
        page_url = base_url.get_url()
        hrefs = collect_hrefs(html)
        unique: set[CustomURL] = set()
        for href in hrefs:
            target = self._to_url(href, page_url)
            # Exact hostname, never a suffix: a sibling subdomain such as
            # `notcrawlme.monzo.com` ends with `crawlme.monzo.com` yet is
            # somebody else's site, so equality is the whole scope rule.
            if target is None or target.hostname != base_url.hostname:
                continue
            unique.add(target)
        # DEBUG, and not higher, because the summary is routine: a page full of
        # `mailto:` hrefs is ordinary rather than an error.
        self._logger.debug(
            "extracted %d same-host link(s) from %d href(s) on %s",
            len(unique),
            len(hrefs),
            page_url,
        )
        return unique

    def _to_url(self, href: str, page_url: str) -> CustomURL | None:
        """Resolve one href against the page and parse the result.

        None for a blank href, a non-http scheme, or a malformed URL.

        Args:
            href: The raw attribute value, as the markup carried it.
            page_url: The canonical URL of the page the href was read from.

        Returns:
            CustomURL | None: The resolved link, or None when it is not a crawlable http(s) URL.
        """
        candidate = href.strip()
        if not candidate:
            return None
        try:
            return CustomURL(urljoin(page_url, candidate))
        except InvalidURLError as error:
            self._logger.debug("skipping %r on %s: %s", candidate, page_url, error)
            return None
