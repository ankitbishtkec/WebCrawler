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

    def __init__(self, logger: logging.Logger) -> None:
        """Hold the logger; nothing is parsed until `extract` is called.

        Args:
            logger: The injected logger, the only one this class writes to.
        """
        self._logger = logger

    def extract(self, html: str, base_url: CustomURL) -> list[CustomURL]:
        """Return the page's unique in-scope links, resolved against the page.

        First-seen order; `CustomURL` hashing collapses duplicates and fragments.

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
            # Exact hostname, never a suffix: a sibling subdomain such as
            # `notcrawlme.monzo.com` ends with `crawlme.monzo.com` yet is
            # somebody else's site, so equality is the whole scope rule.
            if target is None or target.hostname != base_url.hostname:
                continue
            # A self-link is kept, not dropped: the store refuses to insert a URL
            # it already holds, so hiding it would lose the fact that a page
            # links to itself rather than remove a duplicate.
            unique.setdefault(target, None)
        links = list(unique)
        # DEBUG, and not higher, because the summary is routine: a page full of
        # `mailto:` hrefs is ordinary rather than an error.
        self._logger.debug(
            "extracted %d same-host link(s) from %d href(s) on %s",
            len(links),
            len(hrefs),
            page_url,
        )
        return links

    def _to_url(self, href: str, page_url: str) -> CustomURL | None:
        """Resolve one href against the page and parse the result.

        None for a blank href, a non-http scheme, or a malformed URL.

        Args:
            href: The raw attribute value, as the markup carried it.
            page_url: The canonical URL of the page the href was read from.

        Returns:
            CustomURL | None: The resolved link, or None when it is not a
                crawlable http(s) URL.
        """
        # Skipped, not raised, so one bad link costs the page none of its others;
        # logged at DEBUG because a page of `mailto:` hrefs is ordinary.
        candidate = href.strip()
        if not candidate:
            return None
        try:
            return CustomURL(urljoin(page_url, candidate))
        except InvalidURLError as error:
            self._logger.debug("skipping %r on %s: %s", candidate, page_url, error)
            return None
