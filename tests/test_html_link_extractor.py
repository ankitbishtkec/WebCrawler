"""Happy-path tests for `HtmlLinkExtractor`, the exact-hostname scope rule.

The extractor is a pure function on a string, so nothing is mocked. Its one
decision is scope: a link survives only when its hostname equals the page's
own, and the returned set of canonical texts is the whole answer.
"""

import pytest

from webcrawler.domain.custom_url import CustomURL
from webcrawler.infrastructure.html.html_link_extractor import HtmlLinkExtractor

HOST: str = "https://crawlme.monzo.com"

# A nested page, so a relative href resolved against the wrong base would land
# somewhere else and the difference would show.
PAGE: str = f"{HOST}/dir/page.html"


def texts(links: set[CustomURL]) -> set[str]:
    """Return the canonical text of each link, so a test compares an unordered set.

    Args:
    links: The set `extract` returned.

    Returns:
    set[str]: One canonical URL text per link, so no test can depend on an order.
    """
    return {link.get_url() for link in links}


@pytest.mark.parametrize(
    ("html", "expected"),
    [
        (
            '<a href="/a.html">a</a><a href="/b.html">b</a>',
            {f"{HOST}/a.html", f"{HOST}/b.html"},
        ),
        (
            '<a href="/a.html">a</a><a href="/a.html">a again</a>',
            {f"{HOST}/a.html"},
        ),
    ],
)
def test_same_host_links_are_returned(html: str, expected: set[str]) -> None:
    """A page yields exactly its own-host links, one entry per real page.

    Args:
    html: The page body, holding one anchor per link.
    expected: The canonical texts `extract` must return, in any order.

    Returns:
    None
    """
    links = HtmlLinkExtractor().extract(html, CustomURL(PAGE))

    assert texts(links) == expected


def test_an_off_host_link_is_dropped() -> None:
    """A link to somebody else's site is out of scope, and a same-host one stays.

    Returns:
    None
    """
    html = (
        '<a href="https://example.com/elsewhere.html">other</a>'
        '<a href="/kept.html">kept</a>'
    )

    links = HtmlLinkExtractor().extract(html, CustomURL(PAGE))

    assert texts(links) == {f"{HOST}/kept.html"}


def test_a_relative_href_resolves_against_the_page_url() -> None:
    """A relative href resolves as a browser reading this page's own URL would.

    Returns:
    None
    """
    html = '<a href="sibling.html">sibling</a>'

    links = HtmlLinkExtractor().extract(html, CustomURL(PAGE))

    assert texts(links) == {f"{HOST}/dir/sibling.html"}
