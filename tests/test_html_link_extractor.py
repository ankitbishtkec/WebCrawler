"""Tests for `HtmlLinkExtractor`, the exact-hostname scope rule.

The extractor's one decision is scope: an href survives only when its hostname
equals the page's own. Everything asserted here is the returned set of canonical
URL texts, because that set is unordered and its contents are the whole contract.
"""

import pytest

from tests.support import HOST
from webcrawler.domain.custom_url import CustomURL
from webcrawler.infrastructure.html.html_link_extractor import HtmlLinkExtractor

# A nested page, so a relative href that wrongly resolved against the shallow
# seed would land somewhere else and the difference would show.
PAGE: str = f"{HOST}/dir/page.html"
# The shallow page, used where the base URL does not change what is asserted.
SEED: str = f"{HOST}/index.html"


def _texts(links: set[CustomURL]) -> set[str]:
    """Return the canonical text of each link, so a test compares an unordered set.

    Args:
    links: The set `extract` returned.

    Returns:
    set[str]: One canonical URL text per link, so no test can depend on an order.
    """
    return {link.get_url() for link in links}


def _page(hrefs: list[str]) -> str:
    """Return a body holding one anchor per href, joined onto one line.

    Args:
    hrefs: The raw href values, exactly as the markup should carry them.

    Returns:
    str: A body whose anchors carry exactly those href values.
    """
    return "".join(f'<a href="{href}">link</a>' for href in hrefs)


@pytest.mark.parametrize(
    ("href", "expected"),
    [
        ("sibling.html", f"{HOST}/dir/sibling.html"),
        ("sub/deep.html", f"{HOST}/dir/sub/deep.html"),
        ("../parent.html", f"{HOST}/parent.html"),
        ("/rooted.html", f"{HOST}/rooted.html"),
        ("?q=1", f"{HOST}/dir/page.html?q=1"),
    ],
)
def test_relative_href_resolves_against_the_page_and_not_the_seed(
    href: str, expected: str
) -> None:
    """A relative href resolves as a browser reading the page's own URL would.

    Args:
    href: The relative href in the markup.
    expected: The absolute text the resolved link must report.

    Returns:
    None
    """
    result = HtmlLinkExtractor().extract(_page([href]), CustomURL(PAGE))

    assert _texts(result) == {expected}


@pytest.mark.parametrize(
    ("href", "expected"),
    [
        ("https://crawlme.monzo.com/absolute.html", f"{HOST}/absolute.html"),
        (
            "http://crawlme.monzo.com/other-scheme.html",
            "http://crawlme.monzo.com/other-scheme.html",
        ),
        ("https://CRAWLME.MONZO.COM/upper-case.html", f"{HOST}/upper-case.html"),
        (
            "//crawlme.monzo.com/protocol-relative.html",
            f"{HOST}/protocol-relative.html",
        ),
        ("  https://crawlme.monzo.com/padded.html  ", f"{HOST}/padded.html"),
    ],
)
def test_absolute_same_host_href_is_kept(href: str, expected: str) -> None:
    """A link on the page's own host is in scope however it is spelled.

    Args:
    href: The absolute href in the markup.
    expected: The canonical text the surviving link must report.

    Returns:
    None
    """
    result = HtmlLinkExtractor().extract(_page([href]), CustomURL(PAGE))

    assert _texts(result) == {expected}


@pytest.mark.parametrize(
    "off_host_href",
    [
        "https://example.com/elsewhere.html",
        "https://notcrawlme.monzo.com/sibling-subdomain.html",
        "https://www.crawlme.monzo.com/other-subdomain.html",
        "https://crawlme.monzo.com.attacker.test/suffix.html",
    ],
)
def test_off_host_href_is_dropped_while_a_same_host_link_survives(
    off_host_href: str,
) -> None:
    """The scope rule is hostname equality, never a suffix match.

    Args:
    off_host_href: An off-host href that must be dropped.

    Returns:
    None
    """
    html = _page([off_host_href, "/kept.html"])

    result = HtmlLinkExtractor().extract(html, CustomURL(PAGE))

    assert _texts(result) == {f"{HOST}/kept.html"}


@pytest.mark.parametrize(
    ("href", "expected"),
    [
        (
            "https://crawlme.monzo.com:8443/other-port.html",
            f"{HOST}:8443/other-port.html",
        ),
        ("https://crawlme.monzo.com:80/http-port.html", f"{HOST}:80/http-port.html"),
        (
            "https://crawlme.monzo.com:443/default-port.html",
            f"{HOST}/default-port.html",
        ),
    ],
)
def test_same_hostname_on_another_port_stays_in_scope(href: str, expected: str) -> None:
    """Hostname is the whole scope rule, so a port is not compared and 443 drops out.

    Args:
    href: A same-hostname href naming a port.
    expected: The canonical text the surviving link must report.

    Returns:
    None
    """
    result = HtmlLinkExtractor().extract(_page([href]), CustomURL(PAGE))

    assert _texts(result) == {expected}


@pytest.mark.parametrize(
    "self_href",
    [
        PAGE,
        "page.html",
        f"{PAGE}#section",
    ],
)
def test_self_link_is_kept(self_href: str) -> None:
    """A page may link to itself, and the store would refuse a URL it already holds.

    Args:
    self_href: A spelling of the page's own URL.

    Returns:
    None
    """
    result = HtmlLinkExtractor().extract(_page([self_href]), CustomURL(PAGE))

    assert _texts(result) == {PAGE}


@pytest.mark.parametrize(
    ("hrefs", "expected"),
    [
        (["/a.html", "/a.html"], f"{HOST}/a.html"),
        (["/a.html", "/a.html", "/a.html"], f"{HOST}/a.html"),
        (["a.html", f"{HOST}/dir/a.html"], f"{HOST}/dir/a.html"),
    ],
)
def test_two_spellings_of_one_link_collapse_to_a_single_entry(
    hrefs: list[str], expected: str
) -> None:
    """Canonical identity is what a set keys on, so one page yields one row per page.

    Args:
    hrefs: Two or more hrefs naming the same target.
    expected: The single canonical text the result must hold.

    Returns:
    None
    """
    result = HtmlLinkExtractor().extract(_page(hrefs), CustomURL(PAGE))

    assert _texts(result) == {expected}


@pytest.mark.parametrize(
    ("first", "second", "expected"),
    [
        ("/a.html", "/a.html#top", f"{HOST}/a.html"),
        ("/a.html#one", "/a.html#two", f"{HOST}/a.html"),
        (f"{HOST}/absolute.html#one", "/absolute.html#two", f"{HOST}/absolute.html"),
    ],
)
def test_a_fragment_only_difference_is_not_a_second_entry(
    first: str, second: str, expected: str
) -> None:
    """A fragment is a client-side view, so `CustomURL` drops it before the set sees it.

    Args:
    first: One href carrying a fragment.
    second: The same target carrying a different fragment.
    expected: The single canonical text the result must hold.

    Returns:
    None
    """
    result = HtmlLinkExtractor().extract(_page([first, second]), CustomURL(PAGE))

    assert _texts(result) == {expected}


@pytest.mark.parametrize(
    ("first", "second", "expected"),
    [
        ("/a.html?b=1&a=2", "/a.html?a=2&b=1", f"{HOST}/a.html?a=2&b=1"),
        ("/a.html?c=3&b=2&a=1", "/a.html?a=1&c=3&b=2", f"{HOST}/a.html?a=1&b=2&c=3"),
        ("/s.html?tag=x&tag=y", "/s.html?tag=y&tag=x", f"{HOST}/s.html?tag=x&tag=y"),
    ],
)
def test_a_query_differing_only_in_parameter_order_is_one_entry(
    first: str, second: str, expected: str
) -> None:
    """Sorting the query pairs is the canonicalisation that dedupes store rows.

    Args:
    first: A href whose query pairs are out of sorted order.
    second: The same query written in a different order.
    expected: The single canonical text the result must hold.

    Returns:
    None
    """
    result = HtmlLinkExtractor().extract(_page([first, second]), CustomURL(PAGE))

    assert _texts(result) == {expected}


@pytest.mark.parametrize(
    "href",
    [
        "mailto:someone@example.com",
        "javascript:void(0)",
        "",
        "https://crawl me.monzo.com/space-in-authority.html",
        "https://crawlme.monzo.com:not-a-port/bad-port.html",
        "https://[::1/unclosed-ipv6.html",
    ],
)
def test_a_href_that_is_not_a_crawlable_http_url_is_skipped(href: str) -> None:
    """A skipped href contributes nothing instead of raising, and the rest still land.

    Args:
    href: A non-http scheme, a blank value, or a URL that cannot be parsed.

    Returns:
    None
    """
    result = HtmlLinkExtractor().extract(_page([href, "/kept.html"]), CustomURL(PAGE))

    assert _texts(result) == {f"{HOST}/kept.html"}


@pytest.mark.parametrize(
    "body",
    [
        "",
        '<a href="/one.html">link</a>',
        '<a href="/one.html">1</a><a href="/two.html">2</a><a href="/three.html">3</a>',
    ],
)
def test_the_result_is_a_set_exactly(body: str) -> None:
    """The port promises a `set`, so a caller can key on it without copying.

    Args:
    body: A page with no link, one link, or several.

    Returns:
    None
    """
    result = HtmlLinkExtractor().extract(body, CustomURL(SEED))

    assert isinstance(result, set)


@pytest.mark.parametrize(
    "body",
    [
        "",
        "<html><body>no anchors at all</body></html>",
        '<a name="anchor">named, not linked</a>',
        "<a>an anchor with no href attribute</a>",
        "<a href>an anchor whose href carries no value</a>",
        '<link rel="stylesheet" href="/style.css">',
    ],
)
def test_a_page_with_no_anchor_href_yields_an_empty_set(body: str) -> None:
    """Only an `<a href>` names a page to crawl, so everything else yields nothing.

    Args:
    body: A body with no usable anchor href in it.

    Returns:
    None
    """
    result = HtmlLinkExtractor().extract(body, CustomURL(PAGE))

    assert _texts(result) == set()


def test_only_the_same_host_links_of_a_mixed_page_survive() -> None:
    """One real page read end to end, with every scope decision in it at once.

    A `<base href>` is ignored, so the rooted link stays on the page's own host.

    Returns:
    None
    """
    body = (
        '<html><head><base href="https://example.com/">'
        '<link rel="stylesheet" href="/style.css"></head><body>'
        '<a href="sibling.html">relative sibling</a>'
        '<a href="../parent.html">relative parent</a>'
        '<a href="/rooted.html">rooted</a>'
        '<a href="https://crawlme.monzo.com/absolute.html">absolute</a>'
        '<a href="sibling.html#anchor">the sibling again</a>'
        '<a href="https://notcrawlme.monzo.com/other-site.html">sibling subdomain</a>'
        '<a href="https://example.com/elsewhere.html">off domain</a>'
        '<a href="mailto:hello@example.com">mail</a>'
        '<a href="javascript:void(0)">script</a>'
        '<a name="self">named</a>'
        "</body></html>"
    )

    result = HtmlLinkExtractor().extract(body, CustomURL(PAGE))

    assert _texts(result) == {
        f"{HOST}/dir/sibling.html",
        f"{HOST}/parent.html",
        f"{HOST}/rooted.html",
        f"{HOST}/absolute.html",
    }
