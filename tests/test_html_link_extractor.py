"""Tests for `HtmlLinkExtractor`."""

import inspect
import logging

import pytest

from webcrawler.domain.custom_url import CustomURL
from webcrawler.infrastructure.html.html_link_extractor import HtmlLinkExtractor
from webcrawler.ports.link_extractor import LinkExtractor

LOGGER_NAME = "tests.webcrawler.html"
SEED = "https://crawlme.monzo.com/"
PAGE = "https://crawlme.monzo.com/section/page.html"
SIBLING = "https://crawlme.monzo.com/section/sibling.html"
UP = "https://crawlme.monzo.com/up.html"

@pytest.fixture
def logger() -> logging.Logger:
    """Return the real logger the extractor is given, for caplog to capture."""
    return logging.getLogger(LOGGER_NAME)

@pytest.fixture
def extractor(logger: logging.Logger) -> HtmlLinkExtractor:
    """Return the default extractor, wired the way the composition root wires it."""
    return HtmlLinkExtractor(logger)

def anchors(*hrefs: str) -> str:
    """Return a body whose only markup is one anchor per href."""
    return "".join(f'<a href="{href}">link</a>' for href in hrefs)

def links(*raw_urls: str) -> list[CustomURL]:
    """Return the expected links, as the canonical URLs to compare against."""
    return [CustomURL(raw) for raw in raw_urls]

@pytest.mark.parametrize(
    ("href", "expected"),
    [
    pytest.param("/absolute", "https://crawlme.monzo.com/absolute", id="root"),
    pytest.param(
    "sibling", "https://crawlme.monzo.com/section/sibling", id="sibling"
    ),
    pytest.param("../up", "https://crawlme.monzo.com/up", id="parent"),
    pytest.param("./deep/x", "https://crawlme.monzo.com/section/deep/x", id="dot"),
    pytest.param(
    "https://crawlme.monzo.com/absolute",
    "https://crawlme.monzo.com/absolute",
    id="absolute"),
    pytest.param(
    "//crawlme.monzo.com/protocol",
    "https://crawlme.monzo.com/protocol",
    id="protocol_relative"),
    pytest.param(
    "?q=1", "https://crawlme.monzo.com/section/page.html?q=1", id="query"
    ),
    pytest.param(
    "/x?b=2&a=1", "https://crawlme.monzo.com/x?a=1&b=2", id="sorted_query"
    ),
    ])
def test_an_href_resolves_against_the_page_it_was_read_from(
    extractor: HtmlLinkExtractor, href: str, expected: str
    ) -> None:
    """Relative and absolute hrefs both become full URLs of the same host."""
    assert extractor.extract(anchors(href), CustomURL(PAGE)) == links(expected)

@pytest.mark.parametrize(
    ("base", "expected"),
    [
    pytest.param(PAGE, "https://crawlme.monzo.com/section/sibling", id="page"),
    pytest.param(SEED, "https://crawlme.monzo.com/sibling", id="seed"),
    pytest.param(
    "https://crawlme.monzo.com/a/b/c.html",
    "https://crawlme.monzo.com/a/b/sibling",
    id="deeper"),
    ])
def test_a_relative_href_resolves_against_the_page_not_the_seed(
    extractor: HtmlLinkExtractor, base: str, expected: str
    ) -> None:
    """`goal.md:142` resolves against the page just fetched, so the same href on."""
    assert extractor.extract(anchors("sibling"), CustomURL(base)) == links(expected)

@pytest.mark.parametrize(
    ("hrefs", "expected"),
    [
    pytest.param(
    ["/a", "/b", "/a", "/c", "/b"],
    [
    "https://crawlme.monzo.com/a",
    "https://crawlme.monzo.com/b",
    "https://crawlme.monzo.com/c",
    ],
    id="first_seen_order"),
    pytest.param(
    ["/a#one", "/a#two", "/a"],
    ["https://crawlme.monzo.com/a"],
    id="fragments_are_the_same_page"),
    pytest.param(
    ["https://crawlme.monzo.com/a", "/a"],
    ["https://crawlme.monzo.com/a"],
    id="absolute_and_relative_agree"),
    pytest.param([], [], id="none"),
    ])
def test_duplicate_links_collapse_into_one_unique_set(
    extractor: HtmlLinkExtractor, hrefs: list[str], expected: list[str]
    ) -> None:
    """`goal.md:142` asks for a full unique URL set, and the order it keeps is."""
    assert extractor.extract(anchors(*hrefs), CustomURL(PAGE)) == links(*expected)

@pytest.mark.parametrize(
    ("page", "body", "expected"),
    [
    pytest.param(
    PAGE,
    anchors("../section/sibling.html", "/section/page.html"),
    [SIBLING, PAGE],
    id="page_to_sibling_and_back"),
    pytest.param(
    SIBLING,
    anchors("page.html", "/section/other"),
    [PAGE, "https://crawlme.monzo.com/section/other"],
    id="sibling_back_to_page"),
    ])
def test_a_cycle_keeps_both_ends(
    extractor: HtmlLinkExtractor, page: str, body: str, expected: list[str]
    ) -> None:
    """Two pages that link to each other both report the other, and a self."""
    assert extractor.extract(body, CustomURL(page)) == links(*expected)

@pytest.mark.parametrize(
    ("href", "expected"),
    [
    pytest.param("/page#section", "https://crawlme.monzo.com/page", id="path"),
    pytest.param(
    "sibling#top", "https://crawlme.monzo.com/section/sibling", id="relative"
    ),
    pytest.param(
    "https://crawlme.monzo.com/x?a=1#frag",
    "https://crawlme.monzo.com/x?a=1",
    id="query_survives"),
    pytest.param("#only", PAGE, id="self_reference"),
    ])
def test_no_fragment_leaks_into_an_extracted_url(
    extractor: HtmlLinkExtractor, href: str, expected: str
    ) -> None:
    """A fragment is client-side only, so it is absent from the canonical URL."""
    extracted = extractor.extract(anchors(href), CustomURL(PAGE))
    assert extracted == links(expected)
    assert all("#" not in url.get_url() for url in extracted)

@pytest.mark.parametrize(
    ("href", "expected"),
    [
    pytest.param(
    "https://crawlme.monzo.com/kept",
    ["https://crawlme.monzo.com/kept"],
    id="own_host"),
    pytest.param("https://facebook.com/x", [], id="facebook"),
    pytest.param("https://monzo.com/x", [], id="registrable_domain"),
    pytest.param("https://community.monzo.com/x", [], id="sibling_subdomain"),
    pytest.param("//facebook.com/x", [], id="protocol_relative_facebook"),
    pytest.param(
    "http://crawlme.monzo.com/x",
    ["http://crawlme.monzo.com/x"],
    id="other_scheme_same_host"),
    pytest.param(
    "/relative", ["https://crawlme.monzo.com/relative"], id="relative"
    ),
    pytest.param("", [], id="empty"),
    pytest.param("mailto:someone@monzo.com", [], id="mailto"),
    pytest.param("javascript:void(0)", [], id="javascript"),
    pytest.param("tel:+441234567", [], id="tel"),
    pytest.param("ftp://crawlme.monzo.com/x", [], id="other_scheme"),
    ])
def test_only_the_exact_page_hostname_survives(
    extractor: HtmlLinkExtractor, href: str, expected: list[str]
    ) -> None:
    """`goal.md:1` scopes the crawl to the page's own hostname, so the three."""
    assert extractor.extract(anchors(href), CustomURL(SEED)) == links(*expected)

@pytest.mark.parametrize(
    "href",
    [
    "https://notcrawlme.monzo.com/x",
    "https://evilmonzo.com/x",
    "https://crawlme.monzo.com.evil.test/x",
    "https://xcrawlme.monzo.com/x",
    "//notcrawlme.monzo.com/x",
    "https://sub.crawlme.monzo.com/x",
    ])
def test_a_host_that_merely_ends_with_the_page_host_is_a_different_host(
    extractor: HtmlLinkExtractor, href: str
    ) -> None:
    """A suffix or substring match would let `notcrawlme.monzo.com` and."""
    assert extractor.extract(anchors(href), CustomURL(SEED)) == []

@pytest.mark.parametrize(
    ("body", "expected"),
    [
    pytest.param(
    '<link rel="canonical" href="/canonical">', [], id="link_element"
    ),
    pytest.param('<base href="https://facebook.com/">', [], id="base_element"),
    pytest.param('<img src="/picture.png" alt="x">', [], id="image"),
    pytest.param(
    '<base href="https://facebook.com/"><a href="sibling">x</a>',
    ["https://crawlme.monzo.com/section/sibling"],
    id="a_base_href_does_not_rebase"),
    pytest.param(
    '<A HREF="/upper">x</A>',
    ["https://crawlme.monzo.com/upper"],
    id="upper_case_tag"),
    pytest.param(
    '<a href="/self-closing"/>',
    ["https://crawlme.monzo.com/self-closing"],
    id="self_closing"),
    pytest.param("<a>x</a>", [], id="anchor_without_href"),
    pytest.param("<a href>x</a>", [], id="bare_href_names_nothing"),
    pytest.param("<p>no links here</p>", [], id="no_anchors"),
    pytest.param("", [], id="empty_body"),
    ])
def test_only_anchor_hrefs_become_links(
    extractor: HtmlLinkExtractor, body: str, expected: list[str]
    ) -> None:
    """A crawler follows pages, not stylesheets, images, or a rebasing rule, so."""
    assert extractor.extract(body, CustomURL(PAGE)) == links(*expected)

@pytest.mark.parametrize(
    ("body", "expected_count"),
    [
    pytest.param(anchors("/a", "/b"), 2, id="two_links"),
    pytest.param(anchors("/a", "/a"), 1, id="one_unique_link"),
    pytest.param(anchors("https://facebook.com/x"), 0, id="none_in_scope"),
    pytest.param(anchors("mailto:a@b.test"), 0, id="no_anchor_at_all"),
    ])
def test_the_summary_is_logged_through_the_injected_logger(
    caplog: pytest.LogCaptureFixture,
    extractor: HtmlLinkExtractor,
    body: str,
    expected_count: int) -> None:
    """The per-page summary is DEBUG on the injected logger, and it reports how."""
    with caplog.at_level(logging.DEBUG, logger=LOGGER_NAME):
        extractor.extract(body, CustomURL(PAGE))
        summaries = [
            record for record in caplog.records if "same-host link" in record.getMessage()
            ]
        assert len(summaries) == 1
        assert summaries[0].levelno == logging.DEBUG
        assert summaries[0].name == LOGGER_NAME
        assert f"{expected_count} same-host link(s)" in summaries[0].getMessage()

        @pytest.mark.parametrize(
            ("body", "fragment"),
            [
            pytest.param(anchors("mailto:someone@monzo.com"), "mailto:", id="mailto"),
            pytest.param(
            anchors("ftp://crawlme.monzo.com/x"), "absolute http(s)", id="ftp"
            ),
            ])
        def test_a_rejected_href_is_logged_and_the_rest_survives(
            caplog: pytest.LogCaptureFixture,
            extractor: HtmlLinkExtractor,
            body: str,
            fragment: str) -> None:
            """One bad link must not cost a page its other links, so the rejection is."""
            with caplog.at_level(logging.DEBUG, logger=LOGGER_NAME):
                extractor.extract(body, CustomURL(PAGE))
                assert any(fragment in record.getMessage() for record in caplog.records)
                assert all(record.name == LOGGER_NAME for record in caplog.records)

                @pytest.mark.parametrize(
                    ("owner", "label"),
                    [
                    pytest.param(LinkExtractor, "port", id="port"),
                    pytest.param(HtmlLinkExtractor, "implementation", id="implementation"),
                    ])
                def test_extract_is_declared_synchronous(owner: type, label: str) -> None:
                    """Extraction is pure CPU work over a body already in memory, so the port and."""
                    assert not inspect.iscoroutinefunction(owner.extract), label
                    assert not inspect.isasyncgenfunction(owner.extract), label
                    assert "Synchronous:" in (owner.extract.__doc__ or ""), label
