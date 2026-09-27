"""Tests for CustomURL."""

from typing import cast

import pytest

from webcrawler.domain.custom_url import CustomURL, InvalidURLError

@pytest.mark.parametrize(
    ("raw", "scheme", "hostname", "path", "query"),
    [
    ("http://crawlme.monzo.com/", "http", "crawlme.monzo.com", "/", ()),
    ("https://crawlme.monzo.com/", "https", "crawlme.monzo.com", "/", ()),
    (
    "https://community.monzo.com/a/b/c.html",
    "https",
    "community.monzo.com",
                "/a/b/c.html",
            (),
    ),
    ("http://h", "http", "h", "", ()),
    ("http://h/", "http", "h", "/", ()),
    ("HTTP://Example.COM/Path", "http", "example.com", "/Path", ()),
    ("http://user:pw@h/x", "http", "h", "/x", ()),
    (
    "http://h:8080/x?b=2&a=1&b=3#frag",
    "http",
    "h",
    "/x",
    (("a", "1"), ("b", "2"), ("b", "3"))),
    ("http://h/x?a=1", "http", "h", "/x", (("a", "1"),)),
    ("http://h/x?c&a=", "http", "h", "/x", (("a", ""), ("c", ""))),
    ("http://h/x?a=hello+world", "http", "h", "/x", (("a", "hello world"),)),
    ("http://h/x?a=%C3%A9", "http", "h", "/x", (("a", "é"),)),
    ])
def test_constructor_exposes_canonical_components(
    raw: str,
    scheme: str,
    hostname: str,
    path: str,
    query: tuple[tuple[str, str],...]) -> None:
    """Every component is reachable as a property after construction."""
    url = CustomURL(raw)
    assert url.scheme == scheme
    assert url.hostname == hostname
    assert url.path == path
    assert url.query == query

@pytest.mark.parametrize(
    ("raw", "canonical"),
    [
    ("http://crawlme.monzo.com/", "http://crawlme.monzo.com/"),
    ("http://crawlme.monzo.com/#top", "http://crawlme.monzo.com/"),
    ("https://crawlme.monzo.com", "https://crawlme.monzo.com"),
    ("HTTP://Example.COM:80/Path?b=2&a=1#f", "http://example.com/Path?a=1&b=2"),
    ("http://h:80/x", "http://h/x"),
    ("http://h:8080/x", "http://h:8080/x"),
    ("http://user:pw@h/x", "http://h/x"),
    ("http://h/x?b=2&a=1&b=3", "http://h/x?a=1&b=2&b=3"),
    ("http://h/x?a=hello+world", "http://h/x?a=hello+world"),
    ("http://h/x?c&a=", "http://h/x?a=&c="),
    ("http://h/x?", "http://h/x"),
    ])
def test_get_url_returns_canonical_form(raw: str, canonical: str) -> None:
    """get_url rebuilds scheme, host, path, and sorted query, and nothing else."""
    assert CustomURL(raw).get_url() == canonical

@pytest.mark.parametrize(
    "canonical",
    [
    "http://crawlme.monzo.com/",
    "https://crawlme.monzo.com",
    "http://h/x?a=1&b=2&b=3",
    "http://h/x?a=&c=",
    ])
def test_canonical_form_is_idempotent(canonical: str) -> None:
    """Parsing a canonical URL again produces the same canonical URL."""
    url = CustomURL(canonical)
    assert url.get_url() == canonical
    assert CustomURL(url.get_url()) == url

@pytest.mark.parametrize(
    "raw",
    [
    "",
    " ",
    "\t\n",
    "not a url",
    "example.com/x",
    "//example.com/x",
    "/relative/path",
    "relative/path",
    "http://",
    "http:///path",
    "http://[::1",
    "mailto:someone@example.com",
    "javascript:void(0)",
    "ftp://example.com/x",
    "http://h:99999/x",
    "http://h:notaport/x",
    ])
def test_invalid_string_raises_invalid_url_error(raw: str) -> None:
    """A string that is not a crawlable absolute http(s) URL is rejected."""
    with pytest.raises(InvalidURLError):
        CustomURL(raw)

        @pytest.mark.parametrize(
            "raw", [123, None, 3.5, b"http://host/x", ["http://host/x"]]
            )
        def test_non_string_input_raises_invalid_url_error(raw: object) -> None:
            """Untrusted console input is rejected, never coerced to text."""
            with pytest.raises(InvalidURLError):
                CustomURL(cast(str, raw))

                @pytest.mark.parametrize("error", [InvalidURLError])
                def test_invalid_url_error_bases_only_on_value_error(
                    error: type[InvalidURLError]) -> None:
                    """The single project base is ValueError, so callers may catch either."""
                    assert issubclass(error, ValueError)
                    assert error.__bases__ == (ValueError)

                @pytest.mark.parametrize(
                    ("raw", "query"),
                    [
                    ("http://h/x?b=2&a=1&b=3", (("a", "1"), ("b", "2"), ("b", "3"))),
                    ("http://h/x?a=1&a=2&a=3", (("a", "1"), ("a", "2"), ("a", "3"))),
                    (
                    "http://h/x?z=1&y=2&y=3&b=4",
                    (("b", "4"), ("y", "2"), ("y", "3"), ("z", "1"))),
                    ("http://h/x?a=2&a=1", (("a", "2"), ("a", "1"))),
                    ("http://h/x"),
                    ])
                def test_equal_query_keys_keep_duplicates_after_sorting(
                    raw: str, query: tuple[tuple[str, str],...]
                    ) -> None:
                    """Sorting reorders by key only, so a repeated key keeps its input order."""
                    assert CustomURL(raw).query == query

                @pytest.mark.parametrize(
                    ("raw", "canonical"),
                    [
                    ("http://h/x#frag", "http://h/x"),
                    ("http://h/x#", "http://h/x"),
                    ("http://h/x?a=1#frag", "http://h/x?a=1"),
                    ("https://h/x#one#two", "https://h/x"),
                    ])
                def test_fragment_is_dropped(raw: str, canonical: str) -> None:
                    """A fragment never reaches the canonical form."""
                    assert CustomURL(raw).get_url() == canonical

                @pytest.mark.parametrize(
                    ("raw", "canonical"),
                    [
                    ("http://h:80/x", "http://h/x"),
                    ("http://h:8080/x", "http://h:8080/x"),
                    ("https://h:443/x?b=1&a=2#f", "https://h/x?a=2&b=1"),
                    ("https://h:8443/x", "https://h:8443/x"),
                    ])
                def test_a_port_is_defaulted_or_preserved(raw: str, canonical: str) -> None:
                    """A scheme-default port collapses; any other port is kept."""
                    assert CustomURL(raw).get_url() == canonical

                @pytest.mark.parametrize(
                    ("raw", "port"),
                    [
                    ("http://h:8080/x", 8080),
                    ("http://h:80/x", None),
                    ("http://h/x", None),
                    ("https://h:443/x", None),
                    ("https://h:8443/x", 8443),
                    ])
                def test_the_port_property_reports_the_kept_port(raw: str, port: int | None) -> None:
                    """The port property exposes the non-default port or None."""
                    assert CustomURL(raw).port == port

                @pytest.mark.parametrize(
                    ("left", "right"),
                    [
                    ("http://h:80/x", "http://h/x"),
                    ("http://h:80/x", "http://h:80/x"),
                    ("https://h:443/x", "https://h/x"),
                    ])
                def test_the_default_port_is_one_identity(left: str, right: str) -> None:
                    """A scheme-default port and no port are one URL."""
                    first, second = CustomURL(left), CustomURL(right)
                    assert first.get_url() == second.get_url()
                    assert first == second
                    assert hash(first) == hash(second)

                @pytest.mark.parametrize(
                    ("left", "right"),
                    [
                    ("http://h:80/x", "http://h:8080/x"),
                    ("http://h:8080/x", "http://h/x"),
                    ("http://h:8080/x", "http://h:8081/x"),
                    ])
                def test_a_non_default_port_is_its_own_identity(left: str, right: str) -> None:
                    """A non-default port selects a different server, so it never collapses."""
                    first, second = CustomURL(left), CustomURL(right)
                    assert first.get_url() != second.get_url()
                    assert first != second
                    assert hash(first) != hash(second)

                @pytest.mark.parametrize(
                    ("left", "right"),
                    [
                    ("http://h/x", "http://h/x"),
                    ("http://h:80/x", "http://h/x"),
                    ("https://h:443/x", "https://h/x"),
                    ("http://h/x#one", "http://h/x#two"),
                    ("http://h/x?b=2&a=1&b=3", "http://h/x?a=1&b=2&b=3"),
                    ("HTTP://H/x", "http://h/x"),
                    ("http://user@h/x", "http://h/x"),
                    ])
                def test_canonically_equal_urls_compare_equal(left: str, right: str) -> None:
                    """Equality is the canonical form, so a dropped part creates no second peer."""
                    assert CustomURL(left) == CustomURL(right)
                    assert not CustomURL(left) != CustomURL(right)

                @pytest.mark.parametrize(
                    ("left", "right"),
                    [
                    ("http://h/x", "https://h/x"),
                    ("http://h/x", "http://other.example/x"),
                    ("http://h/x", "http://community.monzo.com/x"),
                    ("http://h/x", "http://h/y"),
                    ("http://h/x", "http://h/x/y"),
                    ("http://h/x", "http://h"),
                    ("http://h/x", "http://h/x?a=1"),
                    ("http://h/x?a=1", "http://h/x?a=2"),
                    ("http://h/x?a=1", "http://h/x?b=1"),
                    ("http://h/x?a=1", "http://h/x?a=1&a=2"),
                    ("http://h:80/x", "http://h:8080/x"),
                    ("http://h/x", "http://h:8080/x"),
                    ])
                def test_differing_urls_compare_unequal(left: str, right: str) -> None:
                    """A difference in any canonical component breaks equality."""
                    assert CustomURL(left) != CustomURL(right)
                    assert not CustomURL(left) == CustomURL(right)

                @pytest.mark.parametrize(
                    "other", ["http://h/x", "http://h/x#frag", 42, None, ("http", "h", "/x")]
                    )
                def test_comparison_with_a_foreign_type(other: object) -> None:
                    """A foreign type never matches, even when its text is the same URL."""
                    url = CustomURL("http://h/x")
                    assert url.__eq__(other) is NotImplemented
                    assert url != other
                    assert other != url

                @pytest.mark.parametrize("raw", ["http://h/x", "https://h/x?a=1"])
                def test_dunder_methods_return_values(raw: str) -> None:
                    """__eq__ and __hash__ return real values, never an implicit None."""
                    url = CustomURL(raw)
                    assert url.__eq__(CustomURL(raw)) is True
                    assert url.__eq__(CustomURL("http://other.example/y")) is False
                    assert isinstance(url.__hash__, int)

                @pytest.mark.parametrize(
                    ("left", "right"),
                    [
                    ("http://h/x", "http://h/x"),
                    ("http://h:80/x", "http://h/x"),
                    ("http://h/x?b=2&a=1&b=3", "http://h/x?a=1&b=2&b=3"),
                    ("https://H/x", "https://h/x#frag"),
                    ])
                def test_equal_urls_hash_equal(left: str, right: str) -> None:
                    """The hash is the hash of the canonical text, so peers hash alike."""
                    url, peer = CustomURL(left), CustomURL(right)
                    assert url.__hash__ == hash(url.get_url())
                    assert hash(url) == hash(peer)

                @pytest.mark.parametrize(
                    "raws",
                    [
                    ("http://h/x", "http://h/x#frag", "http://h:80/x"),
                    (
                    "https://h/x?b=2&a=1&b=3",
                    "https://h/x?a=1&b=2&b=3",
                    "https://h:443/x?a=1&b=2&b=3#f"),
                    ])
                def test_canonical_peers_collapse_in_a_set(raws: tuple[str,...]) -> None:
                    """One canonical form is one set entry, so the URL set really dedupes."""
                    urls = [CustomURL(raw) for raw in raws]
                    assert len({*urls}) == 1
                    assert {urls[0]}.issuperset(urls)

                @pytest.mark.parametrize(
                    "raws",
                    [
                    ("http://h/x", "https://h/x", "http://h/y", "http://h/x?a=1"),
                    (
                    "http://crawlme.monzo.com/",
                    "http://community.monzo.com/",
                    "http://monzo.com/"),
                    ])
                def test_distinct_urls_stay_distinct_in_a_set(raws: tuple[str,...]) -> None:
                    """Canonicalisation does not over-collapse genuinely different URLs."""
                    assert len({CustomURL(raw) for raw in raws}) == len(raws)

                @pytest.mark.parametrize(
                    "raw", ["http://h/x", "http://h/x?a=1", "https://crawlme.monzo.com/"]
                    )
                def test_hash_is_a_usable_key_and_partition_input(raw: str) -> None:
                    """hash(url) is repeatable and indexes a dict, as the poller needs."""
                    url = CustomURL(raw)
                    assert hash(url) == hash(url) == hash(CustomURL(raw))
                    assert {url: "page"}[CustomURL(raw)] == "page"
