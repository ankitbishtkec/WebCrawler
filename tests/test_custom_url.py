"""Tests for `CustomURL`, the canonical URL value type.

Canonical form is what collapses two spellings of one page into a single crawl
row, so the equality, hashing, and query sorting tests carry as much weight as
the parsing ones.
"""

import pytest

from webcrawler.domain.custom_url import CustomURL, InvalidURLError


@pytest.mark.parametrize(
    ("raw", "scheme", "hostname", "canonical"),
    [
        ("HTTPS://Example.COM", "https", "example.com", "https://example.com"),
        (
            "HTTPS://Example.COM/Path/to/Page.html",
            "https",
            "example.com",
            "https://example.com/Path/to/Page.html",
        ),
        ("http://Example.com/a", "http", "example.com", "http://example.com/a"),
        (
            "https://example.com:8443/a",
            "https",
            "example.com",
            "https://example.com:8443/a",
        ),
    ],
)
def test_scheme_and_host_are_canonicalised(
    raw: str, scheme: str, hostname: str, canonical: str
) -> None:
    """An uppercase scheme and host land lowercased, and a trailing path is kept.

    Args:
    raw: The URL text handed to the constructor.
    scheme: The scheme the canonical URL must report.
    hostname: The hostname the canonical URL must report.
    canonical: The exact text `get_url` must rebuild.

    Returns:
    None
    """
    url = CustomURL(raw)

    assert url.scheme == scheme
    assert url.hostname == hostname
    assert url.get_url() == canonical


@pytest.mark.parametrize(
    ("scheme", "port", "expected_port"),
    [
        ("https", 443, None),
        ("https", 8443, 8443),
        ("http", 80, None),
        ("http", 8080, 8080),
    ],
)
def test_default_port_is_dropped_and_a_non_default_port_is_kept(
    scheme: str, port: int, expected_port: int | None
) -> None:
    """A scheme-default port names the same server, so only a real port survives.

    Args:
    scheme: The URL scheme, which is what decides the default port.
    port: The port written into the raw URL.
    expected_port: The port the canonical URL must report, None when dropped.

    Returns:
    None
    """
    url = CustomURL(f"{scheme}://example.com:{port}/a")
    authority = (
        "example.com" if expected_port is None else f"example.com:{expected_port}"
    )

    assert url.port == expected_port
    assert url.get_url() == f"{scheme}://{authority}/a"


@pytest.mark.parametrize(
    "raw",
    [
        "/relative/path",
        "example.com/no-scheme",
        "ftp://example.com/file.txt",
        "",
    ],
)
def test_url_that_is_not_an_absolute_http_url_is_rejected(raw: str) -> None:
    """Only an absolute http(s) URL carrying a host is crawlable.

    Args:
    raw: The rejected text: relative, scheme-less, non-http, or empty.

    Returns:
    None
    """
    with pytest.raises(InvalidURLError):
        CustomURL(raw)


@pytest.mark.parametrize(
    ("left", "right"),
    [
        ("https://example.com/a?a=3&a=1", "https://example.com/a?a=1&a=3"),
        ("https://example.com/a?b=1&a=2", "https://example.com/a?a=2&b=1"),
    ],
)
def test_query_is_sorted_so_two_spellings_are_one_url(left: str, right: str) -> None:
    """Sorting the query pairs is the deduplication the store relies on.

    Args:
    left: One spelling of the URL, with its query pairs out of order.
    right: The same query written in sorted order.

    Returns:
    None
    """
    first = CustomURL(left)
    second = CustomURL(right)

    assert first.get_url() == second.get_url()
    assert first == second


@pytest.mark.parametrize(
    ("raw", "expected_query"),
    [
        ("https://example.com/a?a=1&a=3", (("a", "1"), ("a", "3"))),
        (
            "https://example.com/a?b=1&a=1&a=3",
            (("a", "1"), ("a", "3"), ("b", "1")),
        ),
    ],
)
def test_repeated_query_key_survives_as_a_separate_pair(
    raw: str, expected_query: tuple[tuple[str, str], ...]
) -> None:
    """A repeated key is data, not a duplicate to be collapsed.

    Args:
    raw: The URL text whose query repeats a key.
    expected_query: The pairs the canonical query must hold, in sorted order.

    Returns:
    None
    """
    assert CustomURL(raw).query == expected_query


@pytest.mark.parametrize(
    ("raw", "canonical"),
    [
        ("https://example.com/a#frag", "https://example.com/a"),
        ("https://example.com/a?b=1#frag", "https://example.com/a?b=1"),
    ],
)
def test_fragment_is_dropped(raw: str, canonical: str) -> None:
    """A fragment is a client-side view and never part of a fetched identity.

    Args:
    raw: The URL text carrying a fragment.
    canonical: The text the canonical URL must equal, without the fragment.

    Returns:
    None
    """
    url = CustomURL(raw)

    assert "#" not in url.get_url()
    assert url.get_url() == canonical


@pytest.mark.parametrize(
    ("left", "right"),
    [
        ("https://example.com/a?b=1&a=2", "https://example.com/a?a=2&b=1"),
        ("https://example.com:443/a", "https://example.com/a"),
        ("HTTPS://Example.com/a#frag", "https://example.com/a"),
    ],
)
def test_equal_urls_share_a_hash_and_collapse_in_a_set(left: str, right: str) -> None:
    """Identity is the canonical form, so a set keeps one entry per real URL.

    Args:
    left: One spelling of the URL.
    right: A different spelling the canonicalisation must absorb.

    Returns:
    None
    """
    first = CustomURL(left)
    second = CustomURL(right)

    assert first == second
    assert hash(first) == hash(second)
    assert len({first, second}) == 1


@pytest.mark.parametrize(
    ("raw", "scheme", "hostname", "port", "path", "query"),
    [
        (
            "https://example.com:8443/deep/path.html?b=2&a=1",
            "https",
            "example.com",
            8443,
            "/deep/path.html",
            (("a", "1"), ("b", "2")),
        ),
        ("http://example.com", "http", "example.com", None, "", ()),
    ],
)
def test_properties_return_what_the_url_carried(
    raw: str,
    scheme: str,
    hostname: str,
    port: int | None,
    path: str,
    query: tuple[tuple[str, str], ...],
) -> None:
    """Each component is reported as stored, so a caller need not re-parse.

    Args:
    raw: The URL text handed to the constructor.
    scheme: The scheme the `scheme` property must report.
    hostname: The hostname the `hostname` property must report.
    port: The port the `port` property must report.
    path: The path the `path` property must report.
    query: The pairs the `query` property must report.

    Returns:
    None
    """
    url = CustomURL(raw)

    assert url.scheme == scheme
    assert url.hostname == hostname
    assert url.port == port
    assert url.path == path
    assert url.query == query
