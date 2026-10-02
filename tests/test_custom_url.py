"""Tests for `CustomURL`, the canonical URL value type."""

import pytest

from webcrawler.domain.custom_url import CustomURL, InvalidURLError


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (
            "HTTPS://Example.COM/Path/Page.html",
            "https://example.com/Path/Page.html",
        ),
        ("http://example.com:8080/a", "http://example.com:8080/a"),
        ("https://example.com/a?b=1&a=2#frag", "https://example.com/a?a=2&b=1"),
    ],
)
def test_get_url_rebuilds_the_canonical_text(raw: str, expected: str) -> None:
    """Scheme and host are lowercased, the query is sorted, the fragment is gone."""
    assert CustomURL(raw).get_url() == expected


@pytest.mark.parametrize(
    ("left", "right"),
    [
        ("https://example.com/a?b=1&a=2", "https://example.com/a?a=2&b=1"),
        ("HTTPS://Example.com:443/a#top", "https://example.com/a"),
    ],
)
def test_two_spellings_of_one_url_are_equal_and_hash_equal(
    left: str, right: str
) -> None:
    """Identity is the canonical form, so a set keeps one entry per real page."""
    first = CustomURL(left)
    second = CustomURL(right)

    assert first == second
    assert hash(first) == hash(second)


@pytest.mark.parametrize(
    ("raw"),
    [
        ("/a/relative/page.html"),
        ("mailto:someone@example.com"),
        ("example.com/a"),
    ],
)
def test_a_string_that_is_not_an_absolute_http_url_is_rejected(raw: str) -> None:
    """A relative path, a non-http scheme, and a bare host are all uncrawlable."""
    with pytest.raises(InvalidURLError):
        CustomURL(raw)


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("https://example.com/a/b?x=1", ("https", "example.com", None, "/a/b", (("x", "1"),))),
        ("http://example.com:8080/a", ("http", "example.com", 8080, "/a", ())),
        ("http://example.com", ("http", "example.com", None, "", ())),
        ("https://example.com/", ("https", "example.com", None, "/", ())),
        ("http://example.com:80/a", ("http", "example.com", None, "/a", ())),
    ],
)
def test_the_components_are_the_canonical_ones_the_constructor_stored(
    raw: str,
    expected: tuple[str, str, int | None, str, tuple[tuple[str, str], ...]],
) -> None:
    """Each getter returns what construction parsed, not what the raw text said."""
    url = CustomURL(raw)

    assert (
        url.scheme,
        url.hostname,
        url.port,
        url.path,
        url.query,
    ) == expected


@pytest.mark.parametrize(
    "raw",
    [
        pytest.param("https://example.com/a?b=1&a=2", id="query reordered"),
        pytest.param("HTTPS://Example.com:443/a#top", id="case and default port"),
        pytest.param("http://example.com:8080/a", id="a different port"),
        pytest.param("http://example.com/a", id="a different path"),
    ],
)
def test_comparing_to_anything_but_a_url_is_not_an_equality(raw: str) -> None:
    """`NotImplemented` lets Python try the other side, so `==` is never truthy."""
    url = CustomURL(raw)

    assert (url == raw) is False
    assert (url != raw) is True


@pytest.mark.parametrize(
    "raw",
    [
        pytest.param("https://example.com/a?b=1&a=2", id="query reordered"),
        pytest.param("http://example.com:8080/a", id="a non-default port"),
        pytest.param("http://example.com/a", id="no path"),
    ],
)
def test_repr_is_the_canonical_text_so_a_failure_names_the_url(raw: str) -> None:
    """`repr` is what a pytest assertion prints, so it must be the canonical form."""
    assert repr(CustomURL(raw)) == CustomURL(raw).get_url()


@pytest.mark.parametrize(
    "raw",
    [
        pytest.param(None, id="None"),
        pytest.param(123, id="an int"),
        pytest.param(["https://example.com"], id="a list"),
        pytest.param("http://[::1/a", id="an unclosed ipv6 bracket"),
        pytest.param("http://example.com:notaport/a", id="a non-numeric port"),
    ],
)
def test_input_that_is_not_a_usable_url_is_rejected_as_one_error(raw: object) -> None:
    """Every rejection is `InvalidURLError`, so a caller catches one type."""
    with pytest.raises(InvalidURLError):
        CustomURL(raw)  # type: ignore[arg-type]
