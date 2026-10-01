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
