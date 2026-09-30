"""Happy-path tests for `CustomURL`, the canonical URL value type.

Canonical form is what collapses two spellings of one page into a single crawl
row, so the rebuilt text and the equality it produces are the whole contract
here. Nothing is mocked: the type is pure and takes a string.
"""

import pytest

from webcrawler.domain.custom_url import CustomURL


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
    """Scheme and host are lowercased, the query is sorted, the fragment is gone.

    Args:
    raw: The URL text handed to the constructor.
    expected: The exact text `get_url` must rebuild.

    Returns:
    None
    """
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
    """Identity is the canonical form, so a set keeps one entry per real page.

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
