"""A middleware that merges configured headers into every request.

Static request headers need no per-request logic, so the configured mapping is
merged verbatim into each request's headers.
"""

from collections.abc import Mapping

from webcrawler.domain.custom_url import CustomURL
from webcrawler.ports.request_middleware import RequestMiddleware

# Several sites answer a non-browser agent with 503, so the crawler presents as
# a normal Chrome on Windows.
DEFAULT_HEADERS: dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/140.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}


class HeadersMiddleware(RequestMiddleware):
    """Merge a configured header mapping into every outgoing request.

    Extends the `RequestMiddleware` port and is extended by nothing. A header
    the mapping sets overrides whatever an earlier middleware set, because the
    merge happens at this middleware's position in the sequence.

    Args:
    headers: The header names and values to set on every request, stored as a
    private copy so later mutation of the caller's mapping cannot change this
    middleware's behaviour.
    """

    def __init__(self, headers: Mapping[str, str]) -> None:
        """Record a private copy of the configured headers.

        Args:
        headers: The header names and values to set on every request.
        """
        self._headers = dict(headers)

    def apply(self, url: CustomURL, headers: dict[str, str]) -> None:
        """Merge the configured headers into the request's headers.

        Args:
        url: The URL about to be fetched; unused, because these headers
        do not vary per request.
        headers: The headers collected so far, updated in place.
        """
        headers.update(self._headers)
