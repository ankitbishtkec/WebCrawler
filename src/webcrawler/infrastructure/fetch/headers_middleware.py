"""A middleware that merges configured headers into every request.

Static request headers — a `User-Agent` override, an `Accept` policy, a
tenant id — need no per-request logic, so the configured mapping is merged
verbatim into each request's headers.
"""

from collections.abc import Mapping

from webcrawler.domain.custom_url import CustomURL
from webcrawler.ports.request_middleware import RequestMiddleware

# Static request headers — a `User-Agent` override, an `Accept` policy, a
# tenant id — need no per-request logic.
class HeadersMiddleware(RequestMiddleware):
    """Merge a configured header mapping into every outgoing request.

    Extends the `RequestMiddleware` port and is extended by nothing. A header
    the mapping sets overrides whatever an earlier middleware set, because the
    merge happens at this middleware's position in the sequence.

    Args:
    headers: The header names and values to set on every request, stored
    as a private copy so later mutation of the mapping the caller
    passed cannot change the middleware's behaviour.
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
        headers.update(self._headers())
