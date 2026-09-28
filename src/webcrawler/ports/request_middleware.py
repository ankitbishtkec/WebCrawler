"""The request-middleware port: a composable hook on every outgoing request.

The fetcher owns the HTTP call, so auth material and headers must be added at a seam
inside it. Middlewares are applied in order to each request's headers just before it is
sent, so authentication and header policies compose without the fetcher knowing them.
"""

from abc import ABC, abstractmethod

from webcrawler.domain.custom_url import CustomURL


class RequestMiddleware(ABC):
    """One composable transformation of the headers of a single request.

    Applied in sequence, so a later one can override a header an earlier one set
    and ordering is part of the configuration. Sync on purpose: building an
    `Authorization` header is pure computation with no I/O.
    """

    @abstractmethod
    def apply(self, url: CustomURL, headers: dict[str, str]) -> None:
        """Add or override headers for the request about to fetch url.

        Args:
            url: The URL about to be fetched, so a middleware can vary headers per host or path.
            headers: The headers collected so far, mutated in place so the next middleware and the fetcher see them.

        Synchronous: pure header computation, no I/O.
        """
