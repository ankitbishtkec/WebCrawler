"""The request-middleware port: a composable hook on every outgoing request.

The fetcher owns the HTTP call, so auth material and request headers must be
added at a seam inside it rather than at every call site. A middleware is
applied, in order, to the headers of each request just before it is sent, so
authentication and header policies compose without the fetcher knowing any
of them (goal.md:141).
"""

from abc import ABC, abstractmethod

from webcrawler.domain.custom_url import CustomURL


class RequestMiddleware(ABC):
    """One composable transformation of the headers of a single request.

    Middlewares are applied in sequence, so a later one can override a header
    an earlier one set; ordering is therefore part of the configuration. The
    port stays sync on purpose — building an `Authorization` header or merging
    configured headers is pure computation with no I/O, like the politeness
    policy.
    """

@abstractmethod
def apply(self, url: CustomURL, headers: dict[str, str]) -> None:
    """Add or override headers for the request about to fetch url.

    Synchronous: pure header computation, no I/O.

    Args:
    url: The URL about to be fetched, so a middleware can vary
    headers per host or path.
    headers: The headers collected so far, mutated in place so the
    next middleware and then the fetcher see the additions.
    """
