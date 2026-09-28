"""Canonical URL value type.

Only the standard library is used and `urlsplit` is the sole authority on what parses; this
module adds just the crawl-specific rejections. Canonical form is
`scheme://hostname[:port] + path` plus a sorted query, lowercasing scheme and hostname only.
"""

from urllib.parse import parse_qsl, urlencode, urlsplit


class InvalidURLError(ValueError):
    """Raised when a string is not a valid crawlable URL.

    Args:
        message: The reason the input was rejected.
    """


class CustomURL:
    """Immutable URL whose identity is its canonical form.

    Stores the scheme, the hostname, the port, the path, and a sorted tuple of query pairs;
    the fragment and a scheme-default port are discarded at construction. Identity is
    therefore exactly scheme + hostname + port + path + query string.

    Args:
        raw: An absolute `http` or `https` URL string.
    """

    __slots__ = ("_hostname", "_path", "_port", "_query", "_scheme")

    def __init__(self, raw: str) -> None:
        """Parse raw into its canonical components.

        Args:
            raw: The URL text. The parser strips leading ASCII whitespace and
                embedded tabs or newlines, so an href copied out of HTML parses
                the way a browser reads it.

        Raises:
            InvalidURLError: If raw is not a string, is not an absolute
                http(s) URL carrying a hostname, or carries a port that does
                not parse. The fragment is dropped rather than rejected.
        """
        if not isinstance(raw, str):
            raise InvalidURLError(f"URL must be a string, got {type(raw).__name__}")
        try:
            parts = urlsplit(raw)
            hostname = parts.hostname
            # .port raises on a non-numeric or out-of-range port, so it must
            # be parsed inside the same try as the rest of the authority.
            port = parts.port
        except ValueError as error:
            # A malformed authority (an unclosed IPv6 bracket) is translated so
            # callers catch one error type.
            raise InvalidURLError(f"malformed URL {raw!r}: {error}") from error
        # Only http(s) is crawlable, so a mailto: or javascript: href is
        # rejected here rather than at fetch time.
        if parts.scheme not in {"http", "https"} or not hostname:
            raise InvalidURLError(
                f"URL must be an absolute http(s) URL with a host: {raw!r}"
            )
        pairs = parse_qsl(parts.query, keep_blank_values=True)
        self._scheme = parts.scheme
        self._hostname = hostname
        # A scheme-default port names the same server as no port, so only a
        # non-default one is kept: dropping it crawled http://127.0.0.1:8731/
        # as port 80 and failed, while http://h:8080/x stays its own identity.
        default_port = 443 if parts.scheme == "https" else 80
        self._port = port if port is not None and port != default_port else None
        self._path = parts.path
        # Sorted by key then value, so the same query written in a different
        # order gives the same canonical form. Sorting the pair itself is that
        # comparison, and a repeated key still survives as two pairs.
        self._query = tuple(sorted(pairs))

    @property
    def scheme(self) -> str:
        """Return the URL scheme held by the constructor.

        Returns:
            str: The lowercased scheme, always "http" or "https".
        """
        return self._scheme

    @property
    def hostname(self) -> str:
        """Return the hostname, without any userinfo or port.

        Returns:
            str: The lowercased hostname; the exact value the crawl scopes on.
        """
        return self._hostname

    @property
    def port(self) -> int | None:
        """Return the explicit port, or None for the scheme default.

        Returns:
            int | None: The non-default port the URL carries, None when the
                URL omitted one or used the scheme default.
        """
        return self._port

    @property
    def path(self) -> str:
        """Return the path exactly as the URL carried it.

        Returns:
            str: The verbatim path, empty when the URL omitted one.
        """
        return self._path

    @property
    def query(self) -> tuple[tuple[str, str], ...]:
        """Return the canonical query as sorted key/value pairs.

        Returns:
            tuple[tuple[str, str], ...]: The pairs sorted by key, with a
                duplicate key kept as a separate pair in its input order.
        """
        return self._query

    def get_url(self) -> str:
        """Rebuild the canonical URL text from the stored components.

        Returns:
            str: `scheme://hostname[:port] + path`, followed by `?` and the
                query pairs in sorted order when there is a query. The
                fragment and a scheme-default port are absent by
                construction; a non-default port is present, because it
                selects a different server.
        """
        authority = (
            f"{self._hostname}:{self._port}" if self._port is not None else self._hostname
        )
        url = f"{self._scheme}://{authority}{self._path}"
        if not self._query:
            return url
        return f"{url}?{urlencode(self._query)}"

    def __eq__(self, other: object) -> bool:
        """Compare the scheme, hostname, port, path, and query string.

        Returns:
            bool: True when the canonical components match, otherwise False,
                or NotImplemented when other is not a CustomURL, so Python can
                try the reflected comparison.
        """
        if not isinstance(other, CustomURL):
            return NotImplemented
        return (
            self._scheme == other._scheme
            and self._hostname == other._hostname
            and self._port == other._port
            and self._path == other._path
            and self._query == other._query
        )

    def __hash__(self) -> int:
        """Hash the canonical form, so it keys a set and feeds hash(url).

        Returns:
            int: The hash of `get_url`, stable within the process because
                every stored component is already canonical.
        """
        return hash(self.get_url())

    def __repr__(self) -> str:
        """Return the canonical form, so a pytest failure names the URL.

        Returns:
            str: The same text as `get_url`.
        """
        return self.get_url()
