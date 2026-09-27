"""Collect `<a href>` values with the standard library's HTML parser.

`goal.md:2` allows a library for HTML parsing while restricts the
project to the standard library, so `html.parser.HTMLParser` is the parser and
this module is the only place that knows about it. Only an anchor's `href` is
collected: a stylesheet or a canonical link names a resource, and a
`<base href>` is a rebasing rule rather than a link to visit, so honouring one
would drag the crawl outside the site it started on.

`HTMLParser` reports what it reads through overridable callbacks, so the wrapper
is a subclass; it appends to a caller-owned list, which is why `collect_hrefs`
returns the very object the callbacks filled.
"""

from html.parser import HTMLParser

ANCHOR_TAG: str = "a"
HREF_ATTRIBUTE: str = "href"

class HrefCollector(HTMLParser):
    """Appends the `href` of every anchor it is fed, in document order.

    A parser holds the state of the document it is reading, so one instance
    serves one document: `collect_hrefs` builds a fresh collector per call
    instead of resetting a shared one, which keeps it re-entrant.

    The base is the standard library's parser rather than a port, because the
    only alternative to overriding a callback would be a regular expression
    over raw markup, and `goal.md:2` asks for a real parser.
    """

    def __init__(self, hrefs: list[str]) -> None:
        """Bind the parser to the list its callbacks will append to.

        Args:
        hrefs: The caller's result list. It is filled in place, so the
        caller can hand one list to the parser and read it back.
        """
        super.__init__()
        self._hrefs = hrefs

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        """Record an opening anchor's href, and ignore every other tag.

        A self-closing `<a href="..." />` needs no override of its own: the
        inherited `handle_startendtag` delegates here, which is what the
        standard parser already does.

        Args:
        tag: The tag name, already lowercased by the parser.
        attrs: The tag's attributes in document order, each a name and a
        value, where a bare attribute such as `href` has None.
        """
        if tag != ANCHOR_TAG:
            return
            for name, value in attrs:
                if name == HREF_ATTRIBUTE and value is not None:
                    self._hrefs.append(value)

                    def collect_hrefs(html: str) -> list[str]:
                        """Return every anchor href in a document, in the order they appear.

                        Values come back exactly as the markup carried them: the parser has already
                        unescaped attribute values, and nothing here trims or resolves them,
                        because deciding what a link means and whether it is in scope belongs to
                        the link extractor (goal.md:142).

                        Args:
                        html: The page body to read.

                        Returns:
                        list[str]: The raw `href` values in document order, duplicates left in
                        place, since collapsing them is the caller's decision.
                        """
                        hrefs: list[str] = []
                        collector = HrefCollector(hrefs)
                        collector.feed(html)
                        collector.close()
                        return hrefs
