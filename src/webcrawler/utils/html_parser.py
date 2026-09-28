"""HTML link extraction, through the standard library's parser.

The project is restricted to the standard library, so `html.parser.HTMLParser` is the
parser and this module is the only place that knows about it. It must be subclassed to be
used and it appends to a caller-owned list, which is the list `collect_hrefs` returns.
"""

from html.parser import HTMLParser

# Only an `<a href>` names a page to crawl: a stylesheet or a canonical link names a
# resource, and `<base href>` is a rebasing rule whose following would drag the crawl off-site.
ANCHOR_TAG: str = "a"
HREF_ATTRIBUTE: str = "href"


class HrefCollector(HTMLParser):
    """An HTML parser that appends each anchor's href to a caller's list.

    A parser holds the state of the document it is reading, so one collector serves one document:
    `collect_hrefs` builds a fresh collector per call instead of resetting a shared one, which keeps
    it re-entrant. The base is the stdlib parser; a regex over raw markup is not an alternative.

    Args:
        hrefs: The caller's result list. It is filled in place, so the caller
            can hand one list to the parser and read it back.
    """

    def __init__(self, hrefs: list[str]) -> None:
        """Bind the parser to the list it should fill.

        Args:
            hrefs: The list every collected href is appended to.
        """
        super().__init__()
        self._hrefs = hrefs

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        """Record an opening anchor's href, and ignore every other tag.

        Args:
            tag: The tag name, already lowercased by the parser.
            attrs: The tag's attributes in document order, each a name and a
                value, where a bare attribute such as `href` has None.
        """
        if tag != ANCHOR_TAG:
            return
        # A self-closing `<a href="..." />` needs no override of its own: the inherited
        # `handle_startendtag` delegates here, which is what the standard parser does.
        for name, value in attrs:
            if name == HREF_ATTRIBUTE and value is not None:
                self._hrefs.append(value)


def collect_hrefs(html: str) -> list[str]:
    """Return every anchor href in the markup, in document order.

    Values come back exactly as the markup carried them, the parser having unescaped them, and nothing here trims or resolves them: what a link means and whether it is in scope belongs to the link extractor.

    Args:
        html: The page body to read.

    Returns:
        list[str]: The raw `href` values in document order, duplicates
            included, since collapsing them is the caller's decision.
    """
    hrefs: list[str] = []
    collector = HrefCollector(hrefs)
    collector.feed(html)
    collector.close()
    return hrefs
