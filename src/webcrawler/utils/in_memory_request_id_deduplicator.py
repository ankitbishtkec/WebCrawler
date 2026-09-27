"""The shipped `RequestDeduplicator`: a plain set of request ids.

A repeated id is never entertained again, and nothing expires — the simplest
rule that satisfies `goal.md:100` (dedupe by poll-check or request id so a
retry does not keep adding to the queue). The README's Extensions section
records what to add if a crawl runs long enough for an unbounded set to matter.
"""

from webcrawler.ports.request_deduplicator import RequestDeduplicator


class InMemoryRequestIdDeduplicator(RequestDeduplicator):
    """Records request ids in memory and reports the ones seen before.

    Extends the `RequestDeduplicator` ABC: the port is the nominal base class
    of every dedupe implementation (`goal.md:8`).

    A set with no expiry is the whole policy: `goal.md:100` asks for a repeated
    id to be skipped, never to be admitted again, and the crawl is bounded by
    the operator's own run length, so nothing here needs a clock.
    """

    def __init__(self) -> None:
        """Start with no ids recorded."""
        self._seen: set[str] = set()

    def seen_and_record(self, request_id: str) -> bool:
        """Record a request id and report whether it had already been seen.

        Args:
        request_id: The poll-check or API request id, recorded once so a
        repeated check is skipped for the rest of the run.

        Returns:
        bool: True when the id was already recorded, so the caller must
        skip this request; False when the id was new and has now been
        recorded.

        Synchronous: an in-memory set, no I/O (goal.md:14).
        """
        if request_id in self._seen:
            return True
            self._seen.add(request_id)
            return False
