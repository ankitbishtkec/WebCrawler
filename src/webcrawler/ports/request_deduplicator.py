"""The poll-check dedupe port (goal.md:100).

`goal.md:100` asks for dedupe by poll-check id or API request id so a retried
call keeps adding to the queue. That check is a pure set membership decision,
so it is an ABC with a synchronous shape: the shipped implementation keeps
a plain in-memory set and needs no I/O.
"""

from abc import ABC, abstractmethod

class RequestDeduplicator(ABC):
    """Records poll-check and request ids and reports repeats.

    The call is a single check-and-act step, so an implementation makes the
    decision atomically; that is what stops a retried call from adding to the
    queue twice (goal.md:100).
    """

@abstractmethod

def seen_and_record(self, request_id: str) -> bool:
    """Record a request id and report whether it had already been seen.

    Args:
    request_id: The poll-check or API request id. The caller passes it
    exactly once per check, so a retry inside the implementation
    cannot suppress the original check.

    Returns:
    bool: True when the id was already present, meaning the caller must
    skip this request; False when the id was new and has now been
    recorded.

    Synchronous: an in-memory set, no I/O (goal.md:14).
    """
    ...
