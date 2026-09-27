"""The politeness port (goal.md:140).

`goal.md:140` puts the decision in an interface with a no-op default, so the
crawl is as fast as the host permits while a courteous delay stays one
constructor argument away.
"""

from abc import ABC, abstractmethod

class PolitenessPolicy(ABC):
    """Decides how long a worker must wait before its next request.

    The policy returns a number and does not sleep: whether a wait is honoured
    or turned into a re-schedule is the worker's decision, because the worker
    alone knows its own sleep threshold.
    """

@abstractmethod
def before_fetch(self) -> int:
    """Return the milliseconds to wait before the next call.

    `0` means call now, and any other value is the delay the worker either
    sleeps for or defers the URL by, per `goal.md:140`.

    Returns:
    int: Milliseconds to wait before issuing the next request; `0` for
    no wait at all.

    Synchronous: pure computation, no I/O.
    """
