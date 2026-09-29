"""Errors that tell the retry policy how to read a failure.

Retrying everything is wrong: a site that answers 404 has given a final answer for that request, and spending
backoff on it only slows the crawl down. The operation knows the difference, so it raises `NonRetryableError` for a
final answer and `RetryableStatusError` for a transient one the policy should back off and ask again.
"""

class NonRetryableError(RuntimeError):
    """Raised by an operation whose failure must not be retried.

    A single project base, so a caller can catch either this or `RuntimeError`.

    Args:
    url: The canonical URL text whose request produced the failure.
    reason: Why the failure is final, such as the status that answered.
    """

    def __init__(self, url: str, reason: str) -> None:
        """Record the URL text and the reason the failure is final.

        Args:
        url: The canonical URL text whose request produced the failure.
        reason: Why the failure is final, such as the status that answered.
        """
        self.url = url
        self.reason = reason
        super().__init__(f"{url} will not succeed on a retry: {reason}")


class RetryableStatusError(RuntimeError):
    """Raised by an operation whose failure is a transient answer worth another attempt.

    A sibling of `NonRetryableError` and never a subclass of it, so the policy re-raises that one at once and backs off for this one.

    Args:
    url: The canonical URL text whose request produced the failure.
    reason: Why the answer is transient, such as the status that answered.
    """

    def __init__(self, url: str, reason: str) -> None:
        """Record the URL text and the reason another attempt is worth making.

        Args:
        url: The canonical URL text whose request produced the failure.
        reason: Why the answer is transient, such as the status that answered.
        """
        self.url = url
        self.reason = reason
        super().__init__(f"{url} may succeed on a retry: {reason}")
