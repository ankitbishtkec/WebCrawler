"""Errors that tell the retry policy a failure must not be retried.

`goal.md:17` requires every I/O implementation to own its retry, but retrying
everything is wrong: a site that answers 503, or 404, has given a final answer
for that request, and spending backoff on it only slows the crawl down. The
operation knows the difference, so the operation says it by raising
`NonRetryableError` and the policy re-raises it on the first attempt.
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
