"""The URL state store port.
"""

from abc import ABC, abstractmethod
from datetime import datetime, timedelta

from webcrawler.domain.custom_url import CustomURL


class URLStateRepository(ABC):
    """Crawl state for every URL seen, keyed by canonical form.
    """

    @abstractmethod
    async def initialize(self) -> None:
        """initializes.

        Raises:
            Exception: If the schema write cannot be completed.
        """

    @abstractmethod
    async def create_urls(self, urls: set[CustomURL]) -> None:
        """Insert the given URLs, ignoring any that already exist.

        The only path to a `not_crawled` row, and it sets `created_time` and `next_crawl_time` to the same instant, so a fresh row is immediately claimable.

        Args:
            urls: The unique canonical URLs to make known; an empty set is a no-op.

        Raises:
            Exception: If the batch write cannot be completed.
        """

    @abstractmethod
    async def close(self) -> None:
        """Free the resources on end
        """

    @abstractmethod
    async def get_crawlable_urls(
        self,
        now: datetime,
        max_items: int,
        *,
        job_timeout: timedelta,
        queue_timeout: timedelta,
    ) -> set[CustomURL]:
        """Read the rows a claim would select now, changing nothing.

        Never reserves work, so for observability and tests only: a read racing a claim may report an already-moved row. It applies the same predicate and timeouts as `claim_candidates`.

        Args:
            now: The instant the eligibility predicates are evaluated, in UTC.
            max_items: The row limit; `-1` means no limit, the default.
            job_timeout: How long a `started_crawl` row stays untouched before it is abandoned and reclaimable.
            queue_timeout: How long a `queued` row may wait before it is reported as reclaimable.

        Returns:
            set[CustomURL]: Up to `max_items` unique eligible URLs, unordered.

        Raises:
            Exception: If the read cannot be completed.
        """

    @abstractmethod
    async def claim_candidates(
        self,
        now: datetime,
        max_items: int,
        *,
        job_timeout: timedelta,
        queue_timeout: timedelta,
    ) -> set[CustomURL]:
        """Atomically move eligible rows to `queued` and return them.

        One atomic statement hands a row to at most one caller with no lock. The predicate re-targets only rows past their timeout, which also recovers a row whose write landed but whose queue send did not.

        Args:
            now: The instant the eligibility predicates are evaluated, in UTC.
            max_items: The row limit; `-1` means no limit.
            job_timeout: How long a `started_crawl` row stays untouched before it is abandoned and reclaimable.
            queue_timeout: How long a `queued` row may wait before it is reported as reclaimable.

        Returns:
            set[CustomURL]: The unique rows this call moved, unordered.

        Raises:
            Exception: If the claim update cannot be completed.
        """

    @abstractmethod
    async def claim_urls(
        self,
        urls: set[CustomURL],
        now: datetime,
        max_items: int = -1,
        *,
        job_timeout: timedelta,
        queue_timeout: timedelta,
    ) -> set[CustomURL]:
        """Atomically move eligible rows among the given URLs to `queued`.

        The URL restriction sits inside the claiming subquery and before the limit, so `max_items` bounds the result rather than the scan, and an ineligible URL returns nothing instead of displacing another row.

        Args:
            urls: The unique URLs the caller wants queued; an empty set issues no statement.
            now: The instant the eligibility predicates are evaluated, in UTC.
            max_items: The row limit; `-1` means no limit.
            job_timeout: How long a `started_crawl` row stays untouched before it is abandoned and reclaimable.
            queue_timeout: How long a `queued` row may wait before it is reported as reclaimable.

        Returns:
            set[CustomURL]: The unique claimed rows among `urls`, unordered.

        Raises:
            Exception: If the claim update cannot be completed.
        """

    @abstractmethod
    async def mark_started(self, urls: set[CustomURL], now: datetime) -> None:
        """Record that attempts on the given URLs begin now.

        Writes the attempt time, not the outcome, so the row stays claimable while the attempt runs: a worker that dies leaves a `started_crawl` row for `job_timeout` to reclaim.

        Args:
            urls: The unique URLs being crawled, all already held by the store; an empty set issues no statement.
            now: The attempt time, in UTC.

        Raises:
            Exception: If the row update cannot be completed.
        """

    @abstractmethod
    async def complete_crawl(
        self,
        finished: dict[CustomURL, datetime | None],
        discovered: set[CustomURL],
        now: datetime,
    ) -> None:
        """Record every finished row and insert the URLs they revealed.

        One transaction, so a failure rolls the whole batch back and no reader ever sees a row finished while the URLs it discovered are missing.

        Args:
            finished: Each crawled URL mapped to the instant it becomes due again, or None for no re-crawl; unique by URL, so a URL crawled twice in one batch collapses to one entry and the caller picks the winning instant. A `finished_crawl` row with no `next_crawl_time` is never selected again.
            discovered: The unique URLs found on the crawled pages, inserted when absent with `next_crawl_time` equal to their `created_time` so they are claimable at once; an existing row is left exactly as it is.
            now: The completion time, in UTC.

        Raises:
            Exception: If the batch commit cannot be completed.
        """
