"""The persisted crawl states."""

from enum import Enum

class CrawlState(str, Enum):
    """Lifecycle state of one URL row, stored as a lowercase string.

    `not_crawled` is the default and the only state with no time predicate. A
    `queued` row whose send was lost is reclaimable after `queue_timeout`, and
    a `started_crawl` row whose worker died after `job_timeout`.
    """

    NOT_CRAWLED = "not_crawled"
    QUEUED = "queued"
    STARTED_CRAWL = "started_crawl"
    FINISHED_CRAWL = "finished_crawl"
