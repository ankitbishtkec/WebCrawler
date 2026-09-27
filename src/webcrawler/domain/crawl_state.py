"""The persisted crawl states (goal.md).
"""

from enum import Enum

class CrawlState(str, Enum):
    """Lifecycle state of one URL row, stored as a lowercase string.

        Members:
            NOT_CRAWLED: `"not_crawled"`, the column default. The only state with
            no time predicate in the crawlable query, so a fresh row is always
            eligible (goal.md).
            QUEUED: `"queued"`, claimed by the poller and waiting in the topic.
            `last_status_update_time` is what makes a row whose queue send was
            lost reclaimable after `queue_timeout` (goal.md).
            STARTED_CRAWL: `"started_crawl"`, a worker has marked it and is
            fetching it. The `job_timeout` branch reclaims it when that worker
            died without writing a completion (goal.md).
            FINISHED_CRAWL: `"finished_crawl"`, the attempt is over. A non-NULL
            `next_crawl_time` re-queues the row once it is due; NULL means no
            re-crawl is scheduled (goal.md).
    """

    NOT_CRAWLED = "not_crawled"
    QUEUED = "queued"
    STARTED_CRAWL = "started_crawl"
    FINISHED_CRAWL = "finished_crawl"
