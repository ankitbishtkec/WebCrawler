"""The persisted crawl states (goal.md:23).

`goal.md:23` spells the states in UPPER_SNAKE_CASE because it writes them as
SQL literals; the column stores the lowercase `snake_case` text of the member
VALUES below, and every insert and update binds `CrawlState.<MEMBER>.value`
rather than a raw string. The two spellings therefore cannot
drift apart, because there is only one of them in the code.
"""

from enum import Enum

class CrawlState(str, Enum):
    """Lifecycle state of one URL row, stored as a lowercase string.

    Mixes in `str` so a member compares equal to the text held in the column,
    and makes that text the member VALUE rather than a second constant. An enum member cannot carry a docstring of its own, so
    the four persisted values are documented here.

        Members:
            NOT_CRAWLED: `"not_crawled"`, the column default. The only state with
            no time predicate in the crawlable query, so a fresh row is always
            eligible (goal.md:78).
            QUEUED: `"queued"`, claimed by the poller and waiting in the topic.
            `last_status_update_time` is what makes a row whose queue send was
            lost reclaimable after `queue_timeout` (goal.md:84-91).
            STARTED_CRAWL: `"started_crawl"`, a worker has marked it and is
            fetching it. The `job_timeout` branch reclaims it when that worker
            died without writing a completion (goal.md:82-83).
            FINISHED_CRAWL: `"finished_crawl"`, the attempt is over. A non-NULL
            `next_crawl_time` re-queues the row once it is due; NULL means no
            re-crawl is scheduled (goal.md:79-81).
    """

    NOT_CRAWLED = "not_crawled"
    QUEUED = "queued"
    STARTED_CRAWL = "started_crawl"
    FINISHED_CRAWL = "finished_crawl"
