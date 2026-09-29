"""The URL state schema, the row mapping, and the one crawlable predicate.
"""

import sqlite3
from collections.abc import Sequence
from datetime import datetime, timezone
from typing import Final

from webcrawler.domain.crawl_state import CrawlState
from webcrawler.domain.custom_url import CustomURL

# A timestamp is TEXT in SQLite's own `CURRENT_TIMESTAMP` format, UTC, so a
# column default and an application-written value are byte-identical and order
# correctly as text.
TIMESTAMP_FORMAT: Final = "%Y-%m-%d %H:%M:%S"

URLS_TABLE: Final = "urls"

# The claim needs `UPDATE ... RETURNING`, which arrived in SQLite 3.35.
MIN_RETURNING_VERSION: Final = (3, 35, 0)

NO_LIMIT: Final = -1

# max bound parameters which can be used in sql statement
MAX_BOUND_PARAMETERS: Final = 999

# `:claimed_state`, `:now`, and `:max_items` are bound by every chunk, so only
# the remainder of the budget is available for the `:uN` URL placeholders.
CLAIM_RESERVED_PARAMETERS: Final = 3

NEXT_CRAWL_INDEX_NAME: Final = "idx_urls_state_next"

STATUS_INDEX_NAME: Final = "idx_urls_state_status"

PRIMARY_KEY_INDEX_NAME: Final = "sqlite_autoindex_urls_1"

BEGIN_IMMEDIATE_SQL: Final = "BEGIN IMMEDIATE"

COMMIT_SQL: Final = "COMMIT"

ROLLBACK_SQL: Final = "ROLLBACK"

CREATE_TABLE_SQL: Final = f"""CREATE TABLE IF NOT EXISTS {URLS_TABLE} (
    custom_url              TEXT NOT NULL PRIMARY KEY,
    created_time            TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_crawl_time         TEXT,
    next_crawl_time         TEXT DEFAULT CURRENT_TIMESTAMP,
    state                   TEXT NOT NULL DEFAULT 'not_crawled',
    last_status_update_time TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    times_crawled           INTEGER NOT NULL DEFAULT 0
)"""

# One index per predicate: an index on `custom_url` is the primary key's own, and
# each claim filters on `state` plus one time column, so each gets its own.
CREATE_NEXT_CRAWL_INDEX_SQL: Final = f"""CREATE INDEX IF NOT EXISTS {NEXT_CRAWL_INDEX_NAME}
    ON {URLS_TABLE} (state, next_crawl_time)"""

CREATE_STATUS_INDEX_SQL: Final = f"""CREATE INDEX IF NOT EXISTS {STATUS_INDEX_NAME}
    ON {URLS_TABLE} (state, last_status_update_time)"""

SCHEMA_STATEMENTS: Final[tuple[str, ...]] = (
    CREATE_TABLE_SQL,
    CREATE_NEXT_CRAWL_INDEX_SQL,
    CREATE_STATUS_INDEX_SQL,
)

INSERT_URL_SQL: Final = f"""
INSERT INTO {URLS_TABLE} (
    custom_url, created_time, next_crawl_time, state, last_status_update_time
)
VALUES (:custom_url, :created_time, :next_crawl_time, :fresh_state, :status_time)
ON CONFLICT(custom_url) DO NOTHING
"""

MARK_STARTED_SQL: Final = f"""
UPDATE {URLS_TABLE}
SET state = :started_state,
    last_crawl_time = :now,
    last_status_update_time = :now
WHERE custom_url = :custom_url
"""

COMPLETE_CRAWL_SQL: Final = f"""
UPDATE {URLS_TABLE}
SET state = :finished_state,
    next_crawl_time = :next_crawl_time,
    last_status_update_time = :now,
    times_crawled = times_crawled + 1
WHERE custom_url = :custom_url
"""


def adapt_timestamp(moment: datetime) -> str:
    """Render an instant as the stored UTC text.

    Args:
        moment: The instant to render; the port only ever passes a timezone-aware UTC value, since a naive one would be read as local time.

    Returns:
        str: The instant in `TIMESTAMP_FORMAT`, truncated to whole seconds so it stays byte-identical to SQLite's `CURRENT_TIMESTAMP`.
    """
    # No naive-value check here: that rule is the port's.
    return moment.astimezone(timezone.utc).strftime(TIMESTAMP_FORMAT)


def register_timestamp_adapter() -> None:
    """Teach `sqlite3` to store a `datetime` as `TIMESTAMP_FORMAT` UTC text.

    Idempotent, and called once at the bottom of this module: a `datetime` bound without it is stored as `str(datetime)`, whose `+00:00` offset no longer matches the column default's `CURRENT_TIMESTAMP` byte for byte.
    """
    sqlite3.register_adapter(datetime, adapt_timestamp)


def require_returning_support(version: Sequence[int] | None = None) -> None:
    """Refuse a SQLite too old for the claim query.

    The claim is an `UPDATE ... RETURNING`, added in SQLite 3.35, so an older runtime is a deployment fault reported once at `initialize` with the version found, not a syntax error inside the first claim.

    Args:
        version: The version to check, defaulting to the linked library's.

    Raises:
        sqlite3.NotSupportedError: If the version predates 3.35. A `sqlite3.Error` subclass, which is what the port documents.
    """
    current = tuple(version) if version is not None else sqlite3.sqlite_version_info
    if current[:3] < MIN_RETURNING_VERSION:
        raise sqlite3.NotSupportedError(
            f"SQLite {'.'.join(str(part) for part in current[:3])} cannot run the "
            f"claim query: UPDATE... RETURNING needs "
            f"{'.'.join(str(part) for part in MIN_RETURNING_VERSION)} or newer"
        )


def crawlable_predicate(indent: str = "") -> str:
    """Return the shared crawlability predicate.

    All four branches live in one string, so `get_crawlable_urls`, `claim_candidates`, and `claim_urls` cannot disagree about what is eligible, and the `CrawlState` values are interpolated here, so no state literal is ever spelled by hand.

    Args:
        indent: Text placed before every line but the first, so the shared text nests legibly inside the claim's subquery; it changes whitespace only, never the predicate.

    Returns:
        str: A parenthesised SQL boolean expression, to be bound against the named parameters `:now`, `:job_timeout`, and `:queue_timeout`.
    """
    # The staleness branches compare the column bare, against a cutoff the
    # parameter side computed: wrapping `last_status_update_time` in strftime()
    # would hide it from idx_urls_state_status. Timestamps are TIMESTAMP_FORMAT
    # text, so a plain <= sorts correctly, and datetime(..., 'unixepoch') emits
    # that same format.
    cutoff = "datetime(:now_epoch - :{name}_seconds, 'unixepoch')"
    body = (
        # `not_crawled` carries no time predicate, and there is no
        # `next_crawl_time IS NOT NULL` guard: `NULL <= :now` is never true, and
        # the in-flight branches never read that column, so a top-level guard
        # would make an abandoned row unreclaimable.
        f" state = '{CrawlState.NOT_CRAWLED.value}'",
        f" OR (state = '{CrawlState.FINISHED_CRAWL.value}'"
        " AND next_crawl_time <= :now)",
        f" OR (state = '{CrawlState.STARTED_CRAWL.value}'",
        f" AND last_status_update_time <= {cutoff.format(name='job_timeout')})",
        f" OR (state = '{CrawlState.QUEUED.value}'",
        f" AND last_status_update_time <= {cutoff.format(name='queue_timeout')})",
    )
    return ("\n" + indent).join(("(", *body, ")"))


def crawlable_select_statement() -> str:
    """Return the read-only select over the shared predicate.

    Returns:
        str: A `SELECT custom_url` that orders by `next_crawl_time` ascending so the limit keeps the earliest rows, bound against `:max_items`, where `-1` means no limit, plus the shared predicate's `:now`, `:job_timeout`, and `:queue_timeout`, which the caller must bind with the same encoder the claim uses. The caller maps the rows to a set, so that ordering is not observable.
    """
    return (
        f"SELECT custom_url\nFROM {URLS_TABLE}\n"
        f"WHERE {crawlable_predicate()}\n"
        "ORDER BY next_crawl_time ASC\n"
        "LIMIT :max_items"
    )


def url_parameter_names(urls: Sequence[CustomURL]) -> tuple[str, ...]:
    """Name the bound parameters of a caller's `IN (...)` restriction.

    The names carry no leading colon, because that is the form a `sqlite3` mapping is keyed by; the claim text adds the colon back.

    Args:
        urls: The caller's URLs. Non-empty; an empty collection is short-circuited by the repository before a statement is built, because `IN ()` is rejected outright by some engines.

    Returns:
        tuple[str, ...]: The names `("u0", "u1", ...)`, ready to key the bound values and to join into the claim text.
    """
    return tuple(f"u{index}" for index in range(len(urls)))


def claim_statement(url_parameters: Sequence[str] | None = None) -> str:
    """Return the atomic claim `UPDATE ... RETURNING`.

    `BEGIN IMMEDIATE` takes the write lock up front, so the subquery and the update cannot interleave with another connection's writer, and the claim carries no outer `state NOT IN (...)` guard: that guard would permanently exclude the two stale-recovery branches.

    Args:
        url_parameters: The `:uN` names of the caller's restriction, or None to claim over every row.

    Returns:
        str: The claim text, to be bound against `:claimed_state`, `:now`, `:max_items`, `:job_timeout`, `:queue_timeout`, and one parameter per URL when a restriction was given. The repository issues it between an explicit `BEGIN IMMEDIATE` and `COMMIT`.
    """
    indent = " " * 10
    # The caller's `custom_url IN (...)` restriction belongs inside the claiming
    # subquery, ahead of `ORDER BY` and `LIMIT`; in the outer `WHERE` it would
    # filter after the limit and silently under-fill the batch.
    restriction = (
        ""
        if url_parameters is None
        else f"custom_url IN ({', '.join(f':{name}' for name in url_parameters)})\n"
        f"{indent}AND "
    )
    return (
        f"UPDATE {URLS_TABLE}\n"
        "SET state = :claimed_state,\n"
        "    last_status_update_time = :now\n"
        "WHERE custom_url IN (\n"
        f"    SELECT custom_url\n    FROM {URLS_TABLE}\n"
        f"    WHERE {restriction}{crawlable_predicate(indent)}\n"
        "    ORDER BY next_crawl_time ASC\n"
        "    LIMIT :max_items\n"
        ")\n"
        "RETURNING custom_url"
    )


def row_to_custom_url(row: Sequence[str]) -> CustomURL:
    """Rebuild a `CustomURL` from a stored row.

    Every projection this module issues is the single `custom_url` column, so one string in, one value out; the URL is re-parsed rather than trusted, which keeps the stored key and the in-memory identity the same type.

    Args:
        row: One row of a `SELECT custom_url` or `RETURNING custom_url`.

    Returns:
        CustomURL: The URL the row holds, in its canonical form.
    """
    return CustomURL(row[0])


register_timestamp_adapter()
