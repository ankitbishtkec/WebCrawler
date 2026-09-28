# WebCrawler

An async, single-process web crawler. You enter **one seed URL** at startup; it
crawls every page on that exact host, records state in SQLite, and distributes
work through an in-memory topic queue. Exit with `Ctrl+C`.

## Running

```bash
python -m venv .venv
.venv\Scripts\python -m pip install -e ".[dev]"   # Windows (use .venv/bin/python elsewhere)
.venv\Scripts\python -m webcrawler.main            # e.g. seed: https://crawlme.monzo.com/ , then Ctrl+C to stop
.venv\Scripts\python -m pytest                    # run the test suite
```

Requires Python >= 3.11 and SQLite >= 3.35 (the claim query uses
`UPDATE ... RETURNING`). Invoke the venv's python explicitly — a plain
`python` outside the venv lacks `aiohttp`/`aiosqlite`.

To inspect the resulting database file, install `sqlite-utils`:

```bash
.venv\Scripts\python -m pip install sqlite-utils
# Schema of the urls table: column names, types, nullability, defaults, and pk.
.venv\Scripts\python -m sqlite_utils query webcrawler.db "pragma table_info(urls)" --table
# First 10 rows by oldest status update, skipping the very oldest one.
.venv\Scripts\python -m sqlite_utils query webcrawler.db "select * from urls order by last_status_update_time asc limit 10 offset 1" --table
# urls which have been crawled more than once, should have zero rows in crawlme.monzo.com hostname
.venv\Scripts\python -m sqlite_utils query webcrawler.db "select * from urls where times_crawled > 1 order by last_status_update_time asc limit 10 offset 1" --table
# Per-state summary: row counts plus the oldest/newliest status and crawl times.
.venv\Scripts\python -m sqlite_utils query webcrawler.db "select state, count(*) as n, min(last_status_update_time) as oldest_status, max(last_status_update_time) as newest_status, min(last_crawl_time) as first_crawl, max(last_crawl_time) as last_crawl, min(times_crawled), max(times_crawled) from urls group by state" --table
```

## What it does

- One seed URL, read **once** from the console at startup. There is no
  re-seeding: the seed cannot be set again. The crawl then runs until
  `Ctrl+C`.
- Exact-host crawling only: from `https://crawlme.monzo.com/` it follows
  `crawlme.monzo.com` links, never `monzo.com`, `community.monzo.com`, or
  `facebook.com`.
- Each visited page's URL and its links are logged; only the URL state is
  persisted, in SQLite.
- URL state lives in `webcrawler.db`: `not_crawled` -> `queued` ->
  `started_crawl` -> `finished_crawl`, all timestamps UTC, plus a
  `times_crawled` counter per URL (0 on insert, +1 on every
  `complete_crawl`).

## Modules in separate folders

Per the design brief, the project uses **modules in separate folders rather
than one service per module** — a single process, one asyncio event loop:

```
src/webcrawler/
    main.py                     # composition root: wiring only, no logic
    application/                # orchestrator, url_poller, worker, wiring
    domain/                     # CustomURL, CrawlState, BaseMessage, RetrySettings
    ports/                      # every interface (repository, queuer, topics, ...)
    infrastructure/             # db (sqlite), queue (in-memory), fetch (aiohttp),
                                # html, console, retry, politeness, time
    utils/                      # logger, html_parser
```

Dependency direction: `domain` <- `ports` <- `application` <- composition root;
`infrastructure` implements `ports`. Nothing under `ports/` imports an
implementation.

## The queue (simple, prod-shaped)

The queue is deliberately **simple**: one `collections.deque` for the process — no
topics, no partitions, no connected-reader registration, no reader ids. It keeps the
same parameters a real production queue would take (`topic`,
`consumer_group_id`, and the `partition_key` carried on each message), and
accepts them while ignoring them, so the in-memory implementation can be
swapped for a real one without touching a caller.

- Nothing to wire: there is no `connect` step, and either adapter may be
  constructed first.
- Capacity: configurable, default 10,000. `enqueue` past the cap
  raises `QueueOverflowError`; the bulk `enqueue_many` reports `False` for
  that message and still enqueues the rest.
- `peek(n)` returns `min(n, len)` without reserving; `commit(items)` removes
  `min(len(items), len)` from the head and is not idempotent.
- No lock: the work is CPU-bound on a single event loop and the shipped run
  path has exactly one reader.

## Why a queue timeout stands in for CDC

If the database transition to `queued` succeeds but the process dies before
the message reaches the queue, there is no CDC pipeline to reconcile the two.
The recovery is the claim predicate's `queue_timeout` branch instead: a row
stuck in `queued` whose `last_status_update_time` is older than
`queue_timeout` is re-selected and re-queued by a later poll. The same
mechanism covers a worker that dies mid-crawl via `job_timeout` on
`started_crawl` rows.

## Threads

One extra thread exists at runtime: aiosqlite's per-connection worker thread,
joined by `URLStateRepository.close()` on every exit of
`Orchestrator.run()`. The seed is read with a blocking `input()` on the main
thread before any task starts.

HTTP needs no bridge: `aiohttp` is natively async and each attempt is
bounded by `aiohttp.ClientTimeout(total=RetrySettings.timeout_seconds)`.

## Configuration

`main.py` supplies every concrete value: db path (`webcrawler.db`), batch size
(30), `job_timeout` (1 min), `queue_timeout` (30 s), the requeue delay for a
failed fetch (1 min), retry settings (3 attempts, 0.5 s base / 8 s max delay,
0.5 s jitter, 10 s timeout), the topic name, the consumer group id, and the log
level (`INFO`). The one shared retry policy is handed to the two objects that
do I/O — the SQLite store and the fetcher — and to nothing else.

The one interactive value is the seed URL, read with a blocking `input()`
before the worker and poller start; `Ctrl+C` exits at any time.

Request headers are configured as a middleware tuple in `main.py` —
`(HeadersMiddleware({"User-Agent": USER_AGENT}),)` by default, which is the
place to add more. The fetcher adds no header of its own.

## Design decisions

- One `asyncio.TaskGroup` per batch; one `complete_crawl` -> `enqueue_urls`
  -> `commit` per batch.
- Politeness: the shipped `NoOpPolitenessPolicy` reports `0`, so a crawl is
  not throttled. The worker still asks the policy before every fetch, and it
  never sleeps the answer — `0` = fetch now, any positive value = skip this
  attempt and set `next_crawl_time = now + wait_ms`, so a later claim picks the
  URL up once the wait has passed. One slow URL therefore cannot stall a whole
  batch, and a delaying policy plugs in without touching the worker (see
  Extensions).
- Retry: owned by the I/O implementations, not by their callers. The fetcher
  and the SQLite store each hold the shared policy; `fetch(url)` takes no
  policy, and the poller and the worker hold none, so nothing is retried
  twice. A batch's `commit` is never retried either: it is head-based, so a
  second attempt would remove more than the batch owns.
  `delay_n = min(base * 2**n, max) + random.uniform(0, jitter)`, each
  attempt bounded by `asyncio.wait_for(op, timeout_seconds)`. Whether a
  failure is worth retrying is the operation's call: a non-2xx status — a
  503 included — raises `NonRetryableError` and is re-raised on the first
  attempt, while a transport error or a timeout is retried.
- Poller: one `asyncio.Lock` serializes API calls against the poll loop, whose
  interval is `periodic_fetch_seconds` (1 s by default); a retried claim cannot
  grow the queue, because the row is left `queued` and the predicate re-claims
  it only once `queue_timeout` has elapsed.
- Fetched URLs are re-crawled only when `re_crawl_interval` is configured;
  otherwise `next_crawl_time` is `NULL`.
- `times_crawled` makes re-crawl loops detectable from the data alone:

```bash
.venv\Scripts\python -m sqlite_utils query webcrawler.db "select custom_url, times_crawled from urls order by times_crawled desc, custom_url limit 10" --table
```

## Extensions

Deliberately out of the shipped crawler, and where each one would plug in.

**A delaying politeness policy.** The only shipped policy is
`NoOpPolitenessPolicy`, which reports `0` and never throttles a crawl. To add
one, implement `ports/politeness_policy.py`:

```python
class MillisecondDelayPolitenessPolicy(PolitenessPolicy):
    def __init__(self, wait_ms: int) -> None:
        self._wait_ms = wait_ms

    def before_fetch(self, url: CustomURL) -> int:
        return self._wait_ms

    def record_fetch(
        self, now: datetime, url: CustomURL, result: BaseResult
    ) -> None:
        ...
```

and construct it in `main.py` instead of `NoOpPolitenessPolicy()`. The worker
needs no change: it asks the policy before every fetch and never sleeps the
answer, so a non-zero `wait_ms` records `finished_crawl` with
`next_crawl_time = now + wait_ms` and a later claim re-crawls the URL once the
wait has passed.

**Auth middlewares.** Request headers already go through
`ports/request_middleware.py`; an auth middleware is the same shape. It
mutates the headers dict the fetcher hands it:

```python
class BearerAuthMiddleware(RequestMiddleware):
    def __init__(self, token: str) -> None:
        self._value = f"Bearer {token}"

    def apply(self, url: CustomURL, headers: dict[str, str]) -> None:
        headers["Authorization"] = self._value
```

List it in `main.py`'s `request_middlewares` tuple after `HeadersMiddleware`.
A token that has to be refreshed is a different shape — it performs I/O, so
the port's `apply` would have to become `async`.

**A real prod queue.** `TopicProducer` and `TopicReader` already carry the
parameters a prod queue needs — `topic`, `consumer_group_id`, and the message
`partition_key` — and the in-memory implementation accepts and ignores them.
Swapping in Kafka, or anything else partitioned, means writing two new
adapters against those same ports: a producer that routes on
`partition_key % partition_count`, and a reader that reports which partitions
it holds. A consumer group, partition assignment and rebalancing appear there;
none of it is needed for a single-process crawl reading one queue.

**No dedupe today.** The producer and reader accept an optional `request_id` so
a networked broker could make a call idempotent, but queue operations are not
retried, so no id is ever sent twice and nothing deduplicates. A retried
head-based `commit` would remove more than the batch owns, which is why the
worker never re-issues one.

**Persisting page bodies.** Nothing writes a page to disk; the fetched body is
parsed for its links and discarded, because `goal.md` never asks for it. To
keep the pages, add a `PageStore` port, a file-backed implementation that owns
its own `RetryPolicy` (`goal.md:17` — file writes are I/O), and one call in the
worker's per-URL path between the fetch and the extraction.
