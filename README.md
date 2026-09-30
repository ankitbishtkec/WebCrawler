# WebCrawler

An async, single-process web crawler. Give it one seed URL; it crawls every
page on that exact host, keeps crawl state in SQLite, and moves work through a
topic queue. `Ctrl+C` stops it.

## Problem Statement

Given a starting URL, visit every URL found on the same domain and print each
visited URL together with the links found on that page.

The crawl is limited to **one subdomain**: seeded from
`https://crawlme.monzo.com/` it follows `crawlme.monzo.com` links, but never
`monzo.com`, `community.monzo.com`, or `facebook.com`.

The crawler must be our own implementation. Crawling frameworks such as Scrapy
or go-colly are out of scope because they hide the crawling behind someone
else's code; using a library for HTML parsing is fine. It should be written the
way a production service would be, since the interest is in the design, the
structure, the trade-offs, the observable behaviour, the concurrency, and the
tests, rather than in presentation.

## Functional Requirements

The brief, taken from its opening paragraph, asks for exactly three things.

| # | Requirement | How it is met |
|---|---|---|
| FR1 | Given a starting URL, visit each URL found on the same domain. | The seed is the only entry point; the crawl follows links outward from it and runs until interrupted. |
| FR2 | Print each URL visited, and a list of the links found on that page. | The logger writes one `INFO` line per page: the URL visited and the links extracted from it. |
| FR3 | Reject external links; the crawl is limited to one subdomain. | Scope is hostname **equality**, so a link to `monzo.com`, `community.monzo.com` or `facebook.com` is dropped and never fetched. Relative and absolute `href` values are both resolved, against the page they were found on. |

## Non-Functional Requirements

| # | Requirement |
|---|---|
| NFR1 | No crawling framework; the crawl loop, scheduler, and queue are our own. |
| NFR2 | Modules in separate folders rather than one service per module, all on a single asyncio event loop. |
| NFR3 | Composition over inheritance. |
| NFR4 | SOLID, with the database, queue, fetcher, clock, retry, politeness, and middleware behind interfaces that each ship one simple default. |
| NFR5 | Async APIs wherever the operation is I/O. |
| NFR6 | Complete signatures: arguments, return, and the exceptions a caller must handle, documented per function. |
| NFR7 | Comments only for non-obvious design decisions, concurrency invariants, race avoidance, and trade-offs, never for obvious code. |
| NFR8 | Parameterised unit tests. |
| NFR9 | Retry is owned by the I/O implementation, never by its caller, so nothing is retried twice. |
| NFR10 | All timestamps stored are UTC; a new row gets `next_crawl_time = created_time`. |
| NFR11 | Index the primary key, and `(state, next_crawl_time, last_status_update_time)` for the selection predicate. |
| NFR12 | SQLite >= 3.35, because the claim query is `UPDATE ... RETURNING`. |
| NFR13 | No re-crawl of a finished URL unless a re-crawl interval is configured. |

## High Level Design

<!-- Intentionally empty at this stage. -->

## Low Level Design

### Layout

One process, one event loop, 33 modules. `main.py` is the composition root and
holds no logic.

```
src/webcrawler/
  main.py                                  composition root: reads the seed, builds every object, runs, releases
  application/
    orchestrator.py                        seeds once, then runs the poller and the worker in one TaskGroup
    url_poller.py                          CrawlQueuer: claims store rows on a timer and bulk-feeds the queue
    worker.py                              the consumer: peek a batch, crawl it concurrently, record, commit
  domain/
    custom_url.py                          immutable canonical URL; identity is scheme+host+port+path+query
    crawl_state.py                         the four row states: not_crawled, queued, started_crawl, finished_crawl
    messages.py                            frozen queue message, plus the queue-overflow error
    base_result.py                         success/failure verdict handed back to a politeness policy
    errors.py                              the two retry verdicts: non-retryable, retryable status
    retry_settings.py                      frozen backoff/jitter/timeout knobs shared by every I/O module
  ports/
    url_state_repository.py                store interface: create, read, claim, mark started, complete, close
    crawl_queuer.py                        queueing interface: run the poll loop, enqueue URLs, queue candidates
    topic_producer.py                      queue write side: single, bulk, and dead-letter enqueue
    topic_reader.py                        queue read side: non-reserving peek, head-based commit
    web_page_fetcher.py                    fetch one page body and own the transport's lifetime
    link_extractor.py                      pull the in-scope links out of a body
    politeness_policy.py                   decide when a URL may be fetched, and learn from the outcome
    retry_policy.py                        run one fallible operation under a deadline with backoff
    time_provider.py                       the only source of "now"
    request_middleware.py                  per-request header hook
  infrastructure/
    db/sqlite_url_state_repository.py      default store: aiosqlite, explicit transactions, owns its retry
    db/models.py                           all SQL text, the shared crawlable predicate, row mapping
    fetch/aiohttp_web_page_fetcher.py      default fetcher: pooled session, status-to-error mapping
    fetch/headers_middleware.py            merges the default headers into every request
    html/html_link_extractor.py            resolves every href against its own page, keeps exact-host only
    politeness/no_op_politeness_policy.py  default policy: always reports 0 ms, never throttles
    queue/in_memory_single_topic_single_partition_queue.py   the two bounded deques behind both adapters
    queue/in_memory_topic_producer.py      TopicProducer view over that queue
    queue/in_memory_topic_reader.py        TopicReader view over that queue
    retry/exponential_backoff_retry_policy.py  default retry loop: min(base*2**n, max) + uniform(0, jitter)
    time/system_time_provider.py           returns datetime.now(timezone.utc)
  utils/
    html_parser.py                         HTMLParser subclass collecting <a href> in document order
    logger.py                              one UTC stream handler on the "webcrawler" logger
```

Dependency direction, one way only:

```
domain  <-  ports  <-  application  <-  main.py
   ^          ^            ^
   +----------+------------+-------------->  infrastructure
                                                       ^
                                                       +-->  utils
```

`domain/` imports only the standard library. Nothing under `ports/` imports an
implementation. `application/` imports no infrastructure. `utils/` imports
nothing from the project; `infrastructure/html` is what depends on it.

### Extending it

Every boundary is an `ABC`, so a replacement is a new class plus one line in
`main.py`. Nothing in `ports/` changes.

| Port | Shipped default | A new implementation must provide |
|---|---|---|
| `URLStateRepository` | `SQLiteURLStateRepository` | 7 async methods: `initialize`, `create_urls`, `close`, `get_crawlable_urls`, `claim_candidates`, `claim_urls`, `mark_started`, `complete_crawl` |
| `CrawlQueuer` | `URLPoller` | 3 async methods: `run`, `enqueue_urls`, `queue_candidates` |
| `TopicProducer` | `InMemoryTopicProducer` | 3 async methods: `enqueue`, `enqueue_many`, `enqueue_to_deadletter` |
| `TopicReader` | `InMemoryTopicReader` | 2 async methods: `peek`, `commit` |
| `WebPageFetcher` | `AiohttpWebPageFetcher` | 2 async methods: `fetch`, `close` |
| `LinkExtractor` | `HtmlLinkExtractor` | 1 sync method: `extract(html, base_url) -> set[CustomURL]` |
| `PolitenessPolicy` | `NoOpPolitenessPolicy` | 2 async methods: `before_fetch`, `record_fetch` |
| `RetryPolicy` | `ExponentialBackoffRetryPolicy` | 1 async generic method: `execute(operation)` |
| `TimeProviderFactory` | `SystemTimeProvider` | 1 sync method: `now()` |
| `RequestMiddleware` | `HeadersMiddleware` | 1 sync method: `apply(url, headers)` |

Two seams are not ports. The in-memory queue is a plain class with no
interface, so a real broker replaces it by replacing **both** queue adapters
together. And `AiohttpWebPageFetcher` takes a `session_factory`, which is how
the tests substitute a session.

The queue deliberately keeps the parameters a real broker needs, `topic`,
`consumer_group_id`, and a `partition_key` on each message, and accepts them
while ignoring them, so the in-memory default can be swapped without a caller
noticing.

### `pyproject.toml`

The single project file; there is no `setup.py`, `requirements.txt`, or
lockfile.

| Entry | Purpose |
|---|---|
| `[build-system]` | setuptools >= 68 as the build backend. |
| `[project]` | Name, version, description, and `requires-python = ">=3.11"`. |
| `[project] dependencies` | The only two runtime packages: `aiosqlite>=0.20`, `aiohttp>=3.9`. Everything else is the standard library. |
| `[project.optional-dependencies] dev` | The `dev` extra: `pytest>=8`, `pytest-asyncio>=0.24`. Installed with `-e ".[dev]"`. |
| `[tool.setuptools.packages.find] where = ["src"]` | A src layout, so the installed package is `webcrawler` and tests import it rather than the working tree. |
| `[tool.pytest.ini_options]` | `testpaths` limits collection to `tests/`; `asyncio_mode = "auto"` lets `async def` tests run without a marker; `asyncio_default_fixture_loop_scope = "function"` gives each test its own event loop. |

`sqlite-utils` is **not** a dependency. It is only a convenience for the
inspection queries below, so it is installed separately.

## How To Run It

Assumes a fresh Mac with the source only, and no Python packages installed.

**1. Check the Python version.** The floor is 3.11; a stock macOS `python3` is
often older.

```bash
python3 --version
```

If it reports less than 3.11, install a current one with Homebrew and use
`python3.12` (or newer) in the next step instead of `python3`:

```bash
brew install python@3.12
```

**2. Create a virtual environment and install the project.**

```bash
cd /path/to/WebCrawler

python3 -m venv .venv
source .venv/bin/activate

python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
```

Invoke the environment's `python` explicitly from here on. A plain `python`
outside the venv has neither `aiohttp` nor `aiosqlite`.

**3. Run it.**

```bash
python -m webcrawler.main
```

It prints a prompt and waits for one seed URL:

```
seed url>
```

Type any URL and press Enter, for example `https://crawlme.monzo.com/index.html`.
Press Enter on an empty line to use the built-in default,
`https://crawlme.monzo.com`. There is no way to change the seed once the crawl
starts. Add `--debug` for `DEBUG` logging instead of `INFO`:

```bash
python -m webcrawler.main --debug
```

Watch it work, every visited page and its links are logged:

```
2026-09-29 21:14:34 INFO webcrawler.application.worker: visited https://crawlme.monzo.com/index.html, found 10 link(s): [...]
```

Press `Ctrl+C` to stop. It exits cleanly and closes the database and session.

**4. Verify the crawl actually worked.** The state lives in `webcrawler.db` in
the project directory. `sqlite-utils` is only needed for this step:

```bash
python -m pip install sqlite-utils
```

Rows per state, with the oldest and newest timestamps and the crawl counter:

```bash
python -m sqlite_utils query webcrawler.db "select state, count(*) as n, min(times_crawled) as min_crawled, max(times_crawled) as max_crawled, min(last_crawl_time) as first_crawl, max(last_crawl_time) as last_crawl from urls group by state" --table
```

A finished crawl of the default site ends with every row in `finished_crawl` and
`max_crawled` of `1`. If `queued` or `started_crawl` still hold rows, work was
in flight when you stopped, and those rows are reclaimed by the timeouts on the
next run.

Any URL crawled more than once should return **no rows**, that is the dedupe
invariant:

```bash
python -m sqlite_utils query webcrawler.db "select custom_url, times_crawled from urls where times_crawled > 1" --table
```

And a random sample of what was stored:

```bash
python -m sqlite_utils query webcrawler.db "select custom_url, state, times_crawled, last_status_update_time from urls order by random() limit 5" --table
```

The database is also consistent after an abrupt stop:

```bash
python -m sqlite_utils query webcrawler.db "pragma integrity_check" --table
```

**5. Run the tests.**

```bash
python -m pytest
```

**6. Clean up.** Stop the crawler first if it is still running, then remove the
generated database. Nothing else is written to the project directory.

```bash
# If a crawler is still running and holding the file, Ctrl+C in its terminal
# is enough. To find a detached one instead:
lsof webcrawler.db                    # prints the PID holding the file

rm -f webcrawler.db

deactivate                            # leave the virtual environment
```

## Key Features

- **Exact-host scope, not suffix matching.** `notcrawlme.monzo.com` ends with
  `crawlme.monzo.com` and is still a different site, so scope is hostname
  equality.
- **One row per logical page.** Canonicalisation lowercases the scheme and
  host, drops the fragment and a scheme-default port, and sorts the query, so
  two spellings of the same URL are one row. The primary key is the canonical
  text itself, with no surrogate id.
- **One predicate for reading and claiming.** `get_crawlable_urls`,
  `claim_candidates`, and `claim_urls` share a single SQL predicate string, so
  they cannot disagree about what is crawlable. A row is claimable when it is
  new, when `next_crawl_time` has passed, or when it has been `queued` or
  `started_crawl` for longer than its timeout.
- **Timeouts stand in for a CDC pipeline.** If a row reaches `queued` but the
  process dies before the message is sent, no change-data-capture stream
  reconciles it. A row stuck in `queued` past `queue_timeout` is simply
  re-selected, and one stuck in `started_crawl` past `job_timeout` is treated
  the same way. Both are the same mechanism, so there is nothing to reconcile.
- **Dedupe at three levels.** `ON CONFLICT DO NOTHING` on insert, a dict of
  outcomes so one row is written once per batch, and a set of discovered links.
  A URL that is both finished and discovered in the same batch is written once,
  so `times_crawled` cannot jump.
- **Ordering that survives a crash.** `complete_crawl` runs first, then
  `enqueue_urls`, then `commit`, each step only after the one before it is
  durable, so an interrupted batch resumes instead of duplicating work.
- **Politeness defers, it never sleeps.** A non-zero wait reschedules that one
  URL as `now + wait_ms` and moves on, so one slow URL cannot stall a batch, and
  a real rate-limiting policy drops in without touching the worker.
- **Retry belongs to the I/O modules.** The store and the fetcher each hold a
  policy; the poller and the worker hold none, so nothing is retried twice. The
  two settings differ, because a locked database frees in milliseconds while a
  429 clears only on a seconds-scale window, and one shared value spent the
  whole fetch budget in two seconds. A non-retryable status fails on the first
  attempt, and a transport error or timeout backs off exponentially with jitter.
- **A spent URL is terminal, not retried forever.** A fetch that exhausts its
  attempts records a `NULL` `next_crawl_time`, which the predicate never
  selects, and the message is dead-lettered.
- **Indexes the database can actually use.** Staleness is compared against the
  bare column rather than a `strftime(...)` wrapper, and the epoch arithmetic
  happens on the bound parameter, so both composite indexes stay eligible. On
  the demo site this took the selection query from ~60 ms to ~8 ms.
- **Bulk statements that respect SQLite's limit.** Every bulk write is chunked
  to stay under the 999 bound-parameter ceiling, with each chunk binding the
  remaining limit so chunking cannot overshoot `max_items`. An empty input
  issues no statement at all, because `IN ()` is rejected outright.

## Test Strategy

<!--
  PLACEHOLDER - to be written.
  Cover: what is unit-tested vs integration-tested, the shared fakes in
  tests/support.py, how concurrency is (and is not) tested, and how the
  parameterised cases are chosen.
-->

## Opportunities

Product and architecture extensions, in rough order of value.

- **Persist page bodies.** The body is parsed for its links and discarded. A
  `PageStore` port with a file-backed implementation, plus one call in the
  worker's per-URL path, would make the crawl a collector rather than only an
  indexer.
- **A real partitioned broker.** `TopicProducer` and `TopicReader` already
  carry `topic`, `consumer_group_id`, and `partition_key` and ignore them.
  Kafka, or anything else partitioned, means two new adapters: a producer that
  routes on the partition key, and a reader that reports its assignment. A
  consumer group and rebalancing appear there and are needed the moment there
  is more than one worker.
- **More than one worker.** Everything is a single consumer today. Scaling out
  means several workers sharing the queue, which needs the partition key to be
  honoured so two workers cannot claim the same URL, the store's claim already
  makes that safe, but the queue has to support it.
- **Auth and metadata middlewares.** Headers already flow through
  `RequestMiddleware`; an auth middleware is the same shape and mutates the
  header dict in place. A token that must be refreshed is a different shape,
  because it performs I/O and the port's `apply` would have to become `async`.
- **A real politeness policy.** The shipped policy reports `0` and never
  throttles. Per-host rate limiting, or a per-domain concurrency cap, is the
  same interface, and the worker needs no change to honour it.
- **Crawl directives.** `robots.txt` and `sitemap.xml` would both narrow or
  widen what is fetched. `robots.txt` in particular belongs on the fetch path
  with a cached per-host decision, not inside the extractor.
- **Content-type aware extraction.** The extractor reads every body as HTML. A
  PDF, an image, or a JSON endpoint is fetched, parsed as markup, and yields
  nothing, deciding by `Content-Type` before extraction would avoid the work
  and record a terminal result instead.
- **Scheduling policy.** A finished URL is terminal unless a re-crawl interval
  is set, and then every URL shares one interval. Per-URL importance, or
  recency-weighted selection, belongs in the predicate's ordering.
- **Graceful drain on shutdown.** `Ctrl+C` stops promptly and the rows stay
  recoverable through the timeouts. Draining the in-flight batch before exit
  would turn a recovery into a clean finish.
