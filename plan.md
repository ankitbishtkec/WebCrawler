# Web Crawler Implementation Plan

Authoritative requirements: `goal.md` (152 lines). User decisions and their sources: `plan_0_reviewed.md` and the answers given in review. Anything marked **[decision]** was chosen by the user, not read from `goal.md`.

## 1. Requirements from `goal.md`

### Scope and behaviour

1. Crawl a starting URL, visit every URL on the same domain, print each URL visited and the list of links found on that page (`goal.md:1`).
2. Exact-hostname scope only. From `https://crawlme.monzo.com/` the crawler follows `crawlme.monzo.com` links and rejects `facebook.com`, `monzo.com`, and `community.monzo.com` (`goal.md:1`).
3. Own implementation; no crawling frameworks such as scrapy or go-colly. Libraries for HTML parsing are allowed (`goal.md:2`).
4. Production-shaped design: structure, trade-offs, concurrency, and test coverage matter more than UI (`goal.md:3`).

### Code conventions

5. Composition over inheritance (`goal.md:8`).
6. SOLID (`goal.md:10`).
7. Every unit test uses `pytest.mark.parametrize`, including single-case behaviour (`goal.md:9`, `plan_0_reviewed.md:216`).
8. Every class and function carries a complete signature comment — args, return, raised exceptions — plus at most two lines of reasoning for the non-obvious logic (`goal.md:6-7`).
9. `goal.md:6-7` wins over `goal.md:15` for declaration comments: signatures and design rationale are always present, while inline comments are added only for non-obvious design decisions, concurrency invariants, race avoidance, and trade-offs. Obvious code gets no inline comment.
10. Modules, not services; one module per folder; documented in the README or comments (`goal.md:12`).
11. `Orchestrator` runs all modules; a single `asyncio` event loop for everything (`goal.md:13`).
12. Async APIs wherever possible (`goal.md:14`).
13. Only interface implementations are used; no bypassing an abstraction (`goal.md:16`).
14. Every implementation that performs I/O owns retry with exponential backoff, jitter, and a timeout (`goal.md:17`). This covers the fetcher and the SQLite repository — the only two I/O modules left once the page store was removed.
15. DBs, the queue, and all major modules and data types are interfaced, each with a simple default implementation: SQLite for the DB, a self-written in-memory queue (`goal.md:11`, `plan_0_reviewed.md:76`).

### URL state DB module (`goal.md:20-53`)

16. Table columns (`goal.md:23`): `custom_url` (primary key), `created_time`, `last_crawl_time`, `next_crawl_time` (default current timestamp), `state` (default `not_crawled`; values `not_crawled`, `queued`, `started_crawl`, `finished_crawl`), `last_status_update_time` (default current timestamp).
17. All times are UTC (`goal.md:25`).
18. `next_crawl_time` equals `created_time` on insert (`goal.md:27`).
19. Two indexes: on `custom_url`, and on `(state, next_crawl_time, last_status_update_time)` (`goal.md:29-31`).
20. Crawlability conditions (`goal.md:33-51`, `goal.md:76-91`): `not_crawled`; or `finished_crawl` with `next_crawl_time <= :now`; or `started_crawl` with `:now - last_status_update_time >= :job_timeout`; or `queued` with `:now - last_status_update_time >= :queue_timeout`. Ordered by `next_crawl_time ASC`, limited by `:max_items`.
21. No thread is needed for the DB; one DB interface plus an async `aiosqlite` implementation (`goal.md:53`).

### Queuer / poller module (`goal.md:55-113`)

22. Polls the URL state DB (`goal.md:56`).
23. `periodic_fetch_seconds` defaults to 5.0. The periodic poll limit `periodic_max_items` defaults to `-1`, meaning no limit; the API (b) fallback `max_items_to_queue` also defaults to `-1`, per `goal.md:59`'s stated default (`goal.md:58-59`).
24. Uses bulk search-and-update to move rows to `queued`, targeting rows that are not *freshly* `queued` or `started_crawl` — a row already in one of those states is re-selected only once `last_status_update_time` is older than the timeout — then appends only the impacted rows to the queue (`goal.md:61`).
25. The claim runs in a transaction: `BEGIN IMMEDIATE`, the `UPDATE ... RETURNING`, then `COMMIT` (`goal.md:67-98`).
26. A comment explains the CDC alternative: if the DB update succeeds but the queue send does not, the `queued` timeout branch re-claims the row (`goal.md:63-65`). The comment lives in the README (`plan_0_reviewed.md:66`).
27. Dedupe by poll-check id or API request id so retries do not grow the queue (`goal.md:100`).
28. API (a): queue a caller-supplied list of URLs now (`goal.md:103`, list form per `plan_0_reviewed.md:62`).
29. API (b): queue candidate items on demand, with `max_items`; when `None`, fall back to the constructor-supplied `max_items_to_queue`, itself defaulting to `-1` (`goal.md:104`, `goal.md:59`). The interactive console path does not use this API: seeding calls API (a) `enqueue_urls`, which queues only the URL the operator passed. API (b) stays on the interface for programmatic callers and is exercised by tests rather than by the shipped run path.
30. The queuer has an interface that both APIs appear on, plus an implementation (`goal.md:106`).
31. The poller must not run multiple instances, and an API call must not race with the poll loop (`goal.md:107`).
32. `asyncio.Lock` is not required for the state transition, because the conditional update's timeout predicates only re-target rows that are no longer freshly `queued` or `started_crawl` (`goal.md:109`).
33. Messages are partitioned by `hash(url)` (`goal.md:113`).

### Queue module (`goal.md:115-128`)

34. In-memory, built on `collections.deque`, ONE deque for the process: no topics, no partitions, no connected-reader registration, and no `reader_id` bookkeeping in this implementation (`goal.md:115`).
35. A configurable max size defaulting to 10k; enqueue past the cap raises an overflow exception — the overflow exception on enqueue is KEPT (`goal.md:116-117`).
36. A lock is a candidate, but the work is CPU-bound on a single event loop and the shipped run path has exactly one reader, so the implementation lives without a lock and says so in a comment (`goal.md:117`).
37. Two async interfaces: `TopicProducer` and `TopicReader` (`goal.md:119`).
38. The SAME params as a real prod queue are kept for future-proofing — `topic`, `consumer_group_id`, and the `partition_key` carried on each message — and this implementation ACCEPTS them and IGNORES them: one deque, no topic routing, no partition routing, no client id. A real prod queue would use them unchanged (`goal.md:118`).
39. Both adapters are wired by their constructor: there is no `connect()` step, because `goal.md` never asked for one. `TopicReader` takes a topic and a consumer group; the interface carries the group and the implementation treats it as a no-op (`goal.md:120`).
40. `TopicProducer` enqueues a message and creates the topic if absent; the `partition_key` stays on the message for a future prod implementation and is ignored here (`goal.md:121`).
41. A bulk enqueue API: one `True`/`False` result per message, `False` on overflow, and the rest are still enqueued (`goal.md:122`).
42. `TopicReader.peek(n)` returns `min(n, len)` items without reserving them; `TopicReader.commit(items)` removes `min(len(items), len)` from the head and is not idempotent (`goal.md:123-124`).

### Crawler worker module (`goal.md:130-150`)

44. `CustomURL` wraps a library URL type: a URL string is passed to the constructor, the parts are reachable through methods, the constructor raises on an invalid string, and `__eq__` compares scheme + hostname + path + query string (`goal.md:131`).
45. `get_url()` returns a standardized URL rebuilt from the parts (`goal.md:132`).
46. Concurrency through `TopicReader` objects: await peeks (`goal.md:133-134`).
47. Per URL, in order: start a transaction, set the state to `started_crawl` with the update time, commit the transaction (`goal.md:135-137`).
48. Use a task group and await completion (`goal.md:139`).
49. Use `PolitenessPolicy` with a no-op implementation to decide when to call: `0` means call now, a number means call after that many milliseconds (`goal.md:140`).
50. Call `WebPageFetcher` with an async API, passing a `RetryPolicy` with exponential backoff, jitter, logging, and a timeout (`goal.md:141`).
51. Parse the page for `href`, handling absolute and relative URLs; build a full unique URL set using the page's own hostname and drop everything pointing elsewhere (`goal.md:142`).
52. Print the extracted links (`goal.md:143`).
53. Use the implemented DB interface (`goal.md:144`).
54. Then, in one transaction: bulk-update the URL to completed with its time, and insert newly found URLs that do not exist, with `created_time` as `next_crawl_time`; commit (`goal.md:145-148`).
55. Call the queuer for those URLs to queue them immediately (`goal.md:149`).
56. Commit the messages in the queue (`goal.md:150`; granularity fixed by the batch-commit decision, see section 8).

## 2. Decisions

### Confirmed by the user

- **[decision]** DB columns, tables, Python attributes, and functions are `snake_case`; classes are `PascalCase`. `goal.md`'s `url_state` table is `urls`, its `url` column is `custom_url`, and its camelCase columns are renamed.
- **[decision]** Ports are ignored in `CustomURL` identity, because scope is the exact hostname. Trade-off: `http://h:80/x` and `http://h:8080/x` collapse to one identity.
- **[decision]** Duplicate query keys are preserved during canonicalization.
- **[decision]** Invalid URLs raise `InvalidURLError`, a `ValueError` subclass.
- **[decision]** The queuer is named `URLPoller`; the DB interface is named `URLStateRepository`.
- **[decision]** `BaseMessage` carries a partition key and a `CustomURL` payload.
- **[decision]** `enqueue_urls` and `queue_candidates` return `None` and raise when the DB claim fails. A `False` in the `enqueue_many` result is logged and not raised; the row is already `queued` with a fresh `last_status_update_time`, so the next poll skips it and only the first poll after `queue_timeout` elapses re-claims it.
- **[decision]** Batch commit: the worker peeks one batch, processes it in a task group, then calls `commit` exactly once. Producers only append at the tail, and no other consumer or peek interleaves.
- **[decision]** The reader is constructed with a `consumer_group_id`, matching a prod queue's params; in the shipped run path exactly one worker reads the topic, so reservation is unnecessary and `peek` stays non-reserving. An uncaught error leaves the batch uncommitted so it is re-read; handled outcomes commit.
- **[decision]** Retry exhaustion on a fetch failure logs the failure, sets `finished_crawl`, and sets `next_crawl_time = now + reschedule_delay`, default 1 minute, configurable by `main.py` when constructing `CrawlerWorker`. A `complete_crawl` failure is different and not recordable: the DB write that would record it is the write that failed. On a `complete_crawl` failure the worker logs, skips `enqueue_urls`, leaves the batch uncommitted, and lets the rows be reclaimed by the `job_timeout` branch.
- **[decision]** `last_status_update_time` is refreshed on every status change, including inside `complete_crawl` (`plan_0_reviewed.md:56`).
- **[decision]** Python is the language (`plan_0_reviewed.md:50`). Logging uses `logging` with a logger on every major class, `log.debug` for debug and `log.info` otherwise, configured when the orchestrator starts (`plan_0_reviewed.md:180`). The console output required by `goal.md:1` and `goal.md:143` is emitted through this logger at INFO; no `print` is used.
- **[decision]** The politeness policy returns the wait in milliseconds. The worker compares it to `sleep_threshold_ms`: at or below the threshold it sleeps and proceeds; above it, the worker does not crawl now, sets `finished_crawl`, and sets `next_crawl_time = now + timedelta(milliseconds=wait_ms)`.
- **[decision]** No interface for `Orchestrator` or `CrawlerWorker`. Every other major module is interfaced, including the queuer (`goal.md:106`, `plan_0_reviewed.md:88-90`). The value types `CustomURL`, `BaseMessage`, `CrawlState`, and `RetrySettings` are concrete domain types and are deliberately not ported, so `goal.md:11`'s "all major classes" is read as the I/O collaborators.
- **[decision]** Standard library only, plus `aiosqlite`.
- **[decision]** Stale-`queued` recovery lives inside the atomic claim query; there is no separate `claim_stale_queued` method. This is a plan choice: `plan_0_reviewed.md:130` asked about it and left it unanswered.
- **[decision]** The single process owns the whole crawler, so SQLite is opened by one process only.
- **[decision]** Reconstructed URLs omit the fragment but keep scheme, hostname, path, and query (`plan_0_reviewed.md:95`).
- **[decision]** The no-lock rationale is documented in the implementation file (`plan_0_reviewed.md:72`).
- **[decision]** The orchestrator seeds ONE URL, exactly once, then runs the whole crawler until `Ctrl+C`. There is no re-seeding and no second seed prompt; this supersedes the earlier loop-on-console-input decision.

### Derived from `goal.md`, needing no judgement

- `next_crawl_time IS NULL` means "no re-crawl scheduled". A successfully crawled row gets `next_crawl_time = NULL` unless a re-crawl interval is configured, in which case it gets `now + interval`. This is what makes the `finished_crawl` branch at `goal.md:79-81` reachable without contradicting the `NULL` default.
- The outer claim query carries no `state NOT IN (...)` guard, because `goal.md:69-96` has none and adding one would permanently exclude the stale-recovery branches at `goal.md:84-91`. The subquery's timeout predicates are the guard `goal.md:109` describes: a row already `queued` or `started_crawl` is re-selected only once `last_status_update_time` is older than the timeout.

### Design choices recorded; concrete values come from `main.py`

- `job_timeout` and `queue_timeout` are constructor-configurable `timedelta` values. The repository converts each with `int(td.total_seconds())` when binding `:job_timeout` and `:queue_timeout`. `goal.md` passes them as parameters; a fixed value is the alternative.
- The re-crawl interval for `finished_crawl` rows is a `CrawlerWorker` constructor parameter defaulting to `None`, meaning no re-crawl. The alternative is a fixed default interval.
- HTTP fetching uses `aiohttp`, a natively async client, so the event loop is never blocked and no thread is needed for I/O. Two threads exist at runtime: the console-reader bridge thread (next bullet) and aiosqlite's per-connection worker thread. The HTTP attempt is bounded in two places — `aiohttp.ClientTimeout(total=RetrySettings.timeout_seconds)` bounds the socket, and the retry policy's per-attempt `asyncio.wait_for(operation(), timeout_seconds)` bounds the await. The aiosqlite thread is joined by `URLStateRepository.close()`. `goal.md:53`'s "no thread needed" is scoped to the DB's own concurrency, not to these two.
- Console input is one blocking `input()` in `main.py`, taken before any task exists, so the event loop is never blocked and the seed can never be set twice (`goal.md:13`). `main.py` prints that `Ctrl+C` exits at any time, and passes the stripped line to the orchestrator, which validates it as a single `CustomURL`.
- The operator-facing seeding path is API (a) `enqueue_urls`, which queues only the URLs passed to it. API (b) `queue_candidates` remains on the interface for `goal.md:104` but is not called in the shipped run path.

None of the above is an open question; only the concrete values listed in steps 4, 7, 10, and 11 remain for `main.py` to supply. They are collected as a single list in step 10.

## 3. Interfaces

```python
# ports/url_state_repository.py


class URLStateRepository(ABC):
    @abstractmethod
    async def initialize(self) -> None:
        """Create the table and indexes if absent. Raises sqlite3.Error."""

    @abstractmethod
    async def create_urls(self, urls: list[CustomURL]) -> None:
        """Insert urls if absent. Sets created_time = next_crawl_time = now (UTC)."""

    @abstractmethod
    async def close(self) -> None:
        """Close the connection and join its worker thread. Idempotent.

        aiosqlite runs each connection on a non-daemon thread that blocks
        until the connection closes, and interpreter shutdown joins
        non-daemon threads before finalization. Without this, `Ctrl+C` and
        pytest session teardown both hang.
        """

    @abstractmethod
    async def get_crawlable_urls(
        self,
        now: datetime,
        max_items: int,
        *,
        job_timeout: timedelta,
        queue_timeout: timedelta,
    ) -> list[CustomURL]:
        """Read-only SELECT of goal.md:33-51. No state change.

        The two timeouts are required, not optional: goal.md:33-51 selects
        STALE `started_crawl`/`queued` rows, so binding a zero timeout would
        make those branches vacuously true and list every in-flight row as
        crawlable. Sharing them with the claim methods keeps one predicate.

        Observability and tests only; the poller uses the claim methods, so
        this is not the production handoff.
        """

    @abstractmethod
    async def claim_candidates(
        self,
        now: datetime,
        max_items: int,
        *,
        job_timeout: timedelta,
        queue_timeout: timedelta,
    ) -> list[CustomURL]:
        """Atomic BEGIN IMMEDIATE + UPDATE ... RETURNING (goal.md:67-98).

        Returns only the rows this caller actually moved to 'queued'.
        """

    @abstractmethod
    async def claim_urls(
        self, urls: list[CustomURL], now: datetime, max_items: int = -1, *,
        job_timeout: timedelta, queue_timeout: timedelta,
    ) -> list[CustomURL]:
        """As claim_candidates, plus AND custom_url IN (...) (goal.md:92).

        Backs API (a): only the caller's urls are considered. max_items
        defaults to -1 because the caller's list is already the batch.
        """

    @abstractmethod
    async def mark_started(self, url: CustomURL, now: datetime) -> None:
        """One transaction: state='started_crawl', last_crawl_time=now,
        last_status_update_time=now.
        """

    @abstractmethod
    async def complete_crawl(
        self,
        finished: list[tuple[CustomURL, datetime | None]],
        discovered: list[CustomURL],
        now: datetime,
    ) -> None:
        """One bulk transaction (goal.md:145-148).

        finished: each pair is (url, next_crawl_time). NULL means no re-crawl.
        The tuple carries per-row scheduling because a batch can mix success
        (NULL), retry exhaustion (now + reschedule_delay), and a politeness
        skip (now + wait_ms). Every row in this call also gets
        last_status_update_time = now (plan_0_reviewed.md:56).
        discovered: inserted when absent with created_time = next_crawl_time.
        """
```

The crawlable predicate is built once, in `infrastructure/db/models.py`, and shared by `get_crawlable_urls`, `claim_candidates`, and `claim_urls` so the encodings cannot drift. `claim_urls` reuses it and adds the `custom_url IN (...)` restriction from `goal.md:92`.

```python
# ports/crawl_queuer.py


class CrawlQueuer(ABC):
    @abstractmethod
    async def enqueue_urls(
        self, urls: list[CustomURL], request_id: str | None = None
    ) -> None:
        """Queue the given urls now (goal.md:103). Returns None; raises.

        request_id is deduplicated (goal.md:100). Bulk enqueue itself does
        not dedupe; the caller owns that (plan_0_reviewed.md:64).
        """

    @abstractmethod
    async def queue_candidates(
        self,
        now: datetime,
        max_items: int | None = None,
        request_id: str | None = None,
    ) -> None:
        """Queue candidate rows on demand (goal.md:104).

        max_items=None falls back to the constructor-supplied
        max_items_to_queue, which defaults to -1 (goal.md:59).
        request_id is deduplicated (goal.md:100).
        """
```

```python
# ports/topic_producer.py, ports/topic_reader.py


class TopicProducer(ABC):
    # Constructed with (topic, queue, logger); the queue must be the same
    # instance the reader reads from, and the constructor creates the topic,
    # so there is no connect step. There is no consumer_group_id because
    # only readers join a group.
    @abstractmethod
    async def enqueue(self, message: BaseMessage) -> None:
        """Append the message to the topic's single deque (goal.md:121).

        The partition_key stays on the message for a future prod queue and
        is ignored here. Raises QueueOverflowError past the deque max size.
        The poller uses enqueue_many, so this single-message API is for
        tests and callers outside the crawl loop.
        """

    @abstractmethod
    async def enqueue_many(self, messages: list[BaseMessage]) -> list[bool]:
        """Bulk enqueue (goal.md:122). One result per input message.

        No dedupe: the caller owns that (plan_0_reviewed.md:64). False on
        overflow for that message while the rest are still enqueued.
        """


class TopicReader(ABC):
    # Constructed with (topic, consumer_group_id, queue, logger); the params
    # match a real prod queue for future-proofing (goal.md:118). The
    # constructor creates the topic, so there is no connect step;
    # consumer_group_id is a no-op here, and no reader_id is kept — one deque
    # per topic, no connected-reader list. The README's Extensions section
    # covers what a prod queue would use these same ports for.
    @abstractmethod
    async def peek(self, n: int) -> list[BaseMessage]:
        """Return min(n, len(topic)) items without removing them."""

    @abstractmethod
    async def commit(self, items: list[BaseMessage]) -> None:
        """Remove min(len(items), len(topic)) items from the head.

        Head-based and non-idempotent. Safe only because the shipped run
        path has exactly one reader per topic.
        """
```

```python
# ports/politeness_policy.py, ports/retry_policy.py, ports/web_page_fetcher.py,
# ports/link_extractor.py


class PolitenessPolicy(ABC):
    @abstractmethod
    def before_fetch(self) -> int:
        """Return ms to wait before the next call (goal.md:140).

        0 means call now. Synchronous: pure computation, no I/O.
        """


class RetryPolicy(ABC):
    @abstractmethod
    async def execute(self, operation: Callable[[], Awaitable[T]]) -> T:
        """Run operation with timeout, exponential backoff, and jitter.

        Raises the last error after attempts are exhausted.
        """


class WebPageFetcher(ABC):
    @abstractmethod
    async def fetch(self, url: CustomURL, retry_policy: RetryPolicy) -> str:
        """Return the response body. Raises on transport or status failure."""


class RequestMiddleware(ABC):
    @abstractmethod
    def apply(self, url: CustomURL, headers: dict[str, str]) -> None:
        """Add or override headers for the request about to fetch url.

        Applied in order, so a later middleware overrides an earlier one.
        Synchronous: pure header computation, no I/O, like the politeness
        policy. Implemented by `HeadersMiddleware`, `BasicAuthMiddleware`,
        and `BearerAuthMiddleware`; the fetcher applies the configured
        sequence to a fresh headers dict on every attempt.
        """



class LinkExtractor(ABC):
    @abstractmethod
    def extract(self, html: str, base_url: CustomURL) -> list[CustomURL]:
        """Return unique same-host links, resolved against base_url.

        Synchronous: pure CPU parsing, no I/O.
        """
```

```python
# application/url_poller.py


class URLPoller(CrawlQueuer):
    def __init__(
        self,
        repository: URLStateRepository,
        producer: TopicProducer,
        *,
        periodic_fetch_seconds: float = 5.0,
        periodic_max_items: int = -1,
        max_items_to_queue: int = -1,  # goal.md:59 default: no limit
        job_timeout: timedelta,
        queue_timeout: timedelta,
        dedupe: RequestDeduplicator,
        time_provider: TimeProviderFactory,
        logger: logging.Logger,
    ) -> None: ...

    async def run(self) -> None:
        """Long-running loop (goal.md:56-59).

        Every 5 seconds, compute `now = self._time_provider.now()`, mint one
        poll-check id, and call `_claim_and_enqueue` with that id, `now`,
        `urls=None`, and `max_items=periodic_max_items`. The id is passed to
        `seen_and_record` exactly once, immediately before the claim, and
        repository-internal retries never re-enter the dedupe gate, so a
        retried check cannot grow the queue (goal.md:100). Queue growth is
        prevented by the single `enqueue_many` issued for the rows returned
        by the one successful claim. A fresh id is minted per check.

        This method never calls the public APIs; it calls the private
        `_claim_and_enqueue`, which assumes the lock is already held. The
        non-re-entrancy rationale is in steps 9 and section 8.
        """

    async def _claim_and_enqueue(
        self,
        request_id: str | None,
        now: datetime,
        *,
        urls: list[CustomURL] | None = None,
        max_items: int = -1,
    ) -> None:
        """Dedupe, claim, enqueue claimed rows. Caller must hold the lock.

        `urls` given -> `repository.claim_urls(urls, now, max_items, ...)`;
        `urls` None -> `repository.claim_candidates(now, max_items, ...)`.
        `max_items` is already resolved by the caller, which is why this
        takes a plain `int` and not `int | None`: `run()` passes
        `periodic_max_items`, `queue_candidates` passes
        `max_items_to_queue` when its own `max_items` is None, and
        `enqueue_urls` passes -1.

        A `request_id` of None is replaced here with `uuid4().hex`, so every
        call reaches `seen_and_record` with a real id (goal.md:100). Only
        `run()` supplies a stable id, one per poll check.
        """

    async def enqueue_urls(
        self, urls: list[CustomURL], request_id: str | None = None
    ) -> None:
        """Claim the caller's URLs, then enqueue only the claimed rows.

        The URLs are already in the DB, inserted by the worker or the
        orchestrator (plan_0_reviewed.md:62), so this only moves candidates
        to 'queued'. Computes `now = self._time_provider.now()` and
        delegates to `_claim_and_enqueue(request_id, now, urls=urls,
        max_items=-1)`, which mints a `uuid4().hex` id when `request_id`
        is None.
        """

    async def queue_candidates(
        self,
        now: datetime,
        max_items: int | None = None,
        request_id: str | None = None,
    ) -> None:
        """Claim up to max_items candidates, then enqueue only those rows.

        Resolves `max_items` to `max_items_to_queue` when None, then
        delegates to `_claim_and_enqueue(request_id, now, urls=None,
        max_items=<resolved>)`, which mints a `uuid4().hex` id when
        `request_id` is None.
        """
```

```python
# application/worker.py


class CrawlerWorker:
    def __init__(
        self,
        repository: URLStateRepository,
        reader: TopicReader,
        fetcher: WebPageFetcher,
        link_extractor: LinkExtractor,
        politeness_policy: PolitenessPolicy,
        queuer: CrawlQueuer,
        retry_policy: RetryPolicy,
        *,
        batch_size: int,  # reader.peek(self._batch_size) per iteration
        idle_sleep_seconds: float = 1.0,
        reschedule_delay: timedelta = timedelta(minutes=1),
        sleep_threshold_ms: int = 2_000,
        re_crawl_interval: timedelta | None = None,
        time_provider: TimeProviderFactory,
        logger: logging.Logger,
    ) -> None: ...

    async def run(self) -> None: ...
```

```python
# application/orchestrator.py


class Orchestrator:
    def __init__(
        self,
        repository: URLStateRepository,
        poller: URLPoller,
        worker: CrawlerWorker,
        seed_line: str,
        logger: logging.Logger,
    ) -> None: ...

    async def run(self) -> None:
        """Own the single poller and worker instances (goal.md:13).

        The seed was read once, before this method was called: `main.py`
        blocks on a single `input()` and passes the line in, so the event
        loop is never blocked here and no second seed can be set. The line
        is validated as a single `CustomURL`. A valid seed is
        inserted with `repository.create_urls([seed])` and then queued with
        `poller.enqueue_urls([seed])` — the insert must precede the enqueue,
        because the queuer claims rows that already exist
        (plan_0_reviewed.md:62). The line is never split on commas and the
        seed can never be set again.

        Only after the seed is queued are the poller and worker tasks
        created — `poller_task = asyncio.create_task(self._poller.run())`
        and `worker_task = asyncio.create_task(self._worker.run())` — so
        exactly one poller task can exist (goal.md:107) and no task runs
        when there is nothing to crawl. The crawl then runs until `Ctrl+C`:
        the await on both tasks is wrapped in `try/finally` that cancels and
        awaits both tasks, then awaits `repository.close()`, so a `Ctrl+C`
        can never leave a task or a connection thread running.

        An invalid seed, or an exception raised by `create_urls` or
        `enqueue_urls` (an exhausted DB retry), is logged at INFO rather
        than raised, and `run()` shuts down cleanly — with no seed there is
        nothing to crawl, so the session cannot continue. main.py supplies
        reschedule_delay when the worker is built.
        """
```

## 4. Domain types and ports

```python
class CrawlState(str, Enum):
    NOT_CRAWLED = "not_crawled"
    QUEUED = "queued"
    STARTED_CRAWL = "started_crawl"
    FINISHED_CRAWL = "finished_crawl"


class InvalidURLError(ValueError):
    """Raised when a string is not a valid crawlable URL."""


class QueueOverflowError(RuntimeError):
    """Raised when an enqueue would exceed the deque max size."""


class CustomURL:
    def __init__(self, raw: str) -> None:
        """Parse raw. Raises InvalidURLError.

        Stores scheme, hostname, path, and query as a sorted tuple of pairs
        with duplicate keys preserved. Fragment and a scheme-default port
        are dropped; a non-default port is preserved, because it selects a
        different server — dropping it crawled `http://127.0.0.1:8731/` as
        port 80, which the end-to-end smoke run caught.
        """

    @property
    def scheme(self) -> str: ...

    @property
    def hostname(self) -> str: ...

    @property
    def path(self) -> str: ...

    @property
    def query(self) -> tuple[tuple[str, str], ...]: ...

    def get_url(self) -> str:
        """Rebuild scheme://hostname + path + sorted query. No fragment.

        Returns:
            str: The canonical URL text.
        """
        ...

    def __eq__(self, other: object) -> bool:
        """Compare scheme, hostname, path, and query string."""
        ...

    def __hash__(self) -> int:
        """Hash the canonical form so it can key a set and feed hash(url)."""
        ...


class BaseMessage:
    def __init__(self, url: CustomURL, partition_key: int) -> None:
        """partition_key is stored verbatim, so it equals hash(url) (goal.md:113)."""


class RetrySettings:
    max_attempts: int
    base_delay_seconds: float
    max_delay_seconds: float
    jitter_seconds: float
    timeout_seconds: float


class TimeProviderFactory(ABC):
    """Test seam for time. Not a runtime extension point."""

    @abstractmethod
    def now(self) -> datetime:
        """Synchronous: reads the system clock only, no I/O (goal.md:14)."""
        ...


class RequestDeduplicator(ABC):
    """Dedupe by poll-check id or API request id (goal.md:100)."""

    @abstractmethod
    def seen_and_record(self, request_id: str) -> bool:
        """Record request_id; return True if already present, so skip.

        Synchronous: an in-memory set with a TTL, no I/O (goal.md:14).
        """
```

### Schema

`goal.md` writes status literals in uppercase; the persisted values are the lowercase `snake_case` forms in `CrawlState`. Every insert and update uses `CrawlState`, never the uppercase spelling. The SQL blocks below are illustrative and show the literals for readability; the shared predicate builder in `models.py` interpolates `CrawlState.X.value`, and each `SET` clause binds `CrawlState.QUEUED.value` rather than a raw string.

```sql
CREATE TABLE IF NOT EXISTS urls (
    custom_url             TEXT NOT NULL PRIMARY KEY,
    created_time           TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_crawl_time        TEXT,
    next_crawl_time        TEXT DEFAULT CURRENT_TIMESTAMP,
    state                  TEXT NOT NULL DEFAULT 'not_crawled',
    last_status_update_time TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    times_crawled INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_urls_state_times
    ON urls (state, next_crawl_time, last_status_update_time);
```

- `goal.md:30` asks for an index on the URL column; `custom_url TEXT PRIMARY KEY` already provides one, so no separate index is declared for it. The test asserts `PRAGMA index_list('urls')` reports an index over `custom_url`.

- `last_crawl_time` has no default and stays `NULL` until a crawl starts. It is written in exactly one place, `mark_started`, recording the attempt time. The claim query never touches it, and `complete_crawl` does not.
- Timestamps are stored as TEXT in the fixed format `%Y-%m-%d %H:%M:%S` UTC, written through a registered `sqlite3` adapter. The format is byte-identical to SQLite's `CURRENT_TIMESTAMP`, so default and application-written values compare correctly as strings and no locale or microsecond suffix can leak in.
- `next_crawl_time` is nullable so "no re-crawl scheduled" is representable. Inserts always set it equal to `created_time` (`goal.md:27`), overriding the `DEFAULT`, so the equality is guaranteed by the application rather than by SQLite's clock. Because inserts always populate it, a `not_crawled` row never has a `NULL` value.
- `times_crawled` is the per-URL crawl counter, `0` on insert and incremented by `complete_crawl` for every finished row in the call, so a re-crawl or a failure-reschedule is visible in the row and a looping crawl is detectable with one query (`ORDER BY times_crawled DESC`). `CREATE TABLE IF NOT EXISTS` cannot extend an existing table, so `initialize` reads `PRAGMA table_info(urls)` and runs a guarded `ALTER TABLE ... ADD COLUMN times_crawled INTEGER NOT NULL DEFAULT 0` when the column is missing, logging the migration at INFO.
- SQLite's `CURRENT_TIMESTAMP` is UTC (`goal.md:25`), matching the `TimeProviderFactory`.

### Claim query

The subquery reproduces `goal.md:76-91`. Timeout comparisons are done in epoch seconds via `strftime('%s', ...)`, because SQLite's `-` operator coerces a TEXT timestamp to its longest numeric prefix (`'2026-09-26 11:28:00'` becomes `2026`), which would silently break both timeout branches. `:job_timeout` and `:queue_timeout` are epoch-second integers bound from constructor values.

```sql
BEGIN IMMEDIATE;

UPDATE urls
SET state = 'queued',
    last_status_update_time = :now
WHERE custom_url IN (
    SELECT custom_url
    FROM urls
    WHERE (state = 'not_crawled')
       OR (state = 'finished_crawl' AND next_crawl_time <= :now)
       OR (state = 'started_crawl'
           AND CAST(strftime('%s', :now) AS INTEGER)
             - CAST(strftime('%s', last_status_update_time) AS INTEGER)
             >= :job_timeout)
       OR (state = 'queued'
           AND CAST(strftime('%s', :now) AS INTEGER)
             - CAST(strftime('%s', last_status_update_time) AS INTEGER)
             >= :queue_timeout)
    ORDER BY next_crawl_time ASC
    LIMIT :max_items
)
RETURNING custom_url;

COMMIT;
```

- No `next_crawl_time IS NOT NULL` guard is used. `NULL <= :now` is never true, so a `NULL` (no re-crawl) row cannot satisfy the `finished_crawl` branch on its own; the `not_crawled` branch has no time predicate but inserts always populate the column; and the `started_crawl` and `queued` branches do not reference `next_crawl_time` at all, so a top-level guard would have made those rows unreclaimable.
- `not_crawled` carries no time predicate, matching `goal.md:78` exactly.

`claim_urls` reuses the same subquery with the caller's URL restriction placed **inside** the subquery, before `ORDER BY` and `LIMIT`, exactly as `goal.md:92` does. Placing it in the outer `WHERE` instead would filter after `LIMIT :max_items` and silently under-fill the batch. Placeholders are generated as `IN (:u0, :u1, ...)` from the caller's list, chunked to stay under SQLite's bound-parameter limit. All chunks execute inside the single `BEGIN IMMEDIATE` … `COMMIT`, so the claim stays atomic. Each chunk binds `LIMIT` to `-1` when `max_items` is `-1`, and otherwise to `max_items - len(rows_collected_so_far)`, so the total returned never exceeds `max_items`; the loop also stops when a chunk returns no rows. `claim_urls` short-circuits to `[]` without issuing any statement when `urls` is empty, avoiding a pointless round trip on the common no-links case and staying portable to engines that reject `IN ()`.

```sql
    SELECT custom_url
    FROM urls
    WHERE custom_url IN (:u0, :u1, :u2, ...)
      AND ( (state = 'not_crawled')
         OR (state = 'finished_crawl' AND next_crawl_time <= :now)
         OR (state = 'started_crawl' AND <job_timeout predicate>)
         OR (state = 'queued' AND <queue_timeout predicate>) )
    ORDER BY next_crawl_time ASC
    LIMIT :max_items
```

- `RETURNING` yields only the rows this caller moved to `queued`, which is exactly the "find the impacted rows and append only those" rule from `goal.md:61`.
- `BEGIN IMMEDIATE` takes the write lock up front, so the select-then-update sequence cannot interleave with another writer.
- No outer `state NOT IN (...)` guard: `goal.md:69-96` has none, and it would permanently exclude the stale-recovery branches. The subquery's timeout predicates are the guard `goal.md:109` describes.
- The `queued` timeout branch is the CDC fallback for "DB update succeeded, queue send did not" (`goal.md:63-65`).
- The connection is opened with `isolation_level=None` (autocommit) so these explicit `BEGIN IMMEDIATE` / `COMMIT` statements are not wrapped by the driver's own implicit transaction, which would raise "cannot start a transaction within a transaction". Every claim rolls back on exception.
- A claim calls `fetchall()` on the `RETURNING` cursor and only then issues `COMMIT`. SQLite discards the effects of a `RETURNING` statement that is not stepped to completion, so committing before draining the cursor would roll the claim back and lose the update.
- `strftime('%s', x)` returns `NULL` for a non-conforming timestamp, which would silently skip a row. The only two sources of timestamp text are the registered adapter and SQLite's `CURRENT_TIMESTAMP`; the claim query and `mark_started` write through the adapter, and a test asserts every stored value round-trips through `strftime('%s', ...)`.

## 5. Module layout

```text
src/webcrawler/
    __init__.py
    main.py                        # async console entry point, main() under __main__ guard
    application/
        __init__.py
        orchestrator.py            # Orchestrator
        worker.py                  # CrawlerWorker
        url_poller.py              # URLPoller
    domain/
        __init__.py
        custom_url.py              # CustomURL, InvalidURLError
        crawl_state.py             # CrawlState
        messages.py                # BaseMessage, QueueOverflowError
        retry_settings.py          # RetrySettings
    ports/
        __init__.py
        url_state_repository.py    # URLStateRepository
        crawl_queuer.py            # CrawlQueuer
        topic_producer.py          # TopicProducer
        topic_reader.py            # TopicReader
        web_page_fetcher.py        # WebPageFetcher
        request_middleware.py     # RequestMiddleware
        link_extractor.py          # LinkExtractor
        politeness_policy.py       # PolitenessPolicy
        retry_policy.py            # RetryPolicy
        time_provider.py           # TimeProviderFactory ABC
        request_deduplicator.py    # RequestDeduplicator ABC
    infrastructure/
        __init__.py
        db/
            __init__.py
            sqlite_url_state_repository.py
            models.py              # DDL, indexes, row mapping
        queue/
            __init__.py
            in_memory_single_topic_single_partition_queue.py  # InMemorySingleTopicSinglePartitionQueue
            in_memory_topic_producer.py  # InMemoryTopicProducer
            in_memory_topic_reader.py    # InMemoryTopicReader
        console/
            __init__.py
        fetch/
            __init__.py
            aiohttp_web_page_fetcher.py  # AiohttpWebPageFetcher
            headers_middleware.py   # HeadersMiddleware
        html/
            __init__.py
            html_link_extractor.py  # HtmlLinkExtractor
        retry/
            __init__.py
            exponential_backoff_retry_policy.py
        politeness/
            __init__.py
            no_op_politeness_policy.py       # NoOpPolitenessPolicy
        time/
            __init__.py
            system_time_provider.py # SystemTimeProvider
    utils/
        __init__.py
        logger.py                  # stdlib logging, one console handler on the project logger
        html_parser.py             # stdlib HTMLParser wrapper
        in_memory_request_id_deduplicator.py  # InMemoryRequestIdDeduplicator
tests/
    __init__.py
    test_custom_url.py
    test_sqlite_url_state_repository.py
    test_in_memory_topic.py
    test_url_poller.py
    test_orchestrator.py
    test_crawler_worker.py
    test_html_link_extractor.py
    test_aiohttp_web_page_fetcher.py
    test_request_middlewares.py
    test_politeness_and_retry.py
    test_conventions.py
pyproject.toml                    # requires-python >=3.11; aiosqlite, aiohttp, pytest, pytest-asyncio
README.md
```

`requires-python` is `>=3.11`. The claim query needs `UPDATE ... RETURNING`, so SQLite 3.35 or newer is required; this is asserted in step 4 and noted in the README. Sibling imports inside `infrastructure` are allowed, such as `in_memory_topic_reader` importing `in_memory_single_topic_single_partition_queue`.

- Dependency direction: `domain` imports nothing outside `domain`. `ports` depends only on `domain` and the standard library, because every port signature takes a domain type. `application` depends only on `ports` and `domain`, with one exception: the composition root (`main.py` and `application/orchestrator.py`) is the only place that imports `infrastructure` and `utils` concretes for wiring. `infrastructure` depends on `ports`, `domain`, and dependency-free `utils` helpers such as `utils/html_parser.py`. `utils` depends on nothing, with one exception: `utils/in_memory_request_id_deduplicator.py` is a port implementation, so it imports `ports.request_deduplicator` for its type only; it is the only file in `utils` that may.
- `InMemorySingleTopicSinglePartitionQueue` owns one `deque` — no topics, no partitions, no connected-reader list, no `reader_id` bookkeeping. `InMemoryTopicProducer` and `InMemoryTopicReader` are thin views over one shared instance, so producer and reader always see the same deque; each constructor creates the topic, so there is no `connect` step. The queue bounds the deque by a configurable `max_size` defaulting to 10_000; `enqueue` past the cap raises `QueueOverflowError`, and `enqueue_many` reports `False` for that message while still enqueueing the rest. No lock: CPU-bound, single event loop, and the shipped run path has exactly one reader. Both facts are stated in the module docstring, satisfying `goal.md:117`.
- The prod-queue params (`topic`, `consumer_group_id`, and the `partition_key` on each message) are kept for future-proofing: this implementation accepts them and ignores them, so a real prod queue can replace the two adapters without touching a caller (`goal.md:118`). A concise comment in the implementation says so.
- The poller is the only producer: it still builds every message as `BaseMessage(url, partition_key=hash(url))`, which is the partitioning rule a prod queue would use (`goal.md:113`). `CustomURL.__hash__` hashes the canonical form, so it is stable within the process.
- The codebase starts no thread bridge of its own: `main.py` blocks on one `input()` before any task exists, and `aiohttp_web_page_fetcher` uses the natively async `aiohttp` client, bounded by `aiohttp.ClientTimeout(total=RetrySettings.timeout_seconds)`. aiosqlite's connection thread is the only runtime thread, joined by `close()`. `goal.md:53`'s "no thread" is scoped to the DB.

## 6. Implementation order

Each step is a reviewable unit of work. This project has no Git history (see `AGENTS.md`), so progress is recorded by editing files in place rather than by committing.

1. `domain/custom_url.py`: `CustomURL` with string construction, `InvalidURLError`, component properties (scheme, hostname, port, path, query), `get_url`, `__eq__`, `__hash__`; standard-library parsing, exact-host validation, sorted query with duplicate keys preserved, no fragment, no scheme-default port, a preserved non-default port, and `InvalidURLError` on a port that does not parse. This step also establishes the comment policy that every following step follows: every class and function carries a docstring with args, return, and raised exceptions, plus at most two lines of reasoning for non-obvious logic, inline comments appear only for design decisions, concurrency invariants, race avoidance, and trade-offs, and docstrings use those literal `Args:`/`Returns:`/`Raises:` labels — the prose docstrings in sections 3 and 4 are summaries, not literal text.
2. `domain/crawl_state.py`, `domain/messages.py`, `domain/retry_settings.py`: `CrawlState`, `BaseMessage`, `QueueOverflowError`, `RetrySettings`.
3. `ports/*`: every interface ABC above, with the queue split into `ports/topic_producer.py` and `ports/topic_reader.py`. Independently of this step's subject, an empty package `__init__.py` is created alongside each package folder in sections 3 and 5.
4. `infrastructure/db/*`: `SQLiteURLStateRepository(db_path, retry_policy, time_provider, logger)`, asserting SQLite >= 3.35 for `RETURNING`, table creation (including the `times_crawled` counter column and its guarded `ALTER TABLE` migration for databases made before it), the composite index, the `%Y-%m-%d %H:%M:%S` UTC adapter, insert with `created_time = next_crawl_time`, the read-only crawlable select, `claim_candidates`, `claim_urls` with the `IN (...)` list inside the subquery, `mark_started`, `complete_crawl`, and `close`. It logs at INFO when `initialize` creates the schema and at DEBUG for each claim batch, using the injected `logger`. The crawlable predicate is the shared builder in `models.py`. The injected `RetryPolicy` carries the timeout in `RetrySettings.timeout_seconds`, per `goal.md:17`.
5. `infrastructure/queue/*`: `InMemorySingleTopicSinglePartitionQueue` holding one bounded deque, then `InMemoryTopicProducer` and `InMemoryTopicReader` over it. The queue has three methods and no wiring step — nothing to create, so there is no `connect`. Producer: `enqueue` and `enqueue_many` including the overflow rule. Reader: non-reserving `peek` returning `min(n, len)`, and head-based non-idempotent `commit` removing `min(len(items), len)`. No topics, no partitions, no reader list, no `reader_id`. The prod-queue params (`topic`, `consumer_group_id`, message `partition_key`) are accepted and ignored, with one concise comment saying so, and the documented no-lock rationale goes in the queue module.
6. `infrastructure/fetch/*`, `infrastructure/retry/*`, `infrastructure/html/*`, `utils/html_parser.py`: `AiohttpWebPageFetcher` applying the per-call `RetryPolicy` with logging and timeout, fetching through an `aiohttp.ClientSession` and adding no header of its own, the caller's middlewares being the only source bounded by `aiohttp.ClientTimeout(total=RetrySettings.timeout_seconds)`, so the async client needs no thread bridge, and applying its configured `RequestMiddleware` sequence (`HeadersMiddleware` — order is configuration, and a later one overrides an earlier one; auth middlewares are an extension (README) to a fresh headers dict on every attempt; `ExponentialBackoffRetryPolicy` computes `delay_n = min(base_delay_seconds * 2 ** n, max_delay_seconds)` for attempt `n` starting at 0, then sleeps `delay_n + random.uniform(0.0, jitter_seconds)`, where `random` is the module imported in `exponential_backoff_retry_policy.py` and is the only patch point; the per-attempt timeout is `asyncio.wait_for(operation(), timeout_seconds)`; `HtmlLinkExtractor` resolving relative hrefs against the current page URL; and the `utils/html_parser.py` wrapper, created here rather than in step 7 because step 6's extractor imports it.
7. `infrastructure/politeness/*`, `infrastructure/time/*`, `utils/logger.py`, `utils/in_memory_request_id_deduplicator.py`: `NoOpPolitenessPolicy` returning `0` is the only politeness policy shipped, so a crawl is not throttled; the worker's `wait_ms <= sleep_threshold_ms` sleep branch and its above-threshold skip branch stay, because they are what any injected policy drives, and a delaying policy is documented in the README's Extensions section. `InMemoryRequestIdDeduplicator()` satisfies `RequestDeduplicator` structurally with a plain `set` and no expiry: a repeated id is never entertained again (`goal.md:100`). Also the system time provider and the stdlib console logging setup in `utils/logger.py`: `configure_logging(level)` adds one `StreamHandler` to the `webcrawler` logger, added once and never replaced, and returns that logger for injection; `level` is the only filter, so DEBUG or INFO is chosen at startup.
8. `application/worker.py`: `CrawlerWorker`. `run()` peeks until cancelled. Peek one batch; when the batch is empty, `await asyncio.sleep(idle_sleep_seconds)` and loop — the reader's body is CPU-only, so awaiting it does not yield, and that sleep is the only yield on an empty queue. Inside a task group per URL run `mark_started`, the politeness branch, the fetch via `fetcher.fetch(url, self._retry_policy)`, extraction, log the page URL and its links, then record the outcome as a `(url, next_crawl_time)` pair and collect the discovered same-host URLs. A successful row records `now + re_crawl_interval` when that is configured, otherwise `NULL`. After the task group, in this order: (1) `complete_crawl(outcomes, discovered, now)` once, (2) `queuer.enqueue_urls(discovered)` once, (3) `reader.commit(batch)` once. `enqueue_urls` must follow `complete_crawl` because it claims those rows from the DB, so they must be committed first. Handled failures record `now + reschedule_delay`; an uncaught exception aborts without committing.
9. `application/url_poller.py`: `URLPoller` implementing `CrawlQueuer`, the 5-second loop, `request_id` dedupe, `max_items=None` falling back to `max_items_to_queue`, and handing the claimed rows to `producer.enqueue_many([BaseMessage(u, partition_key=hash(u)) for u in rows])` in one bulk call; a `False` result is logged and left for the first poll after `queue_timeout` elapses. A single internal `asyncio.Lock` keeps an API call from interleaving with the periodic poll loop (`goal.md:107`). The lock is acquired exactly once at each of three entry points — `run()`'s poll iteration, `enqueue_urls`, and `queue_candidates` — and each acquires it, calls the private `_claim_and_enqueue`, and releases it. `run()` never calls the public APIs, because `asyncio.Lock` is not re-entrant and a public call from inside the loop would deadlock.
10. `application/orchestrator.py`, `main.py`: composition root and the single-seed run. `main.py` configures logging first, constructs the `SystemTimeProvider`, configures logging first with an explicit `level=logging.INFO`, constructs the `SystemTimeProvider`, constructs one `InMemorySingleTopicSinglePartitionQueue` and injects it into both `InMemoryTopicProducer` and `InMemoryTopicReader` — they must share one queue, or the poller fills a queue the worker never reads — and passes that reader to `CrawlerWorker`, calls `await repository.initialize()`, constructs one shared `ExponentialBackoffRetryPolicy(RetrySettings(...))` and injects it into `SQLiteURLStateRepository` and `CrawlerWorker`, constructs `InMemoryRequestIdDeduplicator()`, constructs `AiohttpWebPageFetcher(retry_settings.timeout_seconds, logger, middlewares=request_middlewares)` with `request_middlewares = (HeadersMiddleware({"User-Agent": USER_AGENT}),)` — the place an auth middleware would be listed — and `HtmlLinkExtractor(logger)`, takes `NoOpPolitenessPolicy()` as the shipped default, blocks on one `input()` for the seed, then awaits `orchestrator.run()`, which owns task creation. `orchestrator.run()` validates that seed as a single `CustomURL`, inserts it, queues it, and only then starts the poller and worker tasks, which run until `Ctrl+C` — which cancels both tasks and closes the repository connection. There is no re-seeding and no child process, and `main.py` guards its `async def main()` behind `if __name__ == "__main__":`, so importing it has no side effects.
11. `tests/*`, `pyproject.toml`, `README.md`. `pyproject.toml` sets `[tool.pytest.ini_options]` with `asyncio_mode = "auto"`, `asyncio_default_fixture_loop_scope = "function"`, and `testpaths = ["tests"]`; the default strict mode would otherwise error or skip every async test and silently weaken the suite.
## 7. Test cases

Every test uses `@pytest.mark.parametrize`, including single-case behaviour. I/O implementations are injected as fakes or in-memory doubles; time comes from `TimeProviderFactory`; the logger is injected.

- `CustomURL`: construction, component access (including the port property), invalid input (including a port that does not parse), reconstruction, equality, and hashing, parameterized over scheme, hostname, path, and query string; equal query keys keep their duplicates after sorting; a fragment and a scheme-default port are dropped, so `http://h:80/x` and `http://h/x` reconstruct identically and compare equal; a non-default port is preserved, so `http://h:80/x` and `http://h:8080/x` are distinct URLs — dropping it would crawl the wrong server.
- Same-host filtering parameterized with `crawlme.monzo.com`, `facebook.com`, `monzo.com`, and `community.monzo.com` (`goal.md:1`).
- `href` resolution: absolute and relative against the current page URL rather than the seed, duplicate links collapsed by the unique set, cycles, and no fragment leaking into the extracted `CustomURL`.
- Retry: success first attempt, success after a transient failure, exhaustion after `max_attempts`, delay growth bounded by `max_delay_seconds`, and an operation exceeding `timeout_seconds` abandoned and retried.
- Fetcher: returns the body on 2xx; a non-2xx status raises; a transport error is retried by the injected `RetryPolicy`; a fetch exceeding `timeout_seconds` is abandoned and retried; every attempt emits a log record (`goal.md:141`); with no middlewares the request carries no header at all, so the caller's middlewares are the only source of headers; every configured middleware's headers reach the request, a later middleware overrides an earlier one, and every retry attempt carries the same headers. Middlewares: `HeadersMiddleware` merges its configured mapping and can override an earlier header; `BasicAuthMiddleware` sets the standard base64 `Authorization`; `BearerAuthMiddleware` sets `Bearer <token>`; chains compose in order and a later one wins.
- Queue: capacity at exactly `max_size` succeeds and one more raises `QueueOverflowError` with `False` in the `enqueue_many` result, and the rest are still enqueued; either adapter may be constructed first, since nothing is wired; a message carrying any `partition_key` (positive, negative, zero) is accepted and ignored — one deque; `enqueue_many` returns one result per message and does not dedupe; `peek` returns `min(n, len)` and removes nothing; `commit` removes exactly `min(len(items), len)` from the head; `commit` is non-idempotent; a consumer group is accepted and is a no-op; appending during processing does not disturb the head.
- Politeness: the shipped `NoOpPolitenessPolicy.before_fetch()` returns `0`; the worker's sleep-or-skip branch is proven through an injected fake policy, since a value at or below `sleep_threshold_ms` sleeps and crawls and a value above it skips the crawl, sets `finished_crawl`, and sets `next_crawl_time = now + timedelta(milliseconds=wait_ms)`. A delaying policy is an extension (README, Extensions), not a shipped class.
- Worker: a batch of N messages produces exactly one `commit`; the call order is `complete_crawl`, then `enqueue_urls`, then `commit`; the per-URL order is `mark_started` before `fetch`; `fetch` receives the injected `RetryPolicy` instance; fetch failure after retries commits and sets `next_crawl_time = now + reschedule_delay`; a `complete_crawl` failure aborts the batch, so there is no `enqueue_urls`, no `commit`, and the row stays `started_crawl` for the `job_timeout` branch to reclaim; success commits once, logs the page URL and its links, and calls `queuer.enqueue_urls` with exactly the discovered same-host URLs; a successful URL records `next_crawl_time = now + re_crawl_interval` when configured and `NULL` otherwise; an empty batch sleeps `idle_sleep_seconds`; `reschedule_delay` defaults to 1 minute and is overridable; `mark_started` writes `started_crawl` and `last_crawl_time` and refreshes `last_status_update_time`.
- Repository: the claim is atomic under concurrent callers with no double-claim and rolls back on failure; every condition in `goal.md:76-91` parameterized independently across `get_crawlable_urls`, `claim_candidates`, and `claim_urls`, which share one predicate builder; `claim_urls` restricts to the caller's URLs before `LIMIT`, so a missing or freshly-queued URL returns nothing and a full batch is not under-filled; `claim_urls([])` returns `[]` and issues no statement; `enqueue_urls` calls `claim_urls` with the caller's URLs while `queue_candidates` calls `claim_candidates`, both passing the configured timeouts; `finished_crawl` with `next_crawl_time <= now` is reclaimed and one with a later time is not; a `NULL` `next_crawl_time` is never selected by the `finished_crawl` branch; a `not_crawled` row always has non-`NULL` `next_crawl_time`; fresh `queued` and `started_crawl` rows are skipped while timed-out ones are reclaimed, proving the epoch arithmetic against a real clock; every stored timestamp round-trips through `strftime('%s', ...)`; `max_items=-1` means no limit; with distinct `next_crawl_time` values and `max_items=1`, the earliest eligible row is returned by `get_crawlable_urls`, `claim_candidates`, and `claim_urls` alike, proving the `ORDER BY` is not dropped; `complete_crawl` inserts discovered URLs with `created_time = next_crawl_time`, leaves existing rows untouched, and refreshes `last_status_update_time`; `create_urls` also writes `created_time == next_crawl_time`; new rows have UTC values and default `state`; `last_crawl_time` is `NULL` before the first attempt and is written only by `mark_started`; `PRAGMA index_list('urls')` reports the composite index and an index over `custom_url`; the injected `RetryPolicy` retries a transient `sqlite3.OperationalError` and re-raises after `max_attempts`; jitter is applied and bounded: with `random.uniform` patched, attempt `n`'s recorded delay lies in `[min(base_delay_seconds * 2**n, max_delay_seconds), min(base_delay_seconds * 2**n, max_delay_seconds) + jitter_seconds]`, so both the exponential growth and the `max_delay_seconds` ceiling are proven. `close()` joins the aiosqlite worker thread and is idempotent; every row starts with `times_crawled = 0`, `complete_crawl` increments it once per finished row and it accumulates across re-crawls, `mark_started` and discovery do not increment it, and a database made before the column is migrated by `initialize` without losing a row or a value.
- Poller: `enqueue_urls` and `queue_candidates` return `None`; `enqueue_urls` queues only the caller's URLs, with no limit, while `queue_candidates` with `max_items=None` falls back to `max_items_to_queue`, which is `-1` unless configured; they raise when the claim or any other DB call raises, while a `False` in the `enqueue_many` result is logged and left for the first poll after `queue_timeout` elapses; the immediately following poll claims nothing while the row's `last_status_update_time` is fresher than `queue_timeout`, and the first poll after it elapses re-claims the row; only rows returned by the claim are enqueued, via a single `enqueue_many` bulk call; a repeated `request_id` is skipped on both APIs; every message passed to `enqueue_many` has `partition_key == hash(url)` for its own url (`goal.md:113`); a retried claim inside one poll check does not grow the queue because the poll-check id reaches `seen_and_record` once per check; an overflowed row is re-claimed by the first poll after `queue_timeout`; a repeated `request_id` is suppressed for the rest of the run, since the shipped deduplicator has no expiry; the loop polls every 5 seconds; API calls are serialized against the loop by a single `asyncio.Lock` (`goal.md:107`); only one poller instance runs; `max_items=None` uses `max_items_to_queue`.
- Orchestrator: the one seed line `main.py` read is queued exactly as given, so a second seed can never be set; a valid seed is inserted (`create_urls`) and then queued (`enqueue_urls`) in that order; an invalid seed is logged at INFO and the run shuts down cleanly with NO poller or worker task created; an exception from `create_urls` or `enqueue_urls` is likewise logged at INFO and shuts down cleanly; only after a successful seed are the poller and worker tasks created, exactly one poller task runs, and the crawl continues until both tasks are cancelled (the `Ctrl+C` path), after which `repository.close()` is awaited. `main()` is executed with a stubbed `input` so a name it uses inside the body but not at module level is still resolved by the suite, and importing `main` starts nothing.
- Conventions: every public class, function, method, and property defined under `src/webcrawler` has a non-empty docstring that uses the literal section labels `Args:`, `Returns:` when the return is not `None`, and `Raises:` when the body raises; dunders are excepted, and enum members are exempt because CPython discards a member docstring rather than attaching it to the member, so `CrawlState`'s values are documented in its class docstring instead; the no-lock rationale is present in the queue module and the CDC rationale in the README; every test function in `tests/` carries `pytest.mark.parametrize`; every public method on a `ports` ABC is a coroutine function or an async generator function, except those whose docstring contains the marker `Synchronous:`, and that exception set is asserted to be exactly `{PolitenessPolicy.before_fetch, LinkExtractor.extract, TimeProviderFactory.now, RequestDeduplicator.seen_and_record, RequestMiddleware.apply}`, so a new sync port fails the test until it is justified; a claim issues `BEGIN IMMEDIATE` before its `UPDATE`; every class under `src/webcrawler` has at most one project base and that base is a `ports` ABC, so no `domain`, `application`, `infrastructure`, or `utils` class is ever used as a base, and the only non-project bases permitted are `str`/`Enum` on `CrawlState`, `ValueError`/`RuntimeError` on `InvalidURLError`/`QueueOverflowError`, and the standard library's `html.parser.HTMLParser` on the single collector class in `utils/html_parser.py`, which cannot receive parser callbacks without subclassing it and whose regex alternative `goal.md:2` rules out; no module under `ports/` or `application/` imports from `infrastructure/` or `utils/` — except `application/orchestrator.py` — and no annotation in `ports/`, `application/worker.py`, or `application/url_poller.py` names a symbol defined in `infrastructure/`; `await repository.close()` joins the aiosqlite worker thread and is idempotent.
- Concurrency: the single-`commit` invariant holds when a producer enqueues while the worker is processing; a barrier-style fake fetcher shows all N URLs of one batch in flight simultaneously (`asyncio.TaskGroup`, `goal.md:139`); one URL's uncaught failure aborts the batch with no `commit` and no `enqueue_urls`.

## 8. Resolved conflicts

- `goal.md:6-7` versus `goal.md:15`: declarations always carry the full signature and at most two lines of reasoning; inline comments are reserved for non-obvious decisions, concurrency invariants, race avoidance, and trade-offs.
- `goal.md:109` says `asyncio.Lock` is not required for the state transition, and the claim query honours that: no lock guards the transition. A lock *is* used in `URLPoller`, for a different problem — keeping an API call from interleaving with the periodic poll loop (`goal.md:107`). The two are unrelated and both are required.
- `last_crawl_time` is written only by `mark_started`, recording the attempt time (`plan_0_reviewed.md:44`); this deliberately overrides `plan_0_reviewed.md:176`, which placed the write in the completion transaction.
- `URLStateRepository.close()` is mandatory, not optional: aiosqlite gives each connection a non-daemon worker thread that blocks until the connection closes, and interpreter shutdown joins non-daemon threads before finalization, so an unclosed connection hangs `Ctrl+C` and pytest alike. `Orchestrator.run()` awaits it after cancelling both tasks.
- The CDC comment lives in the README (`plan_0_reviewed.md:66`); the no-lock queue rationale lives in `in_memory_single_topic_single_partition_queue.py` (`plan_0_reviewed.md:72`).
- No interface for `Orchestrator` or `CrawlerWorker`. Every other major module is interfaced, including `CrawlQueuer`, which `goal.md:106` requires. The value types are deliberately not ported, so `goal.md:11` is read as covering the I/O collaborators.
- `goal.md:59` and `goal.md:104` leave the on-demand limit as "internal constants"; `goal.md:59` names its default (`-1`, no limit), so `max_items_to_queue` is a defaulted constructor parameter rather than an open question, and the periodic poll uses `periodic_max_items=-1`.
- The operator-facing path is API (a) `enqueue_urls`, which queues exactly the URLs passed to it (`plan_0_reviewed.md:62`). API (b) `queue_candidates` stays on the `CrawlQueuer` interface to satisfy `goal.md:104`, but nothing in the shipped run path calls it, so its `max_items` semantics are covered by tests alone.
- Console input is read through an `asyncio.to_thread` bridge around the blocking console read, so the single event loop of `goal.md:13` is preserved without blocking it. That bridge is the only thread this codebase starts; HTTP needs no bridge because `aiohttp` is natively async.
- `plan_0_reviewed.md:107` preferred the standard library so no `pip` install is needed, superseded by the later decision to use `aiohttp` for HTTP: the async client removes the only blocking network call and its thread bridge, which is worth one dependency. `aiosqlite` was already a dependency, and the test dependencies remain `pytest` and `pytest-asyncio`.
- `goal.md:61`'s "check if not queued or startedCrawl" is read as *not currently being processed*: the timeout predicates in `goal.md:84-91` re-select stale `QUEUED`/`STARTED_CRAWL` rows, so no outer `state NOT IN` guard is used.
- `goal.md:150` requires committing queued messages but fixes no granularity; the batch decision chooses one `commit` per batch after the task group.
- `goal.md:103`'s comma-separated form is superseded twice: the port-level API (a) `enqueue_urls` takes a list of `CustomURL` (`plan_0_reviewed.md:62`), and the shipped run path seeds exactly ONE URL once — the whole line is validated as a single `CustomURL` and never split on commas — after which no further seed can be entered and the crawl runs until `Ctrl+C`.
- `goal.md:140` says `0` means call now and a number means call after that many milliseconds, so the policy returns an `int` and the worker decides between sleeping and rescheduling.
- `goal.md:23` gives `next_crawl_time` a current-timestamp default, but `goal.md:27` requires it to equal `created_time` on insert. The application sets both from `TimeProviderFactory` so the equality holds regardless of SQLite's clock.
- `goal.md:131-132` constructor-based `CustomURL` with `get_url` reconstruction is implemented as specified, with the fragment dropped per the user's decision. The earlier plan's port-dropping rule is corrected here: a non-default port is preserved, because it selects a different server — the end-to-end smoke run proved it, crawling `http://127.0.0.1:8731/` as port 80 and failing. Only the scheme default (`http` 80, `https` 443) collapses, so `http://h:80/x` == `http://h/x` while `http://h:8080/x` stays its own URL.
- Request auth and headers go through a `RequestMiddleware` port applied by the fetcher, not through constructor flags. The simpler alternatives — a static `headers`/`auth` constructor parameter, or `aiohttp`'s own session-level `ClientSession(headers=..., auth=...)` — cover a fixed credential but cannot compose or vary per request, and the middleware port composes auth and header policies in configurable order while the fetcher stays ignorant of both. A token that needs refreshing would make the port async; the static-token and Basic cases are pure computation and stay sync, which is why `RequestMiddleware.apply` is the fifth justified `Synchronous:` port method.
- `times_crawled` makes a re-crawl loop detectable from the data alone: `ORDER BY times_crawled DESC` shows any URL the crawler revisited, without needing the visit log.
