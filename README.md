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

The qualities the crawler is judged on, with where each one actually stands. The
status column is deliberate: a requirement list that claims everything is met is
worthless as a design document.

| # | Requirement | Status |
|---|---|---|
| NFR1 | **Concurrent.** Many pages are fetched at once, not one after another. | Met. `CrawlerWorker` fetches all 50 messages of a batch in one `asyncio.TaskGroup`; `CrawlerWorkerV1` detaches a crawl per message and holds up to 1000 fetches in flight. Both on a single event loop. |
| NFR2 | **Fetch fairly, without saturating the host.** Do not hammer a site. | **Not met.** The shipped `NoOpPolitenessPolicy` answers `0` for every URL, so nothing throttles. The interface and the deferral path exist, so a delaying policy drops in without touching the worker, but no such policy is written. See Opportunities. |
| NFR3 | **Not overly saturate the crawled website.** | Partial. Retries back off exponentially with jitter, and a failed URL waits 5 minutes before it is due again, so a rate-limited host is not retried in a tight loop. `CrawlerWorkerV1`'s cap is 1000 fetches in flight and it is **process-wide, not per host**, so on a single-host crawl all 1000 can land on one site: against a local test server that served 150ms pages, 65 of 4000 URLs exhausted the 12s fetch timeout at that setting. There is no `robots.txt` check. |
| NFR4 | **Modular.** Each concern in its own module, not one service per module. | Met. Modules in separate folders, all on one event loop. |
| NFR5 | **Scalable to higher traffic.** | Met by substitution, not by size. The store, queue, and fetcher sit behind interfaces, so the process scales by swapping them; the shipped crawler is single-process and runs one worker, whichever of the two is chosen. |
| NFR6 | **Composition over inheritance.** | Met. Collaborators are constructor arguments. The classes that extend a port are the store and the two workers, and nothing extends them in turn. |
| NFR7 | **Coding best practices.** SOLID, with each boundary behind an interface shipping one simple default. | Met. Eleven interfaces in `ports/`, each with one shipped implementation, except `CrawlWorker`, which ships two. |
| NFR8 | **I/O best practices.** Every I/O call owned by the implementation that makes it. | Met. Retry and timeout live in the store and the fetcher, never in their callers, so nothing is retried twice. No I/O call blocks the event loop. |
| NFR9 | **Fast.** | Met. The selection query is served by its indexes; the claim is a single `UPDATE ... RETURNING` per chunk rather than a select followed by an update. |
| NFR10 | **Testable, with high code coverage.** | Partial. 97 unit tests, every collaborator mocked, so the suite is fast and needs no network. Coverage is **not** measured: no coverage tool is configured, so the number is unverified rather than high. |
| NFR11 | No crawling framework; the crawl loop, scheduler, and queue are our own. | Met. |
| NFR12 | Async APIs wherever the operation is I/O. | Met. Everything under `ports/` is `async` except the three pure-CPU boundaries: link extraction, the clock, and request middleware. |
| NFR13 | Complete signatures: arguments, return, and the exceptions a caller must handle, documented per function. | Met. |
| NFR14 | Comments only for non-obvious design decisions, concurrency invariants, race avoidance, and trade-offs. | Met. |
| NFR15 | Parameterised unit tests. | Met. |
| NFR16 | All timestamps stored are UTC; a new row gets `next_crawl_time = created_time`. | Met. |
| NFR17 | Index the primary key, and the state plus both time columns for the selection predicate. | Met. |
| NFR18 | SQLite >= 3.35, because the claim query is `UPDATE ... RETURNING`. | Met, asserted at startup. |
| NFR19 | No re-crawl of a finished URL unless a re-crawl interval is configured. | Met. |

## High Level Design

<!-- Intentionally empty at this stage. -->

## Low Level Design

### Layout

One process, one asyncio event loop, and seven modules that matter. `main.py` is
the only place that knows which implementation is in use.

| Module | One-line description |
|---|---|
| **db** (`infrastructure/db/`) | The crawl-state store. Holds one row per URL with its state and timestamps, and owns the SQL, the transactions, and its own retry. |
| **db poller** (`application/url_poller.py`) | Asks the store which URLs are due, marks them `queued` in one bulk statement, and feeds them to the queue. |
| **queue** (`infrastructure/queue/`) | Holds the pending work between the poller and the worker, as a bounded buffer with a separate parking area for messages that must not be retried. |
| **crawl worker** (`application/worker.py`) | `CrawlerWorker`, the batch consumer. Takes a batch off the queue, fetches every page in it concurrently, extracts the links, records the outcome, and commits. |
| **non-blocking crawl worker** (`application/worker_v1.py`) | `CrawlerWorkerV1`, the other `CrawlWorker`. Detaches a crawl per message and commits the batch at once, so a slow page never holds new work back, and writes the store in bulk on a 10ms timer. |
| **orchestrator** (`application/orchestrator.py`) | Seeds the crawl once, then runs the poller and the worker together and stops both when either fails. |
| **main** (`main.py`) | The composition root. Reads the seed and the worker's choice, constructs every object with its concrete class, runs the orchestrator, and releases everything on the way out. |

Three supporting packages sit underneath those six:

| Package | One-line description |
|---|---|
| `domain/` | The value types every module agrees on: the canonical URL, the four crawl states, the queue message, and the error types that tell retry how to read a failure. |
| `ports/` | The eleven interfaces. What a module needs from another module, stated without saying how it is done. |
| `utils/` | Two helpers with no project dependency: the `<a href>` collector and the logger setup. |

### Modularity and composition

Two things make this replaceable rather than merely tidy.

**Composition over inheritance.** Every collaborator is a constructor argument, passed in from `main.py`. A module reaches another only through the interface it was handed, never by importing a concrete class. The only classes that extend a port are `SQLiteURLStateRepository` and the two `CrawlWorker`s, and nothing extends them in turn.

**The implementation is the only thing that changes.** Because `application/` imports no implementation at all, swapping one is a constructor change in `main.py`:

| Instead of | Write | Application changes |
|---|---|---|
| the in-memory queue | a Kafka producer and reader against `TopicProducer` / `TopicReader` | None. The reader already takes `topic` and `consumer_group_id`, the producer takes `topic`, and every `BaseMessage` already carries a `partition_key`. The in-memory pair accepts all three and ignores them, so the shape is already a broker's. |
| SQLite | a Postgres or MySQL store against `URLStateRepository` | None, though this one is real work: the claim is a single `UPDATE ... RETURNING`, which Postgres spells differently, and `sqlite3.Error` in the implementation's `Raises:` becomes a driver error. |
| `aiohttp` | `httpx` against `WebPageFetcher` | None. Two methods, `fetch` and `close`. |
| the no-op politeness policy | a rate-limiting one against `PolitenessPolicy` | None. The worker already asks before every fetch and never sleeps the answer. |
| either worker | another one against `CrawlWorker` | None. The orchestrator only ever calls `run` and `close`, so a third worker is a new class and one line in `main.py`. |

What does *not* survive a swap unchanged is scale. The shipped process is one
worker reading one queue, so raising throughput means more workers, and the
queue has to honour the partition key before two workers can share it safely.
The store's claim is already safe for that, since a claimed row leaves the
crawlable set.

### Every file

The seven modules above are the ones that matter. This is the full tree, for
anyone reading along:

```
src/webcrawler/
  main.py                                  composition root: reads the seed, builds every object, runs, releases
  application/
    orchestrator.py                        seeds once, then runs the poller and the worker in one TaskGroup
    url_poller.py                          CrawlQueuer: claims store rows on a timer and bulk-feeds the queue
    worker.py                              CrawlerWorker: peek a batch, crawl it concurrently, record, commit
    worker_v1.py                           CrawlerWorkerV1: detach a crawl per message, bulk-write on a timer
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
    crawl_worker.py                        worker interface: run the consume loop, close what it owns
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
domain(aka models)  <-  ports(aka interfaces)
   ^                       ^            
   +-----------------------+-------------------------->  infrastructure(aka implementations)  <-  application  <-  main.py
                                                               ^
                                                               +-->  utils
```

### Class diagram

The eleven ports, the twelve implementations, and the arrows between them.
`*--` is a constructor-held collaborator, `<|--` is an implementation of a
port, and `..>` is a type used only in a signature.

GitHub renders the block below as it stands. A local markdown viewer may not:
in VS Code install **Markdown Preview Mermaid Support** (the stock preview
shows the source), in Obsidian it works with no plugin, and anywhere else
paste the block into <https://mermaid.live>. The same text is
[`docs/uml.mmd`](docs/uml.mmd), which is what you edit.

```mermaid
classDiagram
    direction TB

    %% ---------------------------------------------------------------------
    %% Ports: every interface lives in src/webcrawler/ports/ and is an ABC.
    %% ---------------------------------------------------------------------

    class CrawlWorker {
        <<abstract>>
        +run()*
        +close()*
    }

    class CrawlQueuer {
        <<abstract>>
        +run()*
        +enqueue_urls(urls, request_id)*
        +queue_candidates(now, request_id, max_items)*
    }

    class URLStateRepository {
        <<abstract>>
        +initialize()*
        +create_urls(urls)*
        +close()*
        +get_crawlable_urls(now, max_items, job_timeout, queue_timeout)*
        +claim_candidates(now, max_items, job_timeout, queue_timeout)*
        +claim_urls(urls, now, max_items, job_timeout, queue_timeout)*
        +mark_started(urls, now)*
        +complete_crawl(finished, discovered, now)*
    }

    class TopicProducer {
        <<abstract>>
        +enqueue(message, request_id)*
        +enqueue_many(messages, request_id)*
        +enqueue_to_deadletter(message, request_id)*
    }

    class TopicReader {
        <<abstract>>
        +peek(n)*
        +commit(messages, request_id)*
    }

    class WebPageFetcher {
        <<abstract>>
        +fetch(url)*
        +close()*
    }

    class LinkExtractor {
        <<abstract>>
        +extract(html, base_url) set~CustomURL~*
    }

    class PolitenessPolicy {
        <<abstract>>
        +before_fetch(url)*
        +record_fetch(now, url, result)*
    }

    class RetryPolicy {
        <<abstract>>
        +execute(operation) T*
    }

    class TimeProviderFactory {
        <<abstract>>
        +now() datetime*
    }

    class RequestMiddleware {
        <<abstract>>
        +apply(url, headers)*
    }

    %% ---------------------------------------------------------------------
    %% Domain. Three frozen dataclasses (BaseResult, BaseMessage, RetrySettings),
    %% one hand-written immutable class with __slots__ and its own __eq__ and
    %% __hash__ (CustomURL), one enum, and the exception types. domain/ imports
    %% nothing from the project except messages.py importing custom_url.py.
    %% ---------------------------------------------------------------------

    class CustomURL {
        <<value>>
        +get_url() str
        +scheme
        +hostname
        +port
        +path
        +query
    }

    class BaseMessage {
        <<value>>
        +url CustomURL
        +partition_key int
    }

    class BaseResult {
        <<value>>
        +is_success bool
    }

    class RetrySettings {
        <<value>>
        +max_attempts
        +base_delay_seconds
        +max_delay_seconds
        +jitter_seconds
        +timeout_seconds
    }

    class CrawlState {
        <<enumeration>>
        NOT_CRAWLED = not_crawled
        QUEUED = queued
        STARTED_CRAWL = started_crawl
        FINISHED_CRAWL = finished_crawl
    }

    class QueueOverflowError {
        <<error>>
    }

    class InvalidURLError {
        <<error>>
    }

    class NonRetryableError {
        <<error>>
    }

    class RetryableStatusError {
        <<error>>
    }

    class RuntimeError {
        <<builtin>>
    }

    class ValueError {
        <<builtin>>
    }

    %% ---------------------------------------------------------------------
    %% Application: the orchestrator and the two interchangeable consumers.
    %% ---------------------------------------------------------------------

    class Orchestrator {
        -URLStateRepository _repository
        -URLPoller _poller
        -CrawlWorker _worker
        -str _seed_line
        +run()
    }

    class URLPoller {
        -URLStateRepository _repository
        -TopicProducer _producer
        -TimeProviderFactory _time_provider
        +run()
        +enqueue_urls(urls, request_id)
        +queue_candidates(now, request_id, max_items)
    }

    class CrawlerWorker {
        -URLStateRepository _repository
        -TopicReader _reader
        -TopicProducer _producer
        -WebPageFetcher _fetcher
        -LinkExtractor _link_extractor
        -PolitenessPolicy _politeness_policy
        -CrawlQueuer _queuer
        -TimeProviderFactory _time_provider
        +run()
        +close()
    }

    class CrawlerWorkerV1 {
        -URLStateRepository _repository
        -TopicReader _reader
        -TopicProducer _producer
        -WebPageFetcher _fetcher
        -LinkExtractor _link_extractor
        -PolitenessPolicy _politeness_policy
        -CrawlQueuer _queuer
        -TimeProviderFactory _time_provider
        -Semaphore _fetch_slots
        -set~CustomURL~ _started
        -dict~CustomURL,datetime-or-None~ _finished
        -set~CustomURL~ _discovered
        -set~Task-of-None~ _crawling
        +run()
        +close()
    }

    %% ---------------------------------------------------------------------
    %% Infrastructure: one shipped implementation per port, except the queue
    %% which is a plain class with no interface at all.
    %% ---------------------------------------------------------------------

    class SQLiteURLStateRepository {
        -str _db_path
        -RetryPolicy _retry_policy
        -TimeProviderFactory _time_provider
        +initialize()
        +create_urls(urls)
        +close()
        +get_crawlable_urls(now, max_items, job_timeout, queue_timeout)
        +claim_candidates(now, max_items, job_timeout, queue_timeout)
        +claim_urls(urls, now, max_items, job_timeout, queue_timeout)
        +mark_started(urls, now)
        +complete_crawl(finished, discovered, now)
    }

    class AiohttpWebPageFetcher {
        -float _timeout_seconds
        -RetryPolicy _retry_policy
        -tuple _middlewares
        +fetch(url) str
        +close()
    }

    class HtmlLinkExtractor {
        +extract(html, base_url) set~CustomURL~
    }

    class NoOpPolitenessPolicy {
        +before_fetch(url) int
        +record_fetch(now, url, result)
    }

    class ExponentialBackoffRetryPolicy {
        -RetrySettings _settings
        +execute(operation) T
    }

    class SystemTimeProvider {
        +now() datetime
    }

    class HeadersMiddleware {
        +apply(url, headers)
    }

    class InMemoryTopicProducer {
        -str _topic
        -InMemorySingleTopicSinglePartitionQueue _queue
        +enqueue(message, request_id) bool
        +enqueue_many(messages, request_id) list~bool~
        +enqueue_to_deadletter(message, request_id) bool
    }

    class InMemoryTopicReader {
        -str _topic
        -str _consumer_group_id
        -InMemorySingleTopicSinglePartitionQueue _queue
        +peek(n) list~BaseMessage~
        +commit(messages, request_id)
    }

    class InMemorySingleTopicSinglePartitionQueue {
        -int _max_size
        -int _max_deadletter_size
        +enqueue(message)
        +peek(n) list~BaseMessage~
        +commit(count)
        +enqueue_deadletter(message)
    }

    %% ---------------------------------------------------------------------
    %% Composition root. The only module that names a concrete
    %% infrastructure class; orchestrator.py also names the concrete
    %% URLPoller, since it is the one port it is constructed with.
    %% `main` is a module-level function, not a class, drawn as a node so the
    %% wiring it performs has somewhere to hang.
    %% ---------------------------------------------------------------------

    class Main {
        <<module, composition root>>
        +main()
    }

    %% ---------------------------------------------------------------------
    %% Inheritance: eleven ABCs, twelve implementations.
    %% CrawlWorker is the only port with more than one.
    %% ---------------------------------------------------------------------

    CrawlWorker <|-- CrawlerWorker
    CrawlWorker <|-- CrawlerWorkerV1
    CrawlQueuer <|-- URLPoller
    URLStateRepository <|-- SQLiteURLStateRepository
    TopicProducer <|-- InMemoryTopicProducer
    TopicReader <|-- InMemoryTopicReader
    WebPageFetcher <|-- AiohttpWebPageFetcher
    LinkExtractor <|-- HtmlLinkExtractor
    PolitenessPolicy <|-- NoOpPolitenessPolicy
    RetryPolicy <|-- ExponentialBackoffRetryPolicy
    TimeProviderFactory <|-- SystemTimeProvider
    RequestMiddleware <|-- HeadersMiddleware

    %% ---------------------------------------------------------------------
    %% Held as constructor arguments.
    %% Filled `*--` = composition, exclusive ownership. Only main.py does that,
    %% because it is the module that constructs everything.
    %% Hollow `o--` = aggregation, a shared instance. One store is held by the
    %% orchestrator, the poller and whichever worker is running; one queue by
    %% both adapters; one clock by four classes. So at runtime a worker's
    %% _queuer is the very URLPoller the orchestrator holds.
    %% ---------------------------------------------------------------------

    Main *-- Orchestrator
    Main *-- SQLiteURLStateRepository
    Main *-- InMemorySingleTopicSinglePartitionQueue
    Main *-- InMemoryTopicProducer
    Main *-- InMemoryTopicReader
    Main *-- AiohttpWebPageFetcher
    Main *-- HtmlLinkExtractor
    Main *-- NoOpPolitenessPolicy
    Main *-- ExponentialBackoffRetryPolicy
    Main *-- SystemTimeProvider
    Main *-- HeadersMiddleware
    Main *-- URLPoller
    Main *-- CrawlWorker

    Orchestrator o-- URLStateRepository
    Orchestrator o-- URLPoller
    Orchestrator o-- CrawlWorker

    URLPoller o-- URLStateRepository
    URLPoller o-- TopicProducer
    URLPoller o-- TimeProviderFactory

    CrawlerWorker o-- URLStateRepository
    CrawlerWorker o-- TopicReader
    CrawlerWorker o-- TopicProducer
    CrawlerWorker o-- WebPageFetcher
    CrawlerWorker o-- LinkExtractor
    CrawlerWorker o-- PolitenessPolicy
    CrawlerWorker o-- CrawlQueuer
    CrawlerWorker o-- TimeProviderFactory

    CrawlerWorkerV1 o-- URLStateRepository
    CrawlerWorkerV1 o-- TopicReader
    CrawlerWorkerV1 o-- TopicProducer
    CrawlerWorkerV1 o-- WebPageFetcher
    CrawlerWorkerV1 o-- LinkExtractor
    CrawlerWorkerV1 o-- PolitenessPolicy
    CrawlerWorkerV1 o-- CrawlQueuer
    CrawlerWorkerV1 o-- TimeProviderFactory

    SQLiteURLStateRepository *-- RetryPolicy
    SQLiteURLStateRepository o-- TimeProviderFactory
    AiohttpWebPageFetcher *-- RetryPolicy
    AiohttpWebPageFetcher *-- RequestMiddleware
    ExponentialBackoffRetryPolicy *-- RetrySettings

    %% The only concrete-to-concrete edges in the graph. The queue has no
    %% port, so both adapters import it directly, and main.py is the only
    %% thing that guarantees both hold the same instance.
    InMemoryTopicProducer o-- InMemorySingleTopicSinglePartitionQueue
    InMemoryTopicReader o-- InMemorySingleTopicSinglePartitionQueue

    %% ---------------------------------------------------------------------
    %% Value types held or passed.
    %% ---------------------------------------------------------------------

    BaseMessage --> CustomURL : holds

    %% Raises, not extends. Dotted so the arrowhead cannot be read as
    %% inheritance: CustomURL does not inherit from InvalidURLError.
    ValueError <|-- InvalidURLError
    RuntimeError <|-- QueueOverflowError
    RuntimeError <|-- NonRetryableError
    RuntimeError <|-- RetryableStatusError

    CustomURL ..> InvalidURLError : raises
    InMemorySingleTopicSinglePartitionQueue ..> QueueOverflowError : raises
    RetryPolicy ..> NonRetryableError : re-raises at once
    RetryPolicy ..> RetryableStatusError : retried
    AiohttpWebPageFetcher ..> NonRetryableError : raises outside 200-299
    AiohttpWebPageFetcher ..> RetryableStatusError : raises on retryable status

    PolitenessPolicy ..> BaseResult : passes
    URLStateRepository ..> CustomURL : keys on
    SQLiteURLStateRepository ..> CrawlState : writes
    TopicProducer ..> BaseMessage : carries
    TopicReader ..> BaseMessage : returns
    InMemorySingleTopicSinglePartitionQueue --> BaseMessage : holds in its deques
    CrawlerWorker ..> BaseMessage : peeks, crawls, commits
    CrawlerWorkerV1 ..> BaseMessage : peeks, crawls, commits
    URLPoller ..> BaseMessage : builds one per claimed URL

    %% The implementations that key on, return, or write the domain types the
    %% ports already name above them. Drawn on both sides deliberately: the
    %% port states the contract, the implementation states the dependency.
    SQLiteURLStateRepository ..> CustomURL : keys on
    AiohttpWebPageFetcher ..> CustomURL : fetches
    HtmlLinkExtractor ..> CustomURL : returns
    NoOpPolitenessPolicy ..> CustomURL : asked about
    NoOpPolitenessPolicy ..> BaseResult : records
    HeadersMiddleware ..> CustomURL : applied per request

    %% ---------------------------------------------------------------------
    %% Notes on the parts a naive reading of the code gets wrong.
    %% ---------------------------------------------------------------------

    note for Orchestrator "Holds the concrete URLPoller,<br>not the CrawlQueuer port.<br>Closes neither the worker nor the store:<br>main.py does that in a finally block."

    note for CrawlerWorker "Batch worker, the non-default choice.<br>mark_started is AWAITED before the fetches,<br>every crawl is awaited to completion,<br>then complete_crawl, then enqueue_urls,<br>then commit LAST."

    note for CrawlerWorkerV1 "Non-blocking worker and the default choice,<br>the alternative to CrawlerWorker, never alongside it.<br>mark_started only fills a set; the store write happens in the<br>flush loop, on a timer of DEFAULT_FLUSH_INTERVAL_SECONDS.<br>commit happens as the tasks are CREATED,<br>before any crawl finishes.<br>Fetches bounded by a semaphore of DEFAULT_MAX_CONCURRENT_FETCHES,<br>not by the queue."

    note for URLPoller "Only the claim is inside try/except.<br>The enqueue_many feed is unguarded and<br>a producer failure propagates to the caller."

    note for SQLiteURLStateRepository "One of twelve classes extending a port,<br>and the only one nothing extends in turn.<br>Owns its retry, a transaction lock, and the aiosqlite connection."

    note for AiohttpWebPageFetcher "Owns its retry.<br>Middleware is a tuple applied synchronously<br>before each request, not an await point.<br>A status outside 200-299 raises, and only the seven in<br>RETRYABLE_STATUS_CODES are retried:<br>everything else, 3xx included, fails at once."

    note for InMemorySingleTopicSinglePartitionQueue "No interface and no port, the one class here with no base.<br>Both adapters aggregate over one instance,<br>and neither subclasses it.<br>peek(n) does not reserve, and commit(count) is head-based:<br>committing twice removes twice as many.<br>The deadletter half is never drained,<br>so its capacity is a memory ceiling."

    note for LinkExtractor "One of three synchronous ports,<br>with TimeProviderFactory and RequestMiddleware.<br>extract is not an await point."

    note for Main "Reads the seed, asks which worker to build,<br>constructs every object, runs the orchestrator,<br>and releases the worker then the store."
```

### Extending it

Every boundary is an `ABC`, so a replacement is a new class plus one line in
`main.py`. Nothing under `ports/` changes.

| Port | Shipped default | A new implementation must provide |
|---|---|---|
| `URLStateRepository` | `SQLiteURLStateRepository` | 7 async methods: `initialize`, `create_urls`, `close`, `get_crawlable_urls`, `claim_candidates`, `claim_urls`, `mark_started`, `complete_crawl` |
| `CrawlQueuer` | `URLPoller` | 3 async methods: `run`, `enqueue_urls`, `queue_candidates` |
| `CrawlWorker` | `CrawlerWorker`, `CrawlerWorkerV1` | 2 async methods: `run`, `close` |
| `TopicProducer` | `InMemoryTopicProducer` | 3 async methods: `enqueue`, `enqueue_many`, `enqueue_to_deadletter` |
| `TopicReader` | `InMemoryTopicReader` | 2 async methods: `peek`, `commit` |
| `WebPageFetcher` | `AiohttpWebPageFetcher` | 2 async methods: `fetch`, `close` |
| `LinkExtractor` | `HtmlLinkExtractor` | 1 sync method: `extract(html, base_url) -> set[CustomURL]` |
| `PolitenessPolicy` | `NoOpPolitenessPolicy` | 2 async methods: `before_fetch`, `record_fetch` |
| `RetryPolicy` | `ExponentialBackoffRetryPolicy` | 1 async generic method: `execute(operation)` |
| `TimeProviderFactory` | `SystemTimeProvider` | 1 sync method: `now()` |
| `RequestMiddleware` | `HeadersMiddleware` | 1 sync method: `apply(url, headers)` |

`AiohttpWebPageFetcher` also takes a `session_factory`, which is how the tests
substitute a session.

Two of these are not ports. The in-memory queue is a plain class with no
interface, so a real broker replaces it by replacing **both** queue adapters
together, which is why the swap table above lists the pair.

### Database schema

One table, `urls`, in `webcrawler.db`. Every timestamp is a UTC `TEXT` in
SQLite's `YYYY-MM-DD HH:MM:SS` form, which sorts correctly as a string and lets
`CURRENT_TIMESTAMP` be used as a default.

| Column | Type | Null | Default | Meaning |
|---|---|---|---|---|
| `custom_url` | `TEXT` | no | | **Primary key.** The canonical URL text itself, not a surrogate id. |
| `created_time` | `TEXT` | no | `CURRENT_TIMESTAMP` | When the crawler first learned the URL. |
| `last_crawl_time` | `TEXT` | yes | | When a worker last began crawling it. `NULL` until the first attempt. |
| `next_crawl_time` | `TEXT` | yes | `CURRENT_TIMESTAMP` | The earliest time the URL may be claimed. Set equal to `created_time` on insert, so a new row is immediately claimable. |
| `state` | `TEXT` | no | `'not_crawled'` | One of `not_crawled`, `queued`, `started_crawl`, `finished_crawl`. |
| `last_status_update_time` | `TEXT` | no | `CURRENT_TIMESTAMP` | When the row last changed state. The two staleness timeouts compare against this. |
| `times_crawled` | `INTEGER` | no | `0` | Incremented once per completed crawl. Makes a re-crawl loop visible in the data alone. |

The primary key is the URL itself, the canonical text rather than a surrogate id,
so two spellings of one page are the same string and a duplicate is impossible by
construction.

**Indexes.** Three exist, one of them SQLite's own for the primary key:

| Index | Columns | Serves |
|---|---|---|
| `sqlite_autoindex_urls_1` | `custom_url` | the primary key lookup |
| `idx_urls_state_next` | `(state, next_crawl_time)` | claiming due URLs, which orders by `next_crawl_time` |
| `idx_urls_state_status` | `(state, last_status_update_time)` | the two staleness branches, which range over `last_status_update_time` within one state |

The second index is why staleness is compared against the bare column rather than
wrapped in `strftime(...)`, which would stop the index matching. `EXPLAIN QUERY
PLAN` confirms both are used, combined as a `MULTI-INDEX OR`.

Two guards are deliberately absent, because each looks like hygiene and quietly
breaks recovery. There is no `NOT NULL` on `next_crawl_time`, since a `NULL`
there is how a finished URL records "never again" and `NULL <= :now` is never
true. And the claim has no `state NOT IN (...)` filter, nor a top-level
`next_crawl_time IS NOT NULL` guard, because either would permanently exclude the
two staleness branches that reclaim abandoned work.

The SQL itself lives in `src/webcrawler/infrastructure/db/models.py`, with the
crawlable predicate written once and shared by the read and the claim so the two
cannot disagree.

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

## Design Decisions

The choices that are not obvious from the code, each with what it cost.

| Decision | Why | What it cost |
|---|---|---|
| Modules in folders on one event loop, not a service per module | The brief asked for a production shape without a deployment's worth of moving parts. One process means one queue, one store, and no network hop between the poller and the worker. | No horizontal scale for free. Raising throughput means more workers, which needs a broker that honours the partition key. |
| One batch, one transaction, one commit | `complete_crawl` writes every outcome and every discovered URL atomically, so a crash cannot leave a page recorded without its links. | A failed write loses the whole batch's outcomes, so the batch is deadlettered and its rows are left `started_crawl` for `job_timeout` to reclaim. |
| `complete_crawl` before `enqueue_urls` before `commit` | Each step only after the one before it is durable. `enqueue_urls` claims the rows the insert created, and `commit` acknowledges work that is only finished once recorded. | None. The other orderings all double-fetch or lose work. |
| A queue timeout stands in for a CDC pipeline | If a row reaches `queued` and the process dies before the message is sent, no change-data-capture stream exists to reconcile the two. A `queued` row older than `queue_timeout` is simply re-selected. | A lost message waits out the timeout. Set to 60 minutes, because at 30s a real backlog was being re-fetched while it still waited. |
| Two retry settings, not one | A locked database frees in milliseconds; a 429 clears only on a seconds-scale window. One value fitted neither: it spent the whole fetch budget in 1.5s of backoff. | Two constants to keep coherent, and the fetch budget must stay under `JOB_TIMEOUT` or a still-retrying URL is claimed twice. |
| A politeness wait defers, it never sleeps | One slow URL must not stall a batch, so the URL is rescheduled as `now + wait_ms` and the batch moves on. | A delaying policy is therefore not a throttle, it is a scheduler hint. Real pacing would need the cap in NFR2. |
| Two workers behind one `CrawlWorker` port, picked at the prompt | They lose in opposite directions. The batch worker is ahead when every page answers in milliseconds; the non-blocking one is far ahead when one page stalls, since the batch worker leaves its whole batch waiting. Which case a run hits is not knowable before the run. | The operator chooses per run, and the two have to be kept at behavioural parity, since the port declares them the same worker. |
| Bulk writes on a timer, not on a batch boundary | Waiting for a batch delays every write by its slowest fetch, and the non-blocking worker has no batch to wait for. | A write lands up to `flush_interval_seconds` late, and a crash inside that window leaves those rows for `job_timeout` to reclaim. The period is 10ms, which is past the knee: on a chain-shaped site 0.5s managed 20 pages in 20s where 10ms managed 335, but going from 50ms to 10ms bought only 1.4x more, because the per-page fetch and store cost starts to dominate the window. |
| Dedupe with sets and dicts, keyed by canonical URL | Two spellings of one page are the same string, so a duplicate is impossible by construction rather than something to check for. `times_crawled` can then be trusted as a re-crawl detector. | A URL that is both finished and discovered in one batch is written once, so the finish update has to win over the insert. |
| One crawlable predicate shared by the read and the claim | Two copies of that SQL would eventually disagree, and the disagreement would be silent. | None, once it is one string. |
| Bulk statements chunked under SQLite's parameter limit | A 1200-URL batch exceeds the 999 bound-parameter ceiling, so statements are chunked, each binding the remaining limit so chunking cannot overshoot `max_items`. | Bound to SQLite. Postgres has no such ceiling, so the chunking becomes unnecessary rather than wrong. |
| Empty input issues no statement | `IN ()` is rejected outright by some engines, so an empty set short-circuits. | None, and it makes the empty case cheap. |
| Retry owned by the I/O implementation | The store and the fetcher each hold a policy; the poller and the worker hold none, so nothing is retried twice. A `commit` is never retried either, being head-based, so a second attempt would remove more than the batch owns. | A caller cannot add its own retry without risking a double. |
| The in-memory queue has no lock | CPU-bound on a single event loop, and the shipped path has exactly one reader. | A second reader would need one. The `peek`/`commit` contract is already count-based, so it would not change the callers. |
| Tests mock every collaborator | A unit test that builds a real store and a real queue tests the implementation twice and breaks whenever it is refactored. | The store's own SQL is asserted through a mocked `aiosqlite` connection rather than a real database, so it is checked as calls and parameters, not as stored state. |
| 97 tests, happy path and the failure that matters | A test earns its place by naming a decision. The failing paths kept are the ones that change what happens next: a fetch that fails without stopping its batch, a `complete_crawl` that fails without enqueueing, a claim that exhausts its budget. Both workers are covered, so the port that stands between them has both sides of the contract tested. | No coverage measurement is configured, so the number is unverified. NFR10. |

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
starts.

Next it asks which worker to use, the same prompt every run:

```
worker> Pick the worker: 1 = CrawlerWorker, which waits for each batch to finish. 2 = CrawlerWorkerV1, which keeps fetching while earlier pages are still in flight, up to 1000 at a time. Press Enter for CrawlerWorkerV1.
```

Press Enter for `CrawlerWorkerV1`, the default, or type `1` for the batch worker.
They also differ in when they write: `CrawlerWorker` writes once per finished
batch, while `CrawlerWorkerV1` writes every 10ms whatever is buffered, and it
looks at the queue every 10ms rather than once a second. On the demo site,
`CrawlerWorkerV1` crawled 18381 pages where `CrawlerWorker` crawled 4856 in the
same 35s, because a fast site spends most of its time waiting for links it has
already found to become crawlable, and 10ms of window is nearly no wait. On a
local site with 150ms responses and a 6-second stall every twentieth page the
gap is far wider: `CrawlerWorkerV1` crawled all 4000 pages where `CrawlerWorker`
managed 321, because it left 3066 claimed rows waiting behind the slow pages.
That is why `CrawlerWorkerV1` is the default: it won both, and the batch worker
is kept for a caller that wants one batch's outcome in one transaction.

Both settings of `CrawlerWorkerV1` are aggressive, and 1000 fetches against one
host can outrun it: on that same local site 65 of the 4000 URLs exhausted the
12s fetch timeout, so those pages were rescheduled and their messages
dead-lettered. Pass a lower `max_concurrent_fetches`, and a longer
`flush_interval_seconds`, for a gentler crawl.

Add `--debug` for `DEBUG` logging instead of `INFO`:

```bash
python -m webcrawler.main --debug
```

Watch it work, every visited page and its links are logged:

```
2026-09-29 21:14:34 INFO webcrawler.application.worker: visited https://crawlme.monzo.com/index.html, found 10 link(s): [...]
```

With worker `2` the same line comes from `webcrawler.application.worker_v1`.

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
- **Two workers, one port.** `CrawlerWorker` waits for a whole batch and writes
  once at the end of it; `CrawlerWorkerV1` detaches a crawl per message, holds up
  to 1000 fetches in flight and writes the store every 10ms. `main.py` asks which
  one to build, and the orchestrator only ever calls `run` and `close`.
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
