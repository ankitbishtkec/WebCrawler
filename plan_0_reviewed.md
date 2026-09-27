# Web Crawler Implementation Plan

## 1. Requirements, clarifications, and ambiguities

### Explicit requirements from `goal.md`

- Implement a crawler in a language familiar to the implementer. Given a starting URL, visit URLs found on the same website, print every visited URL, and print the links found on each page (`goal.md:1`).
- Limit the crawl to one subdomain. For `https://crawlme.monzo.com/*`, do not follow `facebook.com`, `monzo.com`, or `community.monzo.com` (`goal.md:1`).
- Write the crawler behavior directly. Do not use crawler frameworks such as Scrapy or go-colly, or someone else’s crawler implementation. Libraries for HTML parsing are allowed (`goal.md:2`).
- Favor production-oriented structure, behavior, trade-offs, concurrency, and test coverage over UI or sitemap output (`goal.md:3`).
- Use composition over inheritance, SOLID principles, parameterized unit tests, interfaces for the database, queue, and major classes, and simple default implementations (`goal.md:8-11`).
- Put modules in separate folders, not separate services, and document the module structure in the README or comments (`goal.md:12`).
- Use an orchestrator and one `asyncio` event loop; use asynchronous APIs wherever possible (`goal.md:13-14`).
- Use the interface implementations rather than bypassing them. Every implementation that performs I/O owns retry, exponential backoff with jitter, and timeout behavior (`goal.md:16-17`).
- The requirements ask for concise two-line rationale comments before functions, classes, and non-obvious/eureka logic, with function arguments, return values, and possible exceptions documented (`goal.md:6-7`). They also prohibit comments on obvious code (`goal.md:15`). These instructions must be reconciled rather than silently choosing one.
- The URL-state table has `url` as its primary key, `createdTime`, `lastCrawlTime`, `nextCrawlTime`, `status`, and `lastStatusUpdateTime` (`goal.md:20-23`).
- New rows use the current datetime for the specified default timestamps, `nextCrawlTime` equals `createdTime`, and all times are UTC (`goal.md:25-27`).
- Index the URL and the combination of status, next crawl time, and last status update time (`goal.md:29-31`).
- A URL is crawlable according to the four status/time conditions in the supplied query, ordered by `next_crawl_time` ascending and limited by `max_items` (`goal.md:33-51`).
- The URL-state database has an asynchronous interface and an asynchronous SQLite implementation using `aiosqlite`; no application thread is required (`goal.md:53`).
- `CrawlQueuer` polls the database, defaults to polling every five seconds, accepts a configurable maximum item count defaulting to `-1` (no limit), uses bulk search/update operations, updates eligible rows to `QUEUED`, and appends only affected rows (`goal.md:55-61`).
- Document the database-success/queue-failure case as a CDC concern and handle it in the design by recovering stale `QUEUED` rows after `queue_timeout` (`goal.md:63-65`). CDC itself is not specified as an implementation deliverable.
- The supplied claim flow uses `BEGIN IMMEDIATE`, a conditional update, `RETURNING url`, and `COMMIT` (`goal.md:67-98`).
- Deduplicate repeated poll/API requests using a poll-check ID or API request ID; the API parameters are allowed to be added (`goal.md:100`). The exact keying and storage rules need clarification.
- The queuer interface exposes an API for comma-separated URL input and an API for on-demand candidate queuing with an optional maximum; the implementation must be used (`goal.md:102-106`).
- The poller must not run multiple instances, and its API calls must not race with polling. The goal states that the conditional database update prevents those races without `asyncio.Lock` (`goal.md:107-109`).
Ankit: the poller is not a API, rather a forever living polling loop which wakes up every configurable time period and fetches urls from DB and adds them to the queue.
- Queue items are partitioned by `hash(url)` (`goal.md:113`).
- The default queue is an in-memory, single-partition topic backed by `collections.deque`, with a configurable maximum size defaulting to 10,000 and an overflow exception when capacity would be exceeded (`goal.md:115-118`).
- The queue has asynchronous `TopicProducer` and `TopicReader` interfaces, supports topic creation, partition-key routing, bulk enqueue, reader connection with a no-op consumer group, `peek`, and `commit`, and keeps a random reader ID internal to the reader object (`goal.md:119-128`).
- The default single-partition implementation assigns the one partition to at most one reader and needs no redistribution (`goal.md:124-127`).
- `CrawlerWorker` provides `CustomURL`, which rejects invalid URLs, exposes hostname and URL parts, compares scheme plus hostname plus path plus query string, and reconstructs a standardized URL (`goal.md:130-132`).
- The worker uses queue readers and `peek`, transitions a URL to `startedCrawl` in a transaction, and uses a task group to process URLs (`goal.md:133-139`).
- `AbstractPolitenessPolicy` has a no-op default: zero means immediate execution and a supplied millisecond value means a delay (`goal.md:140`). The allowed range and whether the policy or worker supplies the value are not specified.
- `IWebPageFetcher` has an asynchronous fetch API and receives an `AbstractRetryPolicy` with exponential backoff, jitter, logging, and timeout behavior (`goal.md:141`).
- The worker handles absolute and relative `href` values, creates a unique set of full URLs, keeps only URLs pointing to the current page hostname, prints the discovered URLs, and uses the database interface (`goal.md:142-144`).
- The worker transaction updates the current URL to completed, inserts new URLs with `createdTime` as `nextCrawlTime`, and commits (`goal.md:145-148`).
- The worker calls `CrawlQueuer` immediately for discovered URLs and commits queue messages afterward (`goal.md:149-150`).
- ankit: All apis are async

### Clarifications already recorded in this plan

- Failure handling is not a separate feature for this exercise. Document that a failure leads to `FINISHED_CRAWL` rather than adding an unrequested failure state.
- `lastCrawlTime` means the last crawl attempt, regardless of success.
- Tests may use a time-provider abstraction such as `TimeProviderFactory` or controlled monkey patching.

### Open questions that must not be silently assumed

- The implementation language is not fixed. Python is a proposed choice because the goal names `asyncio` and `aiosqlite`; record the selected language before implementation.
Ankit: yes python
- Whether “same domain” means exact hostname or whether path prefixes, ports, schemes, fragments, and query normalization affect identity.
ankit: exact host name
- The final schema spelling: the prose uses names such as `createdTime`, while the SQL uses names such as `next_crawl_time`; status values are also spelled inconsistently.
ankit: follow python best practices in naming and pythonic coding for all naming and syntax
- The default status spelling and initial `lastCrawlTime` are unspecified. The value of `nextCrawlTime` after an attempt is unspecified; `lastCrawlTime` and `lastStatusUpdateTime` must be updated for every attempt, including failure, according to the recorded clarification.
ankit: initial `lastCrawlTime` is None or Null, `nextCrawlTime` after an attempt is configurable and defaults to none or null. `lastStatusUpdateTime` must be updated after every status change.
- Exact `CustomURL` parsing, standardization, exception type, and treatment of unsupported schemes, credentials, ports, fragments, and malformed links. The goal requires a library-based URL implementation rather than ad hoc string splitting.
ankit: if url string passed to constructor is invalid throw a invalid argument exception. 
- Whether `peek` is non-destructive or reserves messages, how `commit` identifies and acknowledges messages, whether fewer than the requested number are returned, and whether commits are partial or idempotent.
ankit: peek just reads the first N messages, min(N, len(queue)) . does not reserves them. Commit removes min(N, len(queue)). commits are not idempotent as we are not dealing with any idempotency key here
- Whether the comma-separated API inserts/claims URL-state rows before publishing, or publishes raw messages directly; how empty, malformed, duplicate, and externally scoped inputs are handled.
ankit: we will not use comma to delimit, rather pass urls as list of customURL types, Also if you note the insertion of the urls are already done in webcrwalerWorker before call this api. So, this api's job is to just fetch these urls if valid candidate and exists from db, change them to queue(the query is in goal.md) and push to queue.
- Whether bulk enqueue is atomic and how poll/API request IDs are stored and expired. The requirement to deduplicate should not be dropped merely because the API parameters are optional.
ankit: the bulk enqueue will not dedupe; as that is responsibilit of the caller. It will take a list of BaseMessage type and return a list for true/ false for each corresponding message.
- The exact CDC boundary: documentation only, a future mechanism, or an implementation requirement.
ankit: its a comment in read me and how we deal with it by using timeout for queued
- Worker count, task-group lifecycle, startup seeding, termination versus periodic recrawling, shutdown behavior, politeness scope, retry limits, timeout values, HTTP redirect policy, content-type handling, and output ordering.
ankit: worker is running as module in this code in async fashion and as single instance. It will asyncio.sleep if no element is present queue for configurable time, default to 1 sec. But it is extensible to work in multiple numbers and in prod; if we use the prod implementation of queue and other interfaces.
- Whether printing an attempted but failed fetch counts as printing a visited URL.
ankit: we will log that this url fetch has failed after these many retries and we are adding the url to the DB with next crwal time = currTime + 1 minute
- The precedence between `goal.md:6-7` and `goal.md:15`, and where the required no-lock queue rationale must be documented.
ankit: yes, in implementation file for sure. All apis are async.
- Whether SQLite is used by one process only or by multiple processes, and the required busy/locked transaction behavior.
ankit: single process that the WebCrawler we are using
- Which classes count as “major” for the interface requirement, including `CrawlerWorker`, `Orchestrator`, and `CustomURL`; document an interface or an explicit exemption for each.
ankit: all main modules and data type
feel free to use a better name:
dbRepository -> for porviding apis to make changes on DB
CrawlQueuer -> Poller resoider in this along with apis for on-demand poll and poll for certain urls
TopicProducer
TopicReader
BaseMessage -> represents the message in the queue
CustomUrl or URL
RetryPolicy
PolitenessPolicy
WebPageFetcher

We will surely not need interface for Orchestrator

I think we will not need interface for CrawlerWorker; but feel free to override on this, if we need this as per SOLID principals or for testing

## 2. Entities and responsibilities

- **`CustomURL`**: Construct from a string using a URL library, reject invalid input, expose hostname and URL components, compare by scheme, hostname, path, and query string, reconstruct a standardized URL, and support the unique-set requirement.
ankit: we will not need fragment in reconstructed url but query params are needed, scheme, host name and path too
- **URL table row**: Store the URL primary key and the fields listed in `goal.md:23`; preserve the specified status and timestamp meanings.
- **URL-state database interface**: Define schema initialization, URL insertion, atomic crawlable claiming, queued/started transitions, completion, and discovery insertion.
- **SQLite URL-state implementation**: Implement that interface with `aiosqlite`, the supplied SQL conditions, indexes, UTC timestamps, transactions, and the retry/timeout contract required for I/O implementations.
- **`CrawlQueuer` interface and implementation**: Poll periodically, perform bulk database operations, enqueue affected rows, expose both queuer APIs, and prevent multiple poller instances.
- **Topic queue**: Route messages by `hash(url)`, maintain one deque per partition, enforce capacity, and assign partitions to readers without assigning one partition to multiple readers.
- **`TopicProducer` and `TopicReader`**: Provide the asynchronous APIs described in `goal.md:119-128`, including automatic topic creation, partition-key routing, bulk enqueue, reader connection, `peek`, and `commit`.
- **`CrawlerWorker`**: Consume reader results, manage state transitions, invoke politeness and fetching policies, extract and filter links, print each page’s URL and links, persist completion, and invoke `CrawlQueuer`.
- **`AbstractPolitenessPolicy` and no-op implementation**: Decide whether execution is immediate or delayed by a supplied number of milliseconds.
- **`AbstractRetryPolicy` and I/O implementation**: Provide exponential backoff, jitter, timeout, and required fetch logging.
- **`IWebPageFetcher` and implementation**: Fetch page bodies asynchronously without embedding queue or database responsibilities.
- **HTML link extraction component**: Parse `href` values and resolve relative and absolute links. The goal permits a parsing library; no particular parser is mandated.
ankit: we will prefer a standard library so that a pip is not required
- **Orchestrator**: Construct dependencies, run all modules on one `asyncio` event loop, seed the starting URL through the defined path, and coordinate lifecycle according to the resolved termination/shutdown decisions.
- **Time provider test helper**: Use `TimeProviderFactory` or controlled monkey patching when needed to make time-dependent tests deterministic.

The names of supporting interfaces and data types are implementation choices. No lease token, failure record, path policy, or response-size policy is part of the minimal plan unless the open questions are resolved.

## 3. Minimal design and interfaces

```text
CustomURL
  constructor(url)
  hostname and URL component accessors
  get_url()
  equality: scheme + hostname + path + query string

URLStateRepository
  initialize()
  insert_or_ignore(urls, current_time)
  ankit CustomUrl list is passed and only unique set is inserted. 
  claim_crawlable(now, max_items, job_timeout, queue_timeout) -> urls
  update_to_started(url, now)
  complete_and_insert_discovered(url, discovered_urls, now)
  claim_stale_queued(now, queue_timeout) -> urls
  ankit: what does claim_stale_queued used for ?

CrawlQueuer
  poll_once(max_items=-1, poll_check_id=None)
  enqueue_csv_urls(csv_urls, request_id=None)
  ankit: why it is csv_urls ? there is no csv file ? we can pass list of customUrls
  queue_candidates(max_items=None, request_id=None)

TopicProducer
  enqueue(message, partition_key)
  enqueue_many(messages)
  ankit: use BaseMessage and its list, enqueue_many is missing partition_key for each message

TopicReader
  connect(topic, consumer_group)
  peek(item_count)
  ankit: returns BaseMessage List
  commit(items)
  ankit: takes list of BaseMessage

AbstractPolitenessPolicy
  before_fetch(delay_ms)

IWebPageFetcher
  fetch(url, retry_policy)

ankit: missing AbstractRetryPolicy
```

- Interface names and exact Python signatures are not specified by `goal.md`; the methods above are a minimal proposed shape for the required operations, not additional behavior requirements.
- Classify `CrawlerWorker`, `Orchestrator`, and `CustomURL` as major or non-major; if major, define their interfaces, otherwise document the reason they do not need separate interfaces.
ankit: already answered above
- Use asynchronous APIs for every interface where feasible, including database, queue, fetch, and orchestration operations. Document any unavoidable synchronous boundary.
- Use composition: construct a worker with its URL, database, reader, politeness, fetcher, and queuer dependencies rather than inheriting from framework classes.
ankit: URL is not needed. read from queue will take care of getting url to fetch
- Keep modules in separate folders and document the module layout in the README or concise design comments.
- Use the database interface in both `CrawlQueuer` and `CrawlerWorker`; do not issue worker or queuer SQL directly.
- Use the queue interfaces rather than accessing the deque from outside the queue implementation.
- Keep the repository’s production claim operation atomic: one `claim_crawlable` operation should encapsulate `BEGIN IMMEDIATE`, the supplied conditional `UPDATE ... RETURNING url`, and `COMMIT`. A separate read-only selection method must not be the production handoff.
- Implement the four supplied crawlability conditions, ordering, and `LIMIT` behavior. Reconcile the apparent conflict between excluding fresh `QUEUED`/`STARTED_CRAWL` rows and including timed-out rows before finalizing the claim predicate.
- For a newly inserted URL, set `createdTime` and `nextCrawlTime` to the same UTC value and use the specified default status after its spelling is resolved.
- Make the database update and queue handoff ordering explicit, document the CDC observation, and recover stale `QUEUED` rows through the specified timeout.
- `TopicProducer.enqueue` and `enqueue_many` must create a missing topic with one partition by default and route each message using `partition_key % partition_count`; an explicit topic-creation method may also be provided.
- The default queue uses a single partition and an in-memory deque with a 10,000-item default limit. Document why no lock is needed under the stated single-event-loop, single-reader model.
- The queue reader generates and stores its own random reader ID; callers do not pass that ID.
- The worker starts the `STARTED_CRAWL` transaction, updates the status and last-status-update time, and commits before fetch, parse, or print work for that URL.
- The completion transaction updates the current URL, records `lastCrawlTime` and `lastStatusUpdateTime` for the attempt, inserts newly discovered URLs with `createdTime` as `nextCrawlTime`, and commits before immediate queuer handoff.
- The worker uses a no-op politeness policy by default and passes a retry policy to the webpage fetcher.
- Keep retry, backoff, jitter, and timeout inside the implementation that performs the relevant I/O. Logging is explicitly required for webpage fetching; whether other I/O implementations must log is not specified.
- The exact meaning of `peek` and `commit`, malformed/non-HTML response behavior, startup seeding, termination, and shutdown remain implementation decisions until the open questions are answered.
- ankit: use import logging for logging with log type in every major class. log.debug for debug logs and logs.info for others. we can pass this as config when starting orchestrator. 

## 4. Genuine extension points

- **Database interface**: SQLite with `aiosqlite` is the required default; another implementation can replace it without changing the queuer or worker.
- **Topic producer/reader interfaces**: The in-memory deque topic is the required default; another queue can satisfy the same interfaces.
- **Politeness policy**: Replace the required no-op implementation with a delay implementation.
- **Retry policy and webpage fetcher**: Supply alternate retry strategies or HTTP clients through the required interfaces.
- **HTML parser**: Use a permitted parsing library or the standard library for `href` extraction.
- **Time provider for tests**: Use `TimeProviderFactory` or controlled monkey patching to make timeout and polling tests deterministic.
- **CDC or an outbox**: The goal calls for documenting the database/queue handoff concern and using `QUEUED` timeout recovery; a durable CDC/outbox implementation remains an extension unless explicitly requested.
ankit: prefer factory over monkey patching
- Do not introduce leases, path-prefix policies, registrable-domain policies, response-size policies, plugin frameworks, or multiple service processes without an explicit requirement change.

## 5. Edge cases grounded in the requirements

- Invalid URLs passed to `CustomURL`, including malformed strings and unsupported URL forms whose expected exception type must be decided.
- Absolute, relative, and duplicate `href` values, including links that resolve to the current hostname and links that do not.
- Hostnames that resemble the starting host but differ, including parent domains, sibling subdomains, and unrelated domains.
- Cycles and repeated links, which must not create duplicate entries in the unique URL set or repeated database insertion.
- A normal completed traversal must not terminate while an already discovered in-scope URL remains unprocessed; the exact quiescence/termination rule and periodic-recrawl behavior remain open.
ankit: the traversal never terminates; it starts everything and asks for a seed url via console input; once it is provided it adds the url to the db. and infinitely keep asking the user for next seed url. It has to be killed via cntl + c
- Equality and reconstruction cases for scheme, hostname, path, and query string; port, fragment, case, and query-order behavior are unspecified and require a decision before implementation.
ankit: a url is made up of scheme+hostname+path+querystingSortedByKeyStringValueString
note fragment is ignored
- New-row timestamp equality, UTC storage, default status spelling, initial `lastCrawlTime`, and the unresolved post-attempt `nextCrawlTime`; every attempt must update `lastCrawlTime` and `lastStatusUpdateTime`.
- Each of the four crawlability conditions, exact timeout boundaries, ordering, `max_items=-1`, and the stale-row eligibility conflict.
- Empty results, conditional updates affecting no rows, transaction rollback, SQLite locking, and any chosen polling/cancellation behavior.
- Stale `STARTED_CRAWL` and `QUEUED` rows, database success followed by queue failure, duplicate poll/API requests, and request-ID expiry.
- Queue size exactly at the configured maximum, overflow, bulk enqueue, automatic topic creation, modulo partition routing, reader assignment, no-op consumer groups, and internal reader IDs.
- Worker state transitions, zero and nonzero politeness values, fetch timeout, retry exhaustion, and fetch logging.
- Task-group cancellation, startup seeding, termination versus recrawling, and shutdown behavior, all of which need explicit decisions.
- Printing the visited URL and the complete list of links for each page, including whether a failed fetch counts as visited. Duplicate links on one page are collapsed before printing by the unique-set requirement; a link appearing on multiple pages appears in each page’s printed list, while cross-page output ordering and global deduplication remain unspecified.

## 6. Pytest cases

- Every unit test must use `pytest.mark.parametrize`, including behavior with a single case.
- Parameterize `CustomURL` construction, component access, invalid input, URL reconstruction, and equality over scheme, hostname, path, and query string.
- Parameterize same-host filtering with `crawlme.monzo.com`, `facebook.com`, `monzo.com`, and `community.monzo.com` cases from `goal.md:1`.
- Parameterize absolute and relative `href` resolution, duplicate links, cycles, and links outside the current hostname.
- Verify new rows have equal `createdTime` and `nextCrawlTime`, UTC values, the selected default status, and the selected default `lastStatusUpdateTime`; the initial `lastCrawlTime` remains an explicit test decision.
- Parameterize every condition in the supplied crawlability query, ordering, `max_items`, empty results, and `max_items=-1` meaning no limit.
- Verify the atomic claim operation updates only eligible rows, returns/enqueues only affected rows, and follows the selected stale-row policy.
- Verify both queuer APIs, periodic polling with the five-second default, on-demand `max_items=None` behavior, the selected comma-separated input path, and that the selected lifecycle mechanism permits only one active poller instance.
- Exercise concurrent poller/API calls against the same database state and verify the conditional transition prevents duplicate claims as intended by `goal.md:107-109`.
- Verify queue capacity, overflow, automatic topic creation, `partition_key % partition_count`, bulk enqueue, one-partition reader assignment, consumer-group no-op behavior, internal reader IDs, and the selected `peek`/`commit` semantics.
- Verify database success followed by queue failure leaves a recoverable `QUEUED` condition according to the documented timeout strategy.
- Verify the worker transitions `QUEUED` to the selected canonical started status and commits before fetching, then updates the URL, records `lastCrawlTime` and `lastStatusUpdateTime` for the attempt, inserts discoveries transactionally, and commits before queuer handoff.
- Verify `lastCrawlTime` records every crawl attempt regardless of success, matching the clarification recorded in this plan; verify the documented `FINISHED_CRAWL` behavior for failure.
- Parameterize politeness values and verify zero/immediate behavior plus the selected nonzero-delay behavior.
- Use fakes to verify retry, backoff, jitter, timeout, and fetch logging for webpage I/O without asserting an algorithm or bound that `goal.md` does not define; separately verify the selected SQLite I/O retry/timeout contract.
- Use fake fetcher/parser implementations for integration tests covering branching pages, cycles, external links, database updates, and queue handoff.
- Add concurrency tests for the task group, poller/API transitions, and queue behavior only after `peek`/`commit` and worker-count semantics are selected.
- Treat the async pytest plugin, lint tool, and type-check tool as plan-local choices; none is specified by `goal.md` or currently present in the repository.

## 7. Concurrency model

- Run the orchestrator and all modules on one `asyncio` event loop (`goal.md:13`).
- Do not create an application-managed thread for the URL-state database; use the asynchronous database interface and `aiosqlite` implementation (`goal.md:53`).
- Let the conditional database update protect transitions to `QUEUED`; the goal explicitly states that `asyncio.Lock` is not required for the poller/API race (`goal.md:107-109`), subject to resolving the stale-row eligibility conflict.
- Prevent multiple poller instances by construction or lifecycle control; the exact mechanism is not specified.
- Use the default single-partition queue with one assigned reader. Document the stated no-lock rationale for the deque under the single-event-loop, single-reader model (`goal.md:116-128`).
- Use the worker task group as requested, but do not choose a worker count or shutdown policy until those behaviors are specified (`goal.md:133-139`).
- Keep the no-op `AbstractPolitenessPolicy` as the default; its supplied delay unit is milliseconds (`goal.md:140`).
- Ensure each I/O implementation owns timeout, retry, exponential backoff, and jitter as required by `goal.md:17`; webpage fetching additionally owns logging (`goal.md:141`). The exact retry contract for SQLite I/O remains to be resolved.
- Treat database update and queue enqueue as separate operations. Document the CDC concern and use the specified stale `QUEUED` timeout for recovery (`goal.md:63-65`).
- Do not add a claim token, semaphore, per-host politeness coordinator, or unrequested shutdown behavior to the baseline plan.
- Define startup seeding, task cancellation, event-loop ownership, and database/queue cleanup before treating the orchestrator lifecycle as complete.

## 8. Implementation order

1. Record the selected implementation language and final decisions for the open questions, especially schema/status spelling, URL identity, stale-row eligibility, `peek`/`commit`, CSV API behavior, retry parameters/contracts, and the comment policy. The requirement that every I/O implementation owns timeout, retry, exponential backoff, and jitter is not optional; only its concrete parameters and SQLite-specific contract need decisions. Do not encode unresolved behavior as an implicit assumption.
2. Establish the separate module folders, public interfaces, dependency injection points, and README/comment strategy required by `goal.md:6-17`, including concise two-line rationale/signature documentation and the required no-lock queue comment.
3. Implement `CustomURL` and focused parameterized tests for library-based construction, accessors, equality, reconstruction, absolute/relative resolution, and hostname filtering.
4. Define the URL-state table and `URLStateRepository` interface; implement the `aiosqlite` schema, indexes, UTC timestamps, supplied crawlability query, atomic claim transaction, and state updates.
5. Implement `TopicProducer`, `TopicReader`, and the single-partition in-memory deque; test capacity, overflow, automatic topic creation, modulo routing, reader assignment, and the selected queue semantics.
6. Implement the no-op politeness policy, retry policy, webpage fetcher interface/implementation, and HTML link extraction with deterministic fakes.
7. Implement `CrawlQueuer`, including periodic polling, both required APIs, deduplication, bulk updates, affected-row enqueueing, and stale `QUEUED` recovery.
8. Implement `CrawlerWorker`, including the started-state commit boundary, task-group execution, fetching, link filtering, printing both the page URL and links, transactional discovery insertion, and immediate queuer handoff.
9. Implement the single-event-loop orchestrator, startup seeding, lifecycle wiring, termination behavior, and module documentation. Preserve the recorded clarification that terminal failure transitions the attempted URL to `FINISHED_CRAWL`.
10. Add parameterized unit tests, async integration tests, and concurrency tests; then run the project’s pytest, lint, and type-check commands after those project-local tools are selected.

Each stage is a reviewable implementation checkpoint. The plan should be committed only after the repository has a valid Git repository and commits are authorized.
