# Code review comments

Comments left in the source as `# ankit:` / `#ankit:`, collected here with the
file and line they were found at, and the action taken for each.

Line numbers are the positions **at the time of the review**; the fixes below
move code around, so search by the comment text if a line no longer matches.

Status values: `open` — not yet addressed, `done` — addressed in code,
`stale` — the comment describes a problem that a previous change already
solved.

## Summary

| # | File | Line | Topic | Status |
|---|------|------|-------|--------|
| 1 | `src/webcrawler/application/wiring.py` | 22 | Politeness over-engineered, keep only the no-op policy | done |
| 2 | `src/webcrawler/infrastructure/console/stdin_seed_source.py` | 15 | Seed input too complex, use a simple blocking `input()` | done |
| 3 | `src/webcrawler/infrastructure/fetch/auth_middleware.py` | 14 | Auth middleware not needed, move to README extensions | done |
| 4 | `src/webcrawler/infrastructure/fetch/auth_middleware.py` | 48 | Auth middleware not needed, move to README extensions | done |
| 5 | `src/webcrawler/infrastructure/fetch/headers_middleware.py` | 13 | leetcode returns 503, needs a Chrome User-Agent | done |
| 6 | `src/webcrawler/infrastructure/queue/topic_registry.py` | 29 | Queue too complex, single topic and single worker | done |
| 7 | `src/webcrawler/main.py` | 25 | Module comment too verbose | done |
| 8 | `src/webcrawler/main.py` | 59 | Add one line saying what the constants do | done |
| 9 | `src/webcrawler/main.py` | 97 | Pass the log level explicitly as INFO | done |
| 10 | `src/webcrawler/main.py` | 102 | `TopicRegistry` name too vague, rename | done |
| 11 | `src/webcrawler/main.py` | 114 | Pass a `HeadersMiddleware` | done |
| 12 | `src/webcrawler/ports/retry_policy.py` | 17 | Policy ignores whether the failure is retryable (503) | done |
| 13 | `src/webcrawler/ports/topic.py` | 19 | Should be a separate file | done |
| 14 | `src/webcrawler/ports/topic.py` | 29 | `connect` not required and not in `goal.md` | done |
| 15 | `src/webcrawler/ports/topic.py` | 91 | Should be a separate file | done |
| 16 | `src/webcrawler/ports/topic.py` | 103 | `connect` not required and not in `goal.md` | done |
| 17 | `src/webcrawler/utils/ttl_deduplicator.py` | 31 | Dedupe too complex, a repeated id is never entertained again | done |

All 17 comments are addressed. One bug was found while verifying item 9
and fixed with a regression test:

| # | Where | Problem | Action |
|---|-------|---------|--------|
| 18 | `src/webcrawler/main.py` | Passing `level=logging.INFO` (item 9) used `logging` without importing it, so `main()` raised `NameError` on the first line. No unit test caught it: they only asserted the module imports. | Added `import logging`, and `test_main_runs_with_every_name_it_uses_resolved`, which executes `main()` with the console read stubbed so an unresolved name inside the body fails the suite. Verified it fails when the import is removed. |

## Details

### 1. `src/webcrawler/application/wiring.py:22`

> ankit: this part of goal is totally misunderstood. before making even single
> http call at first attempt or nthe attempt
> we check this policy for the url. it returns a timeInMs for wait. if this
> return is less the then threshold than the worker sleeps if more work says
> crwal finsihed and update the next crawl time
> here is it doing something different and creating 2 policies. i need only 1
> no-op policy for demo
> we can suggest complex policies in 'extensions' sections read me

**Action:** removed `application/wiring.py` and
`MillisecondDelayPolitenessPolicy` entirely. `NoOpPolitenessPolicy` is the only
policy shipped, and `main.py` constructs it directly. The worker keeps the
`wait_ms <= sleep_threshold_ms` sleep branch and the above-threshold skip branch
because they are what any injected policy drives; the README's new
**Extensions** section describes how a delaying policy would plug in. The
`build_politeness_policy` guard and `tests/test_wiring.py` went with it, so the
threshold no longer has to be passed to two places.

### 2. `src/webcrawler/infrastructure/console/stdin_seed_source.py:15`

> ankit: this is too complex. we can take a simple blocking input before worker
> and poller starts and also inform to press cntrl+c to exit anytime

**Action:** deleted `infrastructure/console/` and `ports/seed_source.py`.
`Orchestrator.run()` now calls blocking `input()` **before** it creates the
poller and worker tasks, logs the seed, and tells the operator that `Ctrl+C`
exits. No thread, no generator, no port.

### 3. `src/webcrawler/infrastructure/fetch/auth_middleware.py:14`

> ankit: not needed remove. Add this is 'extension' section of readme

**Action:** deleted `BasicAuthMiddleware`; the README **Extensions** section
now describes adding HTTP Basic auth as a `RequestMiddleware` when a target
needs it.

### 4. `src/webcrawler/infrastructure/fetch/auth_middleware.py:48`

> ankit: not needed remove. Add this is 'extension' section of readme

**Action:** deleted `BearerAuthMiddleware`; the README **Extensions** section
now describes adding a bearer token the same way.

### 5. `src/webcrawler/infrastructure/fetch/headers_middleware.py:13`

> ankit: I see in many websites like leetcode it is returning http 503 may be a
> chrome user agent header

**Action:** the `User-Agent` is now a Chrome string instead of `webcrawler/0.1`,
so a bot-blocked site that answers 503 to a non-browser agent is not refused.
It lives in `main.py` as `USER_AGENT` and is injected through
`HeadersMiddleware`, the only source of request headers — the fetcher adds
none of its own.

### 6. `src/webcrawler/infrastructure/queue/topic_registry.py:29`

> ankit: too complex, make a simple one, we know there would be single topic and
> single worker and no need to keep the connected readers

**Action:** the queue is now the smallest thing that serves one topic and one
worker: a single `deque` with three methods (`enqueue`, `peek`, `commit`) and
no wiring step. The per-topic `dict`, `ensure_topic`, the `max_size` property,
and the `_head` helper are gone; `topic` and `consumer_group_id` are recorded
by the adapters and never consulted, exactly like `partition_key`. There is
nothing to create, so no adapter constructor calls into the queue and there is
no `connect` anywhere. The file was renamed in the meantime (see item 10).

### 7. `src/webcrawler/main.py:25`

> ankit: too verbose comment

**Action:** the module docstring is now a short summary plus the three wiring
decisions that matter, instead of a long narrative.

### 8. `src/webcrawler/main.py:59`

> ankit: add one line on what these constants do

**Action:** one line above the constants block states that they are the values
a deployment overrides, all of them constructor arguments of the collaborators
below.

### 9. `src/webcrawler/main.py:97`

> ankit: explicitly pass log level as info

**Action:** `configure_logging(level=logging.INFO)` — the level is passed
explicitly rather than relying on the default. (The `name` parameter was later
dropped; `main.py` no longer defines a `LOG_NAME`.)

### 10. `src/webcrawler/main.py:102`

> ankit: name topicRegistry is too vague; just make it
> InMemorySingleTopicSinglePartitionQueue

**Action:** `TopicRegistry` is renamed `InMemorySingleTopicSinglePartitionQueue`
and its module is renamed to match
(`infrastructure/queue/in_memory_single_topic_single_partition_queue.py`).

### 11. `src/webcrawler/main.py:114`

> ankit: pass headersMiddleware

**Action:** `main.py` builds `request_middlewares =
(HeadersMiddleware({"User-Agent": USER_AGENT}),)` and injects it into
`AiohttpWebPageFetcher`, so the shipped run path exercises the middleware
seam rather than leaving it empty.

### 12. `src/webcrawler/ports/retry_policy.py:17`

> ankit: does not takes care of when to retry and when not may be http 503 comes
> and we do not want to retry
> i think the should retry must take a result from the operation

**Action:** added `domain/errors.py::NonRetryableError`. The operation decides:
`AiohttpWebPageFetcher` raises `NonRetryableError` for any non-2xx status —
including the 503 from a bot-blocked site — while transport errors and
timeouts stay retryable, and `ExponentialBackoffRetryPolicy` re-raises a
`NonRetryableError` on the first attempt without spending a backoff.

### 13. `src/webcrawler/ports/topic.py:19`

> ankit: should be seperate file

**Action:** `ports/topic_producer.py` now holds `TopicProducer` on its own.

### 14. `src/webcrawler/ports/topic.py:29`

> ankit: connect is not required and also was not in goal.md. we assume that
> constructor will have all the connection and required param

**Action:** `TopicProducer.connect` is gone. `InMemoryTopicProducer`'s
constructor creates the topic, and `main.py` no longer awaits a connect.

### 15. `src/webcrawler/ports/topic.py:91`

> ankit: should be seperate file

**Action:** `ports/topic_reader.py` now holds `TopicReader` on its own.

### 16. `src/webcrawler/ports/topic.py:103`

> ankit: connect is not required and also was not in goal.md. we assume that
> constructor will have all the connection and required param

**Action:** `TopicReader.connect` is gone, so `CrawlerWorker.run()` no longer
connects or checks an assignment before peeking; the topic is read directly.

### 17. `src/webcrawler/utils/ttl_deduplicator.py:31`

> ankit: too complex make a simple one if request_id is repeated ever do not
> entertain. do not keep this.
> we can add the future changes in the "extension" section of the readme

**Action:** the TTL and its clock are gone. The class is now
`InMemoryRequestIdDeduplicator` in `utils/in_memory_request_id_deduplicator.py`:
a plain `set` where a repeated id is never entertained again and nothing
expires. The README **Extensions** section notes bounded-memory eviction
(sized set, or a TTL) as the thing to add if a crawl runs long enough for the
set to matter.
