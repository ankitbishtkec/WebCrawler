# Goal

We'd like you to write a simple web crawler in a programming language you're familiar with. Given a starting URL, the crawler should visit each URL it finds on the same domain. It should print each URL visited, and a list of links found on that page. The crawler should be limited to one subdomain - so when you start with *\**[*https://crawlme.monzo.com/\**](https://crawlme.monzo.com/*), it would crawl all pages on the crawlme.monzo.com website, but not follow external links, for example to facebook.com, monzo.com or community.monzo.com.

We would like to see your own implementation of a web crawler. Please do not use frameworks like scrapy or go-colly which handle all the crawling behind the scenes or someone else's code. You are welcome to use libraries to handle things like HTML parsing.

Ideally, write it as you would a production piece of code. This exercise is not meant to show us whether you can write code – we are more interested in how you design software. This means that we care less about a fancy UI or sitemap format, and more about how your program is structured: the trade-offs you've made, what behaviour the program exhibits, and your use of concurrency, test coverage, and so on.

## Coding Requirements

- Add 2 line concise and non-verbose comment before every eureka logic with reasoning, and before every function def and class
- func def should clearly add args def, return def and possible exceptions, basically complete signature
- use composition over inheritance
- UTs must be parameterized
- use SOLID principal
- DBs, queue and all major classes must be have interfaced and implementation should be a default on and simple one, db can be a simple sqlite, queue can be in-mem self written one etc
- Since we want to should prod level scalable design instead of overdoing by creating service for each module, I will use modules for each. Modules should be in seperate folders. Add this in readme or comments.
- Orcehestrator module runs all module when called, we use a single ayncio event loop for all
- give asyc apis for all interfaces whereever possible using asycnio
- Add concise comments only for non-obvious design decisions, concurrency invariants, race-condition avoidance, and important trade-offs. Do not comment obvious code.
- Must use interface's implementation
- All modules if making i/o calls in implementation must own the retry with exponential backoff with jitter and timeout

# Design

## module urlState DB library

has

### Url Table

- url(Primary key)
- createdTime
- lastCrawlTime
- nextCrawlTime default currentDATETIME
- status(Not_crawled(default), queued, startedCrawl, FinishedCrawl)
- lastStatusUpdateTime default currentDATETIME
- all time are in utc
- nextCrawlTime must be equal to createdTime when a new item is inserted

### index:

- url
- status, nextCrawlTime, lastStatusUpdateTime

### we should crawl only if:

```sql
SELECT url
FROM url_state
WHERE
    status = 'NOT_CRAWLED'
    OR (
        status = 'FINISHED_CRAWL'
        AND next_crawl_time <= :now
    )
    OR (
        status = 'STARTED_CRAWL'
        AND :now - last_status_update_time >= :job_timeout
    )
    OR (
        status = 'QUEUED'
        AND :now - last_status_update_time >= :queue_timeout
    )
ORDER BY next_crawl_time ASC
LIMIT :max_items;
```

basically no thread needed here only a DB interface and its sqlite implementation in asyn manner using aiosqlite.

## Module CrawlQueuer

Polls the url state db

### Config based:

- periodicFetchTime: default every 5 second.
- MaxItemsToQueue: Can be a passed to make query in db using limit default is -1 i.e. no limit
- it should use bulk operations to search and update to queued. may be trasactions. First update to queued status in the table and check if not queued or startedCrawl. Find the names of impacted rows and then only append those only.

Bascically add a comment that we will use cdc for the case where the the db update works but sending to queue does not happen. Here we are handling by putting a timeout on:

```sql
status = 'QUEUED'
AND :now - last_status_update_time >= :queue_timeout
```

```sql
BEGIN IMMEDIATE;
UPDATE url_state
SET
    status = 'QUEUED',
    last_status_update_time = :now
WHERE url IN (
    SELECT url
    FROM url_state
    WHERE
        (
            status = 'NOT_CRAWLED'
            OR (
                status = 'FINISHED_CRAWL'
                AND next_crawl_time <= :now
            )
            OR (
                status = 'STARTED_CRAWL'
                AND :now - last_status_update_time >= :job_timeout
            )
            OR (
                status = 'QUEUED'
                AND :now - last_status_update_time >= :queue_timeout
            )
        )
        AND url IN (:url_1, :url_2, :url_3, ...)
    ORDER BY next_crawl_time ASC
    LIMIT :max_items
)
RETURNING url;
COMMIT;
```

Dedupe by poll check id or api request id so that we do not keep adding more and more to the queue on retry. Feel free to add these param to the api

### API:

- a- add a comma seperated urls input to queue these url to the queue.
- b- add a api to queue candidate items on demand, with the maxItems provided, if None use the internal constants for limit

built an interface too which shows above api, implement the interface

poller should not run multi instaces to avoid race conditions. the api call should not race with poller run.

Also note that `asyncio.Lock` is not required, since the database update conditionally selects/targets only items that are not already `QUEUED` or `STARTED_CRAWL`, preventing race conditions across transactions when transitioning them to `QUEUED`.

When enqueued an item to the queue partition by hash(url)

## module queue->

simple in-Mem based implementation built on collections.deque, one deque per topic: no partitions, no connected-reader registration, no readerId bookkeeping in this implementation.

to a configurable max size default to 10k. throws overflow exception if enqueue makes more than 10k elements. keep the overflow exception on enqueue.

lock is not needed as this is cpu bound and running on a single event loop, and the shipped run path has exactly one reader. Add in comment.

should have 2 interface which are async. TopicProducer, TopicReader

keep the same params as a real prod queue for future proofing (topic, consumer group id, partition key on the message): this implementation accepts them and ignores them — one deque per topic, no partition routing, no client id. A real prod queue (e.g. kafka-like) would use them unchanged.

### TopicReader

- Connect a reader to a topic and consumer group. interface should have consumer group but for implementation its no-op. connect returns [0], a single virtual partition, so the worker's assignment contract is unchanged.
- peek given number of items, min(N, len), does not reserve them
- commit given items, removes min(N, len) from the head, not idempotent

### TopicProducer

- enqueue a message to the topic. Should create a new topic, if not exist. The partition key stays on the message for a future prod implementation; it is ignored here.
- bulk enqueue api, one true/false result per message, false on overflow, and the rest are still enqueued.

## module CrawlerWorker:

Has CustomURL class based on lib url, url string is passed in to construction, hostName and different part of url are accessible by methods. contructor throws exception if the passed string is not proper url, **__equal__** checks the scheme + hostName + path + queryString being same

getUrl function return a statndardly re-created url from the elements

use concurrency by creating a queueReader objects.

await peeks queueReader.

start trasacation

update the status of url to startCrwal with lastUpdate time

commit transaction

for the url use task group and await for to complete. Do following.

uses the AbstractPolitenessPolicy with no-op implementation to decide when to call, 0 means should call now, or a number means call after given milliseconds

once this is passed we call the IWebPageFetcher having async api to fetch the body while passing AbstarctRetryPolicy with exponential backkoff and jitter with logging and also timeout

We parse the webpage for href and handle both absolute and relative url. Make FULL unique urls set. use the webpage host name for create the full url and also remove all the urls which not pointing to the webpage host name. We only want urls pointing to webpage hostname

Print those.

Use implemented interface for db operations here

start transcation

bulk update to table that given url status to crawl completed. And time.

Insert if not existing new found urls with createdTime as nextCrwaltime.

commit transaction

CrawlQueuer should be called for the given urls to queue them right now

commit messages in queues
