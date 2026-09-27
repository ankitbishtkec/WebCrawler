# Project memory




## Comments

- Keep them short and precise.
- Do cover signaure of function with one line on each input param and output return.
- **Never be too verbose.**
- **After every code change, change the plan.md and the comments to reflect code correctly.**

## Style

- **Do not over-engineer code**

## Verification

- delete webcrawler.db file
- run python -m webcrawler.main and pass https://crawlme.monzo.com/index.html as seed
- run >python -m sqlite_utils query webcrawler.db "select state, count(*) as n, min(last_status_update_time) as oldest_status, max(last_status_update_time) as newest_status, min(last_crawl_time) as first_crawl, max(last_crawl_time) as last_crawl, min(times_crawled), max(times_crawled) from urls group by state" --table
- keep looking for change in different state groups, their min and max of last status update time,  last crwal time and times crwaled
- also see a random sample of rows in the db
- also cntrl+c the python process sometimes and see the logs on console or file from the process. Use debug mode if needed.