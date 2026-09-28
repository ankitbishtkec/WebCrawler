# Project memory




## Comments

- Keep them short and precise.
- Do cover signaure of function with one line on each input param and output return.
- **Never be too verbose.**
- **After every code change, change the comments of ONLY IMPACTED FILES OR FILES USING THE GIVEN CLASSES to reflect code correctly.**

## Style

- **Do not over-engineer code**

## Ports

- The port files (`src/webcrawler/ports/`) are designed for a prod system, so they are ground truth for what a networked extension may need: a networked queue, store, or fetcher is a legal implementation.
- The shipped implementations are the ground truth for the dependencies we actually use today, so an in-memory queue that cannot fail must not be treated as if it can.
- A port must document the failures a real implementation can raise in its `Raises:`, even when the shipped one never does.
- A caller may guard a port call against a production failure, but the guard must be justified by the port's own contract, not by a guess.
- Do not add a parameter, method, or guard for an extension nobody asked for.

## Verification

- Kill exisiting process using the db file and then delete webcrawler.db file
- run python -m webcrawler.main and pass https://crawlme.monzo.com/index.html as seed
- run >python -m sqlite_utils query webcrawler.db "select state, count(*) as n, min(last_status_update_time) as oldest_status, max(last_status_update_time) as newest_status, min(last_crawl_time) as first_crawl, max(last_crawl_time) as last_crawl, min(times_crawled), max(times_crawled) from urls group by state" --table
- keep looking for change in different state groups, their min and max of last status update time,  last crwal time and times crwaled
- also see a random sample of rows in the db
- also cntrl+c the python process sometimes and see the logs on console or file from the process. Use debug mode if needed.

-  Kill process running webcrawler
- Delete any db files or logs files used for verifcation. Keep the working directory clean.