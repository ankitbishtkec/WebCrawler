"""The console logger: one stdlib stream handler, set up once at startup.

Every major class takes a `logging.Logger`: `log.debug` for detail, `log.info` only for a
fetched URL and its found links, `log.warning`/`log.error` for a degraded or failed step.
`main.py` calls `configure_logging` first.
"""

import logging
import time

LOG_FORMAT: str = "%(asctime)s %(levelname)s %(name)s: %(message)s"
DATE_FORMAT: str = "%Y-%m-%d %H:%M:%S"
ROOT_LOGGER_NAME: str = "webcrawler"
HANDLER_NAME: str = "webcrawler-console"

def configure_logging(level: int = logging.INFO) -> logging.Logger:
    """Send the project logger to the console at the given level.

    `level` is the only filter, chosen here rather than at each call site.

    Args:
    level: The lowest level a record must have to be printed.

    Returns:
    logging.Logger: The `webcrawler` logger, ready to be injected.
    """
    project = logging.getLogger(ROOT_LOGGER_NAME)
    project.setLevel(level)
    # The handler is added once and never replaced, so a second call only moves the level.
    if not any(handler.get_name() == HANDLER_NAME for handler in project.handlers):
        print(f"Logging to console at level {logging.getLevelName(level)}")
        handler = logging.StreamHandler()
        handler.set_name(HANDLER_NAME)
        formatter = logging.Formatter(LOG_FORMAT, DATE_FORMAT)
        # Every time here is UTC; converter is the documented attribute for the
        # clock a Formatter renders with.
        formatter.converter = time.gmtime
        handler.setFormatter(formatter)
        project.addHandler(handler)
    return project
