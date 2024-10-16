from __future__ import annotations

import logging
import os
from datetime import datetime, timezone

DEFAULT_LOG_LEVEL = logging.INFO
DEFAULT_LOG_DIR = "e2c_logs"
DEFAULT_LOGPATH = f"{DEFAULT_LOG_DIR}/e2c_root_log-{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S_%f')}.log"

# Logger message and date formats.
MSGFMT = "%(asctime)s_%(msecs)03d:%(levelname)s:%(name)s:%(funcName)s:%(message)s"
DATEFMT = "%Y%m%d_%H%M%S"


def _logger(
    name: str | None = None,
    log_filename: str = DEFAULT_LOGPATH,
    log_level: int = DEFAULT_LOG_LEVEL,
    to_console: bool = False,
    to_logfile: bool = False,
    propagate: bool = False,
):
    """Return a root or named logger with variable configuration.

    Parameters
    ----------
    name : str | None
        The name displayed for the logger in messages.
        If name == None or name == "__main__", the root logger is returned
    log_filename : str
        If logfile handling is requested, any logfile may be specified.
    log_level : LogLevel
        Either logging.DEBUG (10), logging.INFO (20), logging.WARNING (30),
        logging.ERROR (40), logging.CRITICAL (50), by default logging.INFO.
    to_console : boolean
        If True, a logging.StreamHandler is supplied, by default False.
    to_logfile : boolean
        If True, a logging.FileHandler is supplied, by default False.
    propagate : boolean
        If True, messages logged are propagated to the root logger, by default
        False.
    """
    if to_logfile:
        dn = os.path.dirname(log_filename)
        if len(dn) and not os.path.exists(dn):
            os.makedirs(dn)

    if name is None or name == "__main__":
        # FIXME: F821 Undefined name `logger`
        logger = logger.root  # noqa: F821
    else:
        logger = logging.getLogger(name)

    logger.propagate = propagate
    logger.setLevel(log_level)

    logger.handlers = []

    if to_console:
        logStreamHandler = logging.StreamHandler()
        logStreamHandler.setFormatter(logging.Formatter(MSGFMT, datefmt=DATEFMT))
        logger.addHandler(logStreamHandler)

    if to_logfile:
        logFileHandler = logging.FileHandler(log_filename)
        logFileHandler.setFormatter(logging.Formatter(MSGFMT, datefmt=DATEFMT))
        logger.addHandler(logFileHandler)

    return logger
