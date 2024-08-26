import logging
import os
from datetime import datetime, timezone

DEFAULT_LOG_LEVEL = logging.DEBUG
DEFAULT_LOG_DIR = "e2c_logs"
DEFAULT_LOG = f"{DEFAULT_LOG_DIR}/e2c_root_log-{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S_%f')}.log"


def _logger(
    name: str | None = None,
    logfilename: str = DEFAULT_LOG,
    log_level: int = DEFAULT_LOG_LEVEL,
    to_console: bool = False,
    to_logfile: bool = False,
    propagate: bool = False,
):
    """Return a root or named logger with variable configuration.

    Parameters
    ----------
    name : str
        The name displayed for the logger in messages.
        If name == None or name == "__main__", the root logger is returned
    logfilename : str
        If logfile handling is requested, any logfile may be specified, or else
        the default (e2c_logs/dflt_log-{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S_%f')}.log) is used.
    log_level : int
        Logging.DEBUG (default), logging.INFO, logging.WARNING, logging.ERROR,
        or logging.CRITICAL, by default logging.DEBUG.
    to_console : bool
        If True, a logging.StreamHandler is supplied, by default False.
    to_logfile : bool
        If True, a logging.FileHandler is supplied, by default False.
    propagate : boolean
        If True, messages logged are propagated to the root logger, by default
        False.
    """
    if to_logfile:
        dn = os.path.dirname(logfilename)
        if len(dn) and not os.path.exists(dn):
            os.makedirs(dn)

    if name is None or name == "__main__":
        logger = logger.root  # noqa: F821
    else:
        logger = logging.getLogger(name)

    logger.propagate = propagate
    logger.setLevel(log_level)

    logger.handlers = []

    msgfmt = "%(asctime)s_%(msecs)03d:%(levelname)s:%(name)s:%(funcName)s:%(message)s"
    datefmt = "%Y%m%d_%H%M%S"

    if to_console:
        logStreamHandler = logging.StreamHandler()
        logStreamHandler.setFormatter(logging.Formatter(msgfmt, datefmt=datefmt))
        logger.addHandler(logStreamHandler)

    if to_logfile:
        logFileHandler = logging.FileHandler(logfilename)
        logFileHandler.setFormatter(logging.Formatter(msgfmt, datefmt=datefmt))
        logger.addHandler(logFileHandler)

    return logger
