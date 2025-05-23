"""Logger module for setting up a custom logger."""

import logging
from datetime import datetime

LOG_FORMAT = (
    "%(asctime)s [%(levelname)s]: %(filename)s(%(funcName)s:%(lineno)s) >> %(message)s"
)
LOG_FILEMODE = "w"
LOG_LEVEL = logging.INFO


class CustomFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        # Includes microseconds up to 6 digits
        dt = datetime.fromtimestamp(record.created)

        return dt.strftime("%Y-%m-%d %H:%M:%S.%f")


def _setup_root_logger():
    """Configures the root logger.

    This function sets up the root logger with a predefined format and log level.
    It also enables capturing of warnings issued by the `warnings` module and
    redirects them to the logging system.

    Notes
    -----
    - The `force=True` parameter ensures that any existing logging configuration
      is overridden.
    - The file handler is added dynamically to the root logger later in the
      E3SMtoCMIP class once the log file path is known.
    """
    custom_formatter = CustomFormatter(LOG_FORMAT)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(custom_formatter)

    logging.basicConfig(
        level=LOG_LEVEL,
        force=True,
        handlers=[console_handler],
    )

    logging.captureWarnings(True)


def _setup_child_logger(name: str, propagate: bool = True) -> logging.Logger:
    """Sets up a logger that is a child of the root logger.

    This child logger inherits the root logger's handlers.

    Parameters
    ----------
    name : str
        Name of the file where this function is called.
    propagate : bool, optional
        Whether to propagate logger messages or not, by default True.

    Returns
    -------
    logging.Logger
        The child logger.

    Examples
    ---------
    Detailed information, typically of interest only when diagnosing problems:

    >>> logger.debug("")

    Confirmation that things are working as expected:

    >>> logger.info("")

    An indication that something unexpected happened, or indicative of some
    problem in the near future:

    >>> logger.warning("")

    The software has not been able to perform some function due to a more
    serious problem:

    >>> logger.error("")

    Similar to ``logger.error()``, but also outputs stack trace:

    >>> logger.exception("", exc_info=True)

    A serious error, indicating that the program itself may be unable to
    continue running:

    >>> logger.critical("")
    """
    logger = logging.getLogger(name)
    logger.propagate = propagate

    return logger


def _add_filehandler(log_path: str):
    """Adds a file handler to the root logger dynamically.

    Adding the file handler will also create the log file automatically.

    Parameters
    ----------
    log_path : str
        The path to the log file.

    Notes
    -----
    Any warnings that appear before the log filehandler is instantiated will not
    be captured (e.g,. esmpy VersionWarning). However, they will still be
    captured by the console via the default StreamHandler.
    """
    file_handler = logging.FileHandler(log_path, mode=LOG_FILEMODE)

    custom_formatter = CustomFormatter(LOG_FORMAT)
    file_handler.setFormatter(custom_formatter)

    logging.root.addHandler(file_handler)
