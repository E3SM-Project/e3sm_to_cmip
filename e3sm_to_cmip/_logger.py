"""Logger module for setting up a custom logger."""

import logging
import logging.handlers
import shutil
from datetime import datetime, timezone

TIMESTAMP = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
LOG_DIR = "prov"
LOG_FILENAME = f"{TIMESTAMP}.log"
LOG_FORMAT = (
    "%(asctime)s [%(levelname)s]: %(filename)s(%(funcName)s:%(lineno)s) >> %(message)s"
)
LOG_FILEMODE = "a"
LOG_LEVEL = logging.INFO

# Setup the root logger with a default log file.
# `force` is set to `True` to automatically remove root handlers whenever
# `basicConfig` called. This is required for cases where multiple e3sm_to_cmip
# runs are executed. Otherwise, the logger objects attempt to share the same
# root file reference (which gets deleted between runs), resulting in
# `FileNotFoundError: [Errno 2] No such file or directory: 'e3sm_diags_run.log'`.
# More info here: https://stackoverflow.com/a/49202811
logging.basicConfig(
    format=LOG_FORMAT,
    filename=LOG_FILENAME,
    filemode=LOG_FILEMODE,
    level=LOG_LEVEL,
    force=True,
)

logging.captureWarnings(True)


def _setup_logger(name: str, propagate: bool = True) -> logging.Logger:
    """Sets up a custom logger that is a child of the root logger.

    This custom logger inherits the root logger's handlers.

    The log files are saved in a `/logs` directory relative to where
    `e3sm_to_cmip` is executed.

    Parameters
    ----------
    name : str
        Name of the file where this function is called.
    propagate : bool, optional
        Whether to propagate logger messages or not, by default True.

    Returns
    -------
    logging.Logger
        The logger.

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


def _update_root_logger_filepath(log_path: str):
    """Updates the log file path to the provenance directory.

    This function updates the filename of the existing file handler to the new
    path.

    Parameters
    ----------
    log_path : str
        The path to the log file.

    Notes
    -----
    - The method assumes that a logging file handler is already configured.
    - The log file is closed and reopened at the new location.
    - The log file mode is determined by the constant `LOG_FILEMODE`.
    - The log file name is determined by the constant `LOG_FILENAME`.
    """
    for handler in logging.root.handlers:
        if isinstance(handler, logging.FileHandler):
            # Move the log file to the new directory because it might contain
            # warnings that are raised before this function is called
            # (e.g., esmpy VersionWarning).
            shutil.move(handler.baseFilename, log_path)

            handler.baseFilename = log_path
            handler.stream.close()
            handler.stream = open(log_path, LOG_FILEMODE)  # type: ignore

            break
