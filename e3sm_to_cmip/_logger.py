import logging
import os
import time
from datetime import datetime

from pytz import UTC


def _setup_root_logger() -> str:  # pragma: no cover
    """Sets up the root logger.

    The logger module will write to a log file and stream the console
    simultaneously.

    The log files are saved in a `/logs` directory relative to where
    `e3sm_to_cmip` is executed.

    Returns
    -------
    str
        The name of the logfile.
    """
    os.makedirs("logs", exist_ok=True)
    filename = f'logs/{UTC.localize(datetime.utcnow()).strftime("%Y%m%d_%H%M%S_%f")}'
    log_format = "%(asctime)s_%(msecs)03d:%(levelname)s:%(funcName)s:%(message)s"

    # Setup the logging module.
    logging.basicConfig(
        filename=filename,
        format=log_format,
        datefmt="%Y%m%d_%H%M%S",
        level=logging.DEBUG,
    )
    logging.captureWarnings(True)
    logging.Formatter.converter = time.gmtime

    # Configure and add a console stream handler.
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    log_formatter = logging.Formatter(log_format)
    console_handler.setFormatter(log_formatter)
    logging.getLogger().addHandler(console_handler)

    return filename


def _setup_logger(name, propagate=True) -> logging.Logger:
    """Sets up a logger object.

    This function is intended to be used at the top-level of a module.

    Parameters
    ----------
    name : str
        Name of the file where this function is called.
    propagate : bool, optional
        Propogate this logger module's messages to the root logger or not, by
        default True.

    Returns
    -------
    logging.Logger
        The logger.
    """
    logger = logging.getLogger(name)
    logger.propagate = propagate

    return logger
