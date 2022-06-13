import logging
import time
from datetime import datetime

from pytz import UTC


def _setup_custom_logger(
    name: str, propagate: bool = False
) -> logging.Logger:  # pragma: no cover
    """Sets up a custom logger.
    Parameters
    ----------
    name : str
        Name of the file where this function is called.
    propagate : bool, optional
        Whether to propagate logger messages or not, by default False
    Returns
    -------
    logging.Logger
        The logger.
    """
    # Setup
    log_name = f'logs/{UTC.localize(datetime.utcnow()).strftime("%Y%m%d_%H%M%S_%f")}'
    log_format = "%(asctime)s_%(msecs)03d:%(levelname)s:%(funcName)s:%(message)s"
    logging.basicConfig(
        filename=log_name,
        format=log_format,
        datefmt="%Y%m%d_%H%M%S",
        level=logging.INFO,
    )
    logging.Formatter.converter = time.gmtime

    logger = logging.getLogger(log_name)
    logger.propagate = propagate

    # Console output
    consoleHandler = logging.StreamHandler()
    logFormatter = logging.Formatter(log_format)
    consoleHandler.setFormatter(logFormatter)
    logger.addHandler(consoleHandler)

    return logger
