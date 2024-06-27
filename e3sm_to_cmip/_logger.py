import inspect
import logging
import os
from datetime import datetime, timezone

'''
    ACCEPTS:
        name=<anyname>
        logfilename=<anypath/file>      [default = e2c_logs/dflt_log-{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S_%f')}.log]
        log_level=<level>               [default = DEBUG]
        to_console=[True|False]         [default = False]
        to_logfile=[True|False]         [default = False]
        propagate=[True|False]          [default = False]
'''

default_log_dir = "e2c_logs"
default_log = f"{default_log_dir}/e2c_root_log-{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S_%f')}.log"

def e2c_logger(name=None, logfilename=default_log, set_log_level=None, to_console=False, to_logfile=False, propagate=False):

    # print(f"DEBUG: _logger: entered e2c_logger at {datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S_%f')} called by {name}", flush=True)

    default_log_lvl = logging.DEBUG

    # create logging directory as required

    if to_logfile:
        dn = os.path.dirname(logfilename)
        if len(dn) and not os.path.exists(dn):
            os.makedirs(dn)

    logger = logging.getLogger(name)
    if name == None or name == "__main__":
        logger = logger.root

    logger.propagate = propagate

    if set_log_level == "None" or set_log_level == "DEBUG":
        log_level = default_log_lvl
    elif set_log_level == "INFO":
        log_level = logging.INFO
    elif set_log_level == "WARNING":
        log_level = logging.WARNING
    elif set_log_level == "ERROR":
        log_level = logging.ERROR
    elif set_log_level == "CRITICAL":
        log_level = logging.CRITICAL
    else: log_level = default_log_lvl

    logger.setLevel(log_level)

    logger.handlers = []

    if to_console:
        logStreamHandler = logging.StreamHandler()
        logStreamHandler.setFormatter(logging.Formatter("%(asctime)s_%(msecs)03d:%(levelname)s:%(name)s:%(funcName)s:%(message)s",datefmt="%Y%m%d_%H%M%S"))
        logger.addHandler(logStreamHandler)

    if to_logfile:
        logFileHandler = logging.FileHandler(logfilename)
        logFileHandler.setFormatter(logging.Formatter("%(asctime)s_%(msecs)03d:%(levelname)s:%(name)s:%(funcName)s:%(message)s",datefmt="%Y%m%d_%H%M%S"))
        logger.addHandler(logFileHandler)

    return logger


