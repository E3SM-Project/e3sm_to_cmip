import os
import logging

from e3sm_to_cmip.util import find_atm_files
from e3sm_to_cmip.util import find_mpas_files
from e3sm_to_cmip.util import print_message
from e3sm_to_cmip.util import format_debug


def run_parallel(pool, handlers, input_path, tables_path, metadata_path, mode='atm', nproc=6):
    """
    Run all the handlers in parallel
    Params:
    -------
        pool (multiprocessing.Pool): a processing pool to run the handlers in
        handlers: a dict(str: (function_pointer, list(str) ) )
        input_path (str): path to the input files directory
        tables_path (str): path to the tables directory
        metadata_path (str): path to the cmor input metadata
        mode (str): what type of files to work with
    Returns:
    --------
        returns 1 if an error occurs, else 0
    """
    pool_res = list()
    for idx, handler in enumerate(handlers):
        for _, handler_info in handler.items():

            handler_method = handler_info[0]
            handler_variables = handler_info[1]
            # find the input files this handler needs
            if mode in ['atm', 'lnd']:

                input_paths = {var: [
                    os.path.join(input_path, x) for x in find_atm_files(var, input_path)
                ] for var in handler_variables}
            else:
                input_paths = {var: [
                    os.path.join(input_path, x) for x in find_mpas_files(var, input_path)
                ] for var in handler_variables}

            _args = (input_paths,
                     tables_path,
                     metadata_path,
                     idx)
            pool_res.append(
                pool.apply_async(
                    handler_method,
                    args=_args,
                    kwds={}))

    for idx, res in enumerate(pool_res):
        try:
            out = res.get(9999999)
            msg = 'Finished {handler}, {done}/{total} jobs complete'.format(
                handler=out,
                done=idx + 1,
                total=len(pool_res))
            logging.info(msg)
        except Exception as e:
            print(format_debug(e))
            logging.error(e)
            terminate(pool)
            return 1

    terminate(pool)
    return 0
# ------------------------------------------------------------------


def run_serial(handlers, input_path, tables_path, metadata_path, mode='atm'):
    """
    Run each of the handlers one at a time on the main process

    Params:
    -------
        handlers: a dict(str: (function_pointer, list(str) ) )
        input_path (str): path to the input files directory
        tables_path (str): path to the tables directory
        metadata_path (str): path to the cmor input metadata
        mode (str): what type of files to work with
    Returns:
    --------
        returns 1 if an error occurs, else 0
    """
    try:
        for handler in handlers:
            for _, handler_info in handler.items():

                handler_method = handler_info[0]
                handler_variables = handler_info[1]
                # find the input files this handler needs
                if mode in ['atm', 'lnd']:

                    input_paths = {var: [
                        os.path.join(input_path, x) for x in find_atm_files(var, input_path)
                    ] for var in handler_variables}

                handler_method(
                    input_paths,
                    tables_path,
                    metadata_path)

    except Exception as error:
        print(format_debug(error))
        return 1
    else:
        return 0
# ------------------------------------------------------------------


def terminate(pool, debug=False):
    """
    Terminates the process pool

    Params:
    -------
        pool (multiprocessing.Pool): the pool to shutdown
        debug (bool): if we're running in debug mode
    Returns:
    --------
        None
    """
    if debug:
        print_message('Shutting down process pool', 'debug')
    pool.close()
    pool.terminate()
    pool.join()
# ------------------------------------------------------------------
