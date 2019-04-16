import os
import cmor
import logging
import cdms2
import logging

from tqdm import tqdm

from e3sm_to_cmip.util import find_atm_files
from e3sm_to_cmip.util import find_mpas_files
from e3sm_to_cmip.util import print_message
from e3sm_to_cmip.util import format_debug
from e3sm_to_cmip.util import get_dimension_data
from e3sm_to_cmip.util import setup_cmor
from e3sm_to_cmip.util import load_axis


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

            # setup the input args for the handler
            _args = (input_paths,
                     tables_path,
                     metadata_path)

            # add the future to the results list
            pool_res.append(
                pool.apply_async(
                    handler_method,
                    args=_args,
                    kwds={}))

    # wait for each result to complete
    for idx, res in enumerate(tqdm(pool_res)):
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
        for idx, handler in enumerate(handlers):
            for _, handler_info in handler.items():

                handler_method = handler_info[0]
                handler_variables = handler_info[1]
                # find the input files this handler needs
                if mode in ['atm', 'lnd']:

                    input_paths = {var: [
                        os.path.join(input_path, x) for x in find_atm_files(var, input_path)
                    ] for var in handler_variables}

                name = handler_method(
                    input_paths,
                    tables_path,
                    metadata_path,
                    serial=True)
                msg = 'Finished {handler}, {done}/{total} jobs complete'.format(
                    handler=name,
                    done=idx + 1,
                    total=len(handlers))
                logging.info(msg)
                print_message(msg, 'ok')

    except Exception as error:
        print_message(format_debug(error))
        return 1
    else:
        print_message("All handlers complete", 'ok')
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


def handle_variables(infiles, raw_variables, write_data, outvar_name, outvar_units, table, tables, metadata_path, serial=None, positive=None):
    """
    """
    msg = '{}: Starting'.format(outvar_name)
    logging.info(msg)
    if serial:
        print(msg)
    
    msg = '{}: running with input files: {}'.format(
        outvar_name,
        infiles)
    logging.info(msg)

    # setup cmor
    setup_cmor(
        outvar_name,
        tables,
        table,
        metadata_path)

    msg = '{}: CMOR setup complete'.format(outvar_name)
    logging.info(msg)
    if serial:
        print(msg)

    data = {}

    # assuming all year ranges are the same for every variable
    num_files_per_variable = len(infiles[raw_variables[0]])

    # sort the input files for each variable
    for var_name in raw_variables:
        infiles[var_name].sort()

    for index in range(num_files_per_variable):

        # reload the dimensions for each time slice
        get_dims = True

        # load data for each variable
        for var_name in raw_variables:

            # extract data from the input file
            msg = '{name}: loading {variable}'.format(
                name=outvar_name,
                variable=var_name)
            logging.info(msg)

            new_data = get_dimension_data(
                filename=infiles[var_name][index],
                variable=var_name,
                levels=False,
                get_dims=get_dims)
            data.update(new_data)
            get_dims = False

        msg = '{name}: loading axes'.format(name=outvar_name)
        logging.info(msg)
        if serial:
            print(msg)

        # create the cmor variable and axis
        axis_ids, _ = load_axis(data=data)
        if positive:
            varid = cmor.variable(outvar_name, outvar_units, axis_ids, positive=positive)
        else:
            varid = cmor.variable(outvar_name, outvar_units, axis_ids)

        # write out the data
        msg = "{}: writing {} - {}".format(
            outvar_name,
            data['time_bnds'][0][0],
            data['time_bnds'][-1][-1])
        if serial:
            print(msg)
            for index, val in enumerate(  # data['time']):
                tqdm(
                    data['time'],
                    position=0,
                    desc="{}: {} - {}".format(
                        outvar_name,
                        data['time_bnds'][0][0],
                        data['time_bnds'][-1][-1]))):

                write_data(
                    varid=varid,
                    data=data,
                    timeval=val,
                    timebnds=[data['time_bnds'][index, :]],
                    index=index)
        else:
            for index, val in enumerate(data['time']):
                write_data(
                    varid=varid,
                    data=data,
                    timeval=val,
                    timebnds=[data['time_bnds'][index, :]],
                    index=index)
    msg = '{}: write complete, closing'.format(outvar_name)
    logging.info(msg)
    if serial:
        print(msg)
    cmor.close()
    msg = '{}: file close complete'.format(outvar_name)
    logging.info(msg)
    if serial:
        print(msg)
# ------------------------------------------------------------------

# def handle_variables_3d(infiles, raw_variables, write_data, outvar_name, outvar_units, table, tables, metadata_path, serial=None, positive=None):
#     """
#     """
#     msg = '{}: Starting'.format(outvar_name)
#     logging.info(msg)
#     if serial:
#         print(msg)
    
#     msg = '{}: running with input files: {}'.format(
#         outvar_name,
#         infiles)
#     logging.info(msg)

#     # setup cmor
#     setup_cmor(
#         outvar_name,
#         tables,
#         table,
#         metadata_path)

#     msg = '{}: CMOR setup complete'.format(outvar_name)
#     logging.info(msg)
#     if serial:
#         print(msg)

#     data = {}

#     # assuming all year ranges are the same for every variable
#     num_files_per_variable = len(infiles[raw_variables[0]])

#     # sort the input files for each variable
#     for var_name in raw_variables:
#         infiles[var_name].sort()

#     for index in range(num_files_per_variable):

#         # reload the dimensions for each time slice
#         get_dims = True

#         # load data for each variable
#         for var_name in raw_variables:

#             # extract data from the input file
#             msg = '{name}: loading {variable}'.format(
#                 name=outvar_name,
#                 variable=var_name)
#             logging.info(msg)

#             new_data = get_dimension_data(
#                 filename=infiles[var_name][index],
#                 variable=var_name,
#                 levels=False,
#                 get_dims=get_dims)
#             data.update(new_data)
#             get_dims = False

#         msg = '{name}: loading axes'.format(name=outvar_name)
#         logging.info(msg)
#         if serial:
#             print(msg)

#         # create the cmor variable and axis
#         axis_ids, _ = load_axis(data=data)
#         if positive:
#             varid = cmor.variable(outvar_name, outvar_units, axis_ids, positive=positive)
#         else:
#             varid = cmor.variable(outvar_name, outvar_units, axis_ids)

#         # write out the data
#         msg = "{}: writing {} - {}".format(
#             outvar_name,
#             data['time_bnds'][0][0],
#             data['time_bnds'][-1][-1])
#         if serial:
#             print(msg)
#             for index, val in enumerate(  # data['time']):
#                 tqdm(
#                     data['time'],
#                     position=0,
#                     desc="{}: {} - {}".format(
#                         outvar_name,
#                         data['time_bnds'][0][0],
#                         data['time_bnds'][-1][-1]))):

#                 write_data(
#                     varid=varid,
#                     data=data,
#                     timeval=val,
#                     timebnds=[data['time_bnds'][index, :]],
#                     index=index)
#         else:
#             for index, val in enumerate(data['time']):
#                 write_data(
#                     varid=varid,
#                     data=data,
#                     timeval=val,
#                     timebnds=[data['time_bnds'][index, :]],
#                     index=index)
#     msg = '{}: write complete, closing'.format(outvar_name)
#     logging.info(msg)
#     if serial:
#         print(msg)
#     cmor.close()
#     msg = '{}: file close complete'.format(outvar_name)
#     logging.info(msg)
#     if serial:
#         print(msg)
# # ------------------------------------------------------------------
