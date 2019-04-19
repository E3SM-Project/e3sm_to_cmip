import os
import cmor
# import logging
import cdms2
import logging

from progressbar import ProgressBar

from e3sm_to_cmip.util import find_atm_files
from e3sm_to_cmip.util import find_mpas_files
from e3sm_to_cmip.util import print_message
from e3sm_to_cmip.util import print_debug
from e3sm_to_cmip.util import setup_cmor
from e3sm_to_cmip.util import terminate


def run_parallel(pool, handlers, input_path, tables_path, metadata_path, mode='atm', nproc=6, logging=None):
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
    pbar = ProgressBar(maxval=len(pool_res))
    pbar.start()
    for idx, res in enumerate(pool_res):
        try:
            out = res.get(9999999)
            msg = 'Finished {handler}, {done}/{total} jobs complete'.format(
                handler=out,
                done=idx + 1,
                total=len(pool_res))
            logging.info(msg)
            pbar.update(idx)
        except Exception as error:
            # print(format_debug(e))
            logging.error(error)
            terminate(pool)
            raise error
            # return 1
    pbar.finish()
    terminate(pool)
    return 0
# ------------------------------------------------------------------


def run_serial(handlers, input_path, tables_path, metadata_path, mode='atm', logging=None):
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
                    serial=True,
                    logging=logging)
                msg = 'Finished {handler}, {done}/{total} jobs complete'.format(
                    handler=name,
                    done=idx + 1,
                    total=len(handlers))
                logging.info(msg)
                print_message(msg, 'ok')

    except Exception as error:
        print_debug(error)
        return 1
    else:
        print_message("All handlers complete", 'ok')
        return 0
# ------------------------------------------------------------------


def handle_variables(infiles, raw_variables, write_data, outvar_name, outvar_units, table, tables, metadata_path, serial=None, positive=None, levels=None):
    """
    """
    msg = '{}: Starting'.format(outvar_name)

    if serial:
        print(msg)
    nonzero = False
    for variable in raw_variables:
        if len(infiles[variable]) == 0:
            msg = '{}: Unable to find input files for {}'.format(outvar_name, variable)
            print_message(msg)
            nonzero = True
    if nonzero:
        return

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
            if logging:
                logging.info(msg)

            new_data = get_dimension_data(
                filename=infiles[var_name][index],
                variable=var_name,
                levels=levels,
                get_dims=get_dims)
            data.update(new_data)
            get_dims = False

        msg = '{name}: loading axes'.format(name=outvar_name)
        if logging:
            logging.info(msg)
        if serial:
            print(msg)

        # create the cmor variable and axis
        axis_ids, ips = load_axis(data=data, levels=levels)
        if ips:
            data['ips'] = ips
        if positive:
            varid = cmor.variable(outvar_name, outvar_units,
                                  axis_ids, positive=positive)
        else:
            varid = cmor.variable(outvar_name, outvar_units, axis_ids)

        # write out the data
        msg = "{}: writing {} - {}".format(
            outvar_name,
            data['time_bnds'][0][0],
            data['time_bnds'][-1][-1])
        if logging:
            logging.info(msg)
        if serial:
            print(msg)
            pbar = ProgressBar(maxval=len(data['time']))
            pbar.start()
            for index, val in enumerate(data['time']):

                write_data(
                    varid=varid,
                    data=data,
                    timeval=val,
                    timebnds=[data['time_bnds'][index, :]],
                    index=index)
                pbar.update(index)
            pbar.finish()
        else:
            for index, val in enumerate(data['time']):
                write_data(
                    varid=varid,
                    data=data,
                    timeval=val,
                    timebnds=[data['time_bnds'][index, :]],
                    index=index)
    msg = '{}: write complete, closing'.format(outvar_name)

    if serial:
        print(msg)
    cmor.close()
    msg = '{}: file close complete'.format(outvar_name)

    if serial:
        print(msg)
# ------------------------------------------------------------------

def get_dimension_data(filename, variable, levels=None, get_dims=False):
    """
    Returns a list of data, along with the dimension and dimension bounds
    for a given lis of variables, with the option for vertical levels.

    Params:
    -------
        filename: the netCDF file to look inside
        variable: (str): then name of the variable to load
        levels (bool): return verticle information
        get_dims (bool): is dimension data should be loaded too
    Returns:

        {
            data: cdms2 transient variable from the file
            lat: numpy array of lat midpoints,
            lat_bnds: numpy array of lat edge points,
            lon: numpy array of lon midpoints,
            lon_bnds: numpy array of lon edge points,
            time: array of time points,
            time_bdns: array of time bounds
        }

        optionally for 3d:

        lev, ilev, ps, p0, hyam, hyai, hybm, hybi
    """
    # extract data from the input file
    data = dict()

    if not os.path.exists(filename):
        raise IOError("File not found: {}".format(filename))

    try:
        f = cdms2.open(filename)

        # load the data for each variable
        variable_data = f(variable)

        if not variable_data.any():
            return data

        # load
        data.update({
            variable: variable_data
        })

        # load the lon and lat info & bounds
        # load time & time bounds
        if get_dims:
            data.update({
                'lat': variable_data.getLatitude(),
                'lon': variable_data.getLongitude(),
                'lat_bnds': f('lat_bnds'),
                'lon_bnds': f('lon_bnds'),
                'time': variable_data.getTime(),
                'time_bnds': f('time_bnds')
            })
            # load level and level bounds
            if levels:
                data.update({
                    'lev': f.getAxis('lev')[:]/1000,
                    'ilev': f.getAxis('ilev')[:]/1000,
                    'ps': f('PS'),
                    'p0': f('P0'),
                    'hyam': f('hyam'),
                    'hyai': f('hyai'),
                    'hybm': f('hybm'),
                    'hybi': f('hybi'),
                })
    finally:
        f.close()
        return data
# ------------------------------------------------------------------


def load_axis(data, levels=None):

    # create axes
    axes = [{
        str('table_entry'): str('time'),
        str('units'): data['time'].units
    }, {
        str('table_entry'): str('latitude'),
        str('units'): data['lat'].units,
        str('coord_vals'): data['lat'][:],
        str('cell_bounds'): data['lat_bnds'][:]
    }, {
        str('table_entry'): str('longitude'),
        str('units'): data['lon'].units,
        str('coord_vals'): data['lon'][:],
        str('cell_bounds'): data['lon_bnds'][:]
    }]
    if levels:
        lev_axis = {
            str('table_entry'): levels.get('name'),
            str('units'): levels.get('units'),
            str('coord_vals'): data['lev'][:],
            str('cell_bounds'): data['ilev'][:]
        }
        axes.insert(1, lev_axis)

    axis_ids = list()
    for axis in axes:
        axis_id = cmor.axis(**axis)
        axis_ids.append(axis_id)

    ips = None

    # add hybrid level formula terms
    if levels:
        cmor.zfactor(
            zaxis_id=axis_ids[1],
            zfactor_name=str('a'),
            axis_ids=[axis_ids[1], ],
            zfactor_values=data['hyam'][:],
            zfactor_bounds=data['hyai'][:])
        cmor.zfactor(
            zaxis_id=axis_ids[1],
            zfactor_name=str('b'),
            axis_ids=[axis_ids[1], ],
            zfactor_values=data['hybm'][:],
            zfactor_bounds=data['hybi'][:])
        cmor.zfactor(
            zaxis_id=axis_ids[1],
            zfactor_name=str('p0'),
            units=str('Pa'),
            zfactor_values=data['p0'])
        ips = cmor.zfactor(
            zaxis_id=axis_ids[1],
            zfactor_name=str('ps'),
            axis_ids=[axis_ids[0], axis_ids[2], axis_ids[3]],
            units=str('Pa'))

    return axis_ids, ips
# ------------------------------------------------------------------