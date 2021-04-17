from e3sm_to_cmip.util import terminate
from e3sm_to_cmip.util import print_debug
from e3sm_to_cmip.util import print_message
from e3sm_to_cmip.util import find_mpas_files
from e3sm_to_cmip.util import find_atm_files
from e3sm_to_cmip.mpas import write_netcdf
from e3sm_to_cmip import resources
from tqdm import tqdm
import os
import cmor
import logging
import xarray as xr
import numpy as np
import json
logger = logging.getLogger()


def run_parallel(pool, handlers, input_path, tables_path, metadata_path,
                 map_path=None, mode='atm', nproc=6, **kwargs):
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
    will_run = []
    for idx, handler in enumerate(handlers):
        handler_method = handler['method']
        handler_variables = handler['raw_variables']
        # find the input files this handler needs
        if mode in ['atm', 'lnd']:

            input_paths = {var: [os.path.join(input_path, x) for x in
                                 find_atm_files(var, input_path)]
                           for var in handler_variables}
        else:
            input_paths = {var: find_mpas_files(var, input_path,
                                                map_path)
                           for var in handler_variables}

        # setup the input args for the handler
        _kwargs = {
            'table': handler.get('table'),
            'raw_variables': handler.get('raw_variables'),
            'units': handler.get('units'),
            'positive': handler.get('positive'),
            'name': handler.get('name'),
            'logdir': kwargs.get('logdir'),
            'unit_conversion': handler.get('unit_conversion'),
            'simple': kwargs.get('simple'),
            'outpath': kwargs.get('outpath')
        }
        will_run.append(handler.get('name'))

        pool_res.append(
            pool.submit(
                handler_method,
                input_paths,
                tables_path,
                metadata_path,
                **_kwargs))

    # wait for each result to complete
    pbar = tqdm(total=len(pool_res))
    num_success = 0
    num_handlers = len(handlers)
    finished_success = []
    for idx, res in enumerate(pool_res):
        try:
            out = res.result()
            finished_success.append(out)
            if out:
                num_success += 1
                msg = f'Finished {out}, {idx + 1}/{num_handlers} jobs complete'
            else:
                msg = f'Error running handler {handlers[idx]["name"]}'
                print_message(msg, 'error')

            logger.info(msg)
        except Exception as e:
            print_debug(e)
        pbar.update(1)

    pbar.close()
    terminate(pool)
    print_message(f"{num_success} of {num_handlers} handlers complete", 'ok')
    failed = set(will_run) - set(finished_success)
    if failed:
        print_message(f"{', '.join(list(failed))} failed to complete")
    return 0
# ------------------------------------------------------------------


def run_serial(handlers, input_path, tables_path, metadata_path, map_path=None,
               mode='atm', logdir=None, simple=False, outpath=None, freq="mon"):
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

        num_handlers = len(handlers)
        num_success = 0
        name = None

        if mode != 'atm':
            pbar = tqdm(total=len(handlers))

        for _, handler in enumerate(handlers):

            handler_method = handler['method']
            handler_variables = handler['raw_variables']
            unit_conversion = handler.get('unit_conversion')

            # find the input files this handler needs
            if mode in ['atm', 'lnd']:

                input_paths = {var: [os.path.join(input_path, x) for x in
                                     find_atm_files(var, input_path)]
                               for var in handler_variables}
            elif mode == 'fx':
                input_paths = {var: [os.path.join(input_path, x) for x in os.listdir(input_path) if x[-3:] == '.nc']
                               for var in handler_variables}
            else:
                input_paths = {var: find_mpas_files(var, input_path,
                                                    map_path)
                               for var in handler_variables}

            try:
                name = handler_method(
                    input_paths,
                    tables_path,
                    metadata_path,
                    raw_variables=handler.get('raw_variables'),
                    units=handler.get('units'),
                    name=handler.get('name'),
                    table=handler.get('table'),
                    positive=handler.get('positive'),
                    serial=True,
                    logdir=logdir,
                    simple=simple,
                    outpath=outpath,
                    unit_conversion=unit_conversion,
                    freq=freq)
            except Exception as e:
                print_debug(e)

            if name is not None:
                num_success += 1
                msg = f'Finished {name}, {num_success}/{num_handlers} jobs complete'
            else:
                msg = f'Error running handler {handler["name"]}'
                print_message(msg, status='error')
            logger.info(msg)

            if mode != 'atm':
                pbar.update(1)
        if mode != 'atm':
            pbar.close()

    except Exception as error:
        print_debug(error)
        return 1
    else:
        print_message(
            f"{num_success} of {num_handlers} handlers complete", 'ok')
        return 0
# ------------------------------------------------------------------


def handle_simple(infiles, raw_variables, write_data, outvar_name, outvar_units, serial=None, positive=None, levels=None, axis=None, logdir=None, outpath=None, table='Amon', has_time=True):
    from e3sm_to_cmip.util import print_message
    logger = logging.getLogger()

    logger.info(f'{outvar_name}: Starting')

    # check that we have some input files for every variable
    zerofiles = False
    for variable in raw_variables:
        if len(infiles[variable]) == 0:
            msg = f'{outvar_name}: Unable to find input files for {variable}'
            print_message(msg)
            logging.error(msg)
            zerofiles = True
    if zerofiles:
        return None

    # Create the logging directory and setup cmor
    if logdir:
        logpath = logdir
    else:
        outpath, _ = os.path.split(logger.__dict__['handlers'][0].baseFilename)
        logpath = os.path.join(outpath, 'cmor_logs')
    os.makedirs(logpath, exist_ok=True)

    _, inputfile = os.path.split(sorted(infiles[raw_variables[0]])[0])
    # counting from the end, since the variable names might have a _ in them
    start_year = inputfile[len(raw_variables[0]) + 1:].split('_')[0]
    end_year = inputfile[len(raw_variables[0]) + 1:].split('_')[1]

    data = {}

    # assuming all year ranges are the same for every variable
    num_files_per_variable = len(infiles[raw_variables[0]])

    # sort the input files for each variable
    for var_name in raw_variables:
        infiles[var_name].sort()

    for file_index in range(num_files_per_variable):
        loaded = False

        # reload the dimensions for each time slice
        get_dims = True

        # load data for each variables
        for var_name in raw_variables:

            # extract data from the input file
            logger.info(f'{outvar_name}: loading {var_name}')

            new_data = get_dimension_data(
                filename=infiles[var_name][file_index],
                variable=var_name,
                levels=levels,
                get_dims=get_dims)
            data.update(new_data)
            get_dims = False
            if not loaded:
                loaded = True

                # new data set
                ds = xr.Dataset()
                if has_time:
                    dims = ['time', 'lat', 'lon']
                else:
                    dims = ['lat', 'lon']

                for depth_dim in ['lev', 'plev', 'levgrnd']:
                    if depth_dim in new_data.keys():
                        dims.insert(1, depth_dim)

                ds[outvar_name] = (tuple(dims), new_data[var_name])
                for d in dims:
                    ds.coords[d] = new_data[d][:]

        # write out the data
        msg = f"{outvar_name}: time {data['time_bnds'][0][0]:1.1f} - {data['time_bnds'][-1][-1]:1.1f}"
        logger.info(msg)

        if serial:
            pbar = tqdm(total=len(data['time']))
            pbar.set_description(msg)

        for time_index, val in enumerate(data['time']):
            
            outdata = write_data(
                varid=0,
                data=data,
                timeval=val,
                timebnds=[data['time_bnds'][time_index, :]],
                index=time_index,
                raw_variables=raw_variables,
                simple=True)
            ds[outvar_name][time_index] = outdata
            if serial:
                pbar.update(1)

        if serial:
            pbar.close()

    with xr.open_dataset(infiles[raw_variables[0]][0], decode_cf=False, decode_times=False) as inputds:
        for attr, val in inputds.attrs.items():
            ds.attrs[attr] = val

        ds['lat_bnds'] = inputds['lat_bnds']
        ds['lon_bnds'] = inputds['lon_bnds']

        # check for and change the bounds name for lnd files since "time_bounds" is different
        # from every other bounds name in the entire E3SM project
        time_bounds_name = 'time_bnds' if 'time_bnds' in inputds.data_vars else 'time_bounds'
        ds['time_bnds'] = inputds[time_bounds_name]
        ds['time'] = inputds['time']
        ds['time'].attrs['bounds'] = 'time_bnds'

    resource_path, _ = os.path.split(os.path.abspath(resources.__file__))
    table_path = os.path.join(resource_path, table)
    with open(table_path, 'r') as ip:
        table_data = json.load(ip)

    variable_attrs = ['standard_name', 'long_name',
                      'comment', 'cell_methods', 'cell_measures', 'units']
    for attr in variable_attrs:
        ds[outvar_name].attrs[attr] = table_data['variable_entry'][outvar_name][attr]

    output_file_path = os.path.join(
        outpath, f'{outvar_name}_{table[:-5]}_{start_year}-{end_year}')
    msg = f'writing out variable to file {output_file_path}'
    print_message(msg, 'ok')
    fillVals = {
        np.dtype('float32'): 1e20,
        np.dtype('float64'): 1e20,
    }
    write_netcdf(ds, output_file_path, fillValues=fillVals, unlimited=['time'])

    msg = f'{outvar_name}: file close complete'
    logger.debug(msg)

    return outvar_name

# ------------------------------------------------------------------
def var_has_time(table_path, variable):
    with open(table_path, 'r') as inputstream:
        table_info = json.load(inputstream)
    axis_info = table_info['variable_entry'][variable]['dimensions'].split(' ')
    if 'time' in axis_info:
        return 'time'
    elif 'time1' in axis_info:
        return 'time1'
    return False


def handle_variables(infiles, raw_variables, write_data, outvar_name, outvar_units, table, tables, metadata_path, serial=None, positive=None, levels=None, axis=None, logdir=None, simple=False, outpath=None):
    
    timename = var_has_time(os.path.join(tables, table), outvar_name)
    if simple:
        return handle_simple(
            infiles,
            raw_variables,
            write_data,
            outvar_name,
            outvar_units,
            serial=serial,
            table=table,
            positive=positive,
            levels=levels,
            axis=axis,
            logdir=logdir,
            outpath=outpath,
            has_time=timename)

    from e3sm_to_cmip.util import print_message
    logger = logging.getLogger()

    logger.info(f'{outvar_name}: Starting')

    # check that we have some input files for every variable
    zerofiles = False
    for variable in raw_variables:
        if len(infiles[variable]) == 0:
            msg = f'{outvar_name}: Unable to find input files for {variable}'
            print_message(msg)
            logging.error(msg)
            zerofiles = True
    if zerofiles:
        return None

    # Create the logging directory and setup cmor
    if logdir:
        logpath = logdir
    else:
        outpath, _ = os.path.split(logger.__dict__['handlers'][0].baseFilename)
        logpath = os.path.join(outpath, 'cmor_logs')
    os.makedirs(logpath, exist_ok=True)

    logfile = os.path.join(logpath, outvar_name + '.log')

    cmor.setup(
        inpath=tables,
        netcdf_file_action=cmor.CMOR_REPLACE,
        logfile=logfile)

    cmor.dataset_json(str(metadata_path))
    cmor.load_table(str(table))

    msg = f'{outvar_name}: CMOR setup complete'
    logging.info(msg)

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
            logger.info(f'{outvar_name}: loading {var_name}')

            new_data = get_dimension_data(
                filename=infiles[var_name][index],
                variable=var_name,
                levels=levels,
                get_dims=get_dims)
            data.update(new_data)
            get_dims = False
            if simple and not loaded_one:
                loaded_one = True

                # new data set
                ds = xr.Dataset()
                if timename:
                    dims = (timename, 'lat', 'lon')
                else:
                    dims = ('lat', 'lon')

                if 'lev' in new_data.keys():
                    dims = (timename, 'lev', 'lat', 'lon')
                elif 'plev' in new_data.keys():
                    dims = (timename, 'plev', 'lat', 'lon')
                ds[outvar_name] = (dims, new_data[var_name])
                for d in dims:
                    ds.coords[d] = new_data[d][:]

        logger.info(f'{outvar_name}: loading axes')

        # create the cmor variable and axis
        axis_ids, ips = load_axis(data=data, levels=levels, has_time=timename)

        if ips:
            data['ips'] = ips

        if positive:
            varid = cmor.variable(outvar_name, outvar_units,
                                  axis_ids, positive=positive)
        else:
            varid = cmor.variable(outvar_name, outvar_units, axis_ids)

        # write out the data
        msg = f"{outvar_name}: time {data['time_bnds'].values[0][0]:1.1f} - {data['time_bnds'].values[-1][-1]:1.1f}"
        logger.info(msg)

        if serial:
            pbar = tqdm(total=data['time'].shape[0])
            pbar.set_description(msg)

        if timename:
            try:
                for index, val in enumerate(data['time'].values):
                    write_data(
                        varid=varid,
                        data=data,
                        timeval=val,
                        timebnds=[data['time_bnds'].values[index, :]],
                        index=index,
                        raw_variables=raw_variables,
                        simple=False)
                    if serial:
                        pbar.update(1)
            except Exception as e:
                print(e)
        else:
            write_data(
                varid=varid,
                data=data,
                raw_variables=raw_variables,
                simple=False)
        if serial:
            pbar.close()

    msg = f'{outvar_name}: write complete, closing'
    logger.debug(msg)
    cmor.close()

    msg = f'{outvar_name}: file close complete'
    logger.debug(msg)

    return outvar_name
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
            data: xarray Dataset from the file
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
        raise IOError(f"File not found: {filename}")

    ds = xr.open_dataset(filename, decode_times=False)

    # load the data for each variable
    variable_data = ds[variable]

    # load
    if 'plev' in ds.dims or 'lev' in ds.dims:
        data[variable] = variable_data.values
    else:    
        data[variable] = variable_data

    # atm uses "time_bnds" but the lnd component uses "time_bounds"
    time_bounds_name = 'time_bnds' if 'time_bnds' in ds.data_vars.keys() else 'time_bounds'

    # load the lon and lat and time info & bounds
    if get_dims:
        data.update({
            'lat': variable_data['lat'],
            'lon': variable_data['lon'],
            'lat_bnds': ds['lat_bnds'],
            'lon_bnds': ds['lon_bnds'],
            'time': variable_data['time']
        })
        try:
            time2 = variable_data['time2']
        except KeyError:
            pass
        else:
            data['time2'] = time2

        if time_bounds_name in ds.data_vars:
            time_bnds = ds[time_bounds_name]
            if len(time_bnds.shape) == 1:
                time_bnds = time_bnds.reshape(1, 2)
            data['time_bnds'] = time_bnds

        try:
            levgrnd = variable_data['levgrnd']
        except KeyError:
            pass
        else:
            data['levgrnd'] = levgrnd

        # load level and level bounds
        if levels is not None:
            if levels.get('name') == 'standard_hybrid_sigma' or levels.get('name') == 'standard_hybrid_sigma_half':
                data.update({
                    'lev': ds['lev'].values/1000,
                    'ilev': ds['ilev'].values/1000,
                    'ps': ds['PS'].values,
                    'p0': ds['P0'].values.item(),
                    'hyam': ds['hyam'],
                    'hyai': ds['hyai'],
                    'hybm': ds['hybm'],
                    'hybi': ds['hybm'],
                })
            else:
                name = levels.get('e3sm_axis_name')
                if name in ds.data_vars or name in ds.coords:
                    data[name] = ds[name]

                bnds = levels.get('e3sm_axis_bnds')
                if bnds:
                    if bnds in ds.dims or bnds in ds.data_vars:
                        data[bnds] = ds[bnds]
                    else:
                        raise IOError("Unable to find e3sm_axis_bnds")
    return data
# ------------------------------------------------------------------


def load_axis(data, levels=None, has_time=True):
    # use the special name for time if it exists
    if levels and levels.get('time_name'):
        name = levels.get('time_name')
        units = data[levels.get('time_name')].units
        time = cmor.axis(name, units=units)
    # else add the normal time name
    elif has_time:
        time = cmor.axis(has_time, units=data['time'].attrs['units'])

    # use whatever level name this handler requires
    if levels:
        name = levels.get('name')
        units = levels.get('units')
        coord_vals = data[levels.get('e3sm_axis_name')]

        axis_bnds = levels.get('e3sm_axis_bnds')
        if axis_bnds:
            lev = cmor.axis(name,
                            units=units,
                            cell_bounds=data[axis_bnds],
                            coord_vals=coord_vals)
        else:
            lev = cmor.axis(name,
                            units=units,
                            coord_vals=coord_vals)

    # add lon/lat
    lat = cmor.axis('latitude',
                    units=data['lat'].units,
                    coord_vals=data['lat'].values,
                    cell_bounds=data['lat_bnds'].values)

    lon = cmor.axis('longitude',
                    units=data['lon'].units,
                    coord_vals=data['lon'].values,
                    cell_bounds=data['lon_bnds'].values)

    if levels:
        axes = [time, lev, lat, lon]
    elif has_time:
        axes = [time, lat, lon]
    else:
        axes = [lat, lon]

    ips = None
    # add hybrid level formula terms
    if levels and levels.get('name') in ['standard_hybrid_sigma', 'standard_hybrid_sigma_half']:
        cmor.zfactor(
            zaxis_id=lev,
            zfactor_name='a',
            axis_ids=[lev, ],
            zfactor_values=data['hyam'].values,
            zfactor_bounds=data['hyai'].values)
        cmor.zfactor(
            zaxis_id=lev,
            zfactor_name='b',
            axis_ids=[lev, ],
            zfactor_values=data['hybm'].values,
            zfactor_bounds=data['hybi'].values)
        cmor.zfactor(
            zaxis_id=lev,
            zfactor_name='p0',
            units='Pa',
            zfactor_values=data['p0'])
        ips = cmor.zfactor(
            zaxis_id=lev,
            zfactor_name='ps',
            axis_ids=[time, lat, lon],
            units='Pa')

    return axes, ips
# ------------------------------------------------------------------
