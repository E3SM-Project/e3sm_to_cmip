from e3sm_to_cmip.util import terminate
from e3sm_to_cmip.util import print_debug
from e3sm_to_cmip.util import print_message
from e3sm_to_cmip.util import find_mpas_files
from e3sm_to_cmip.util import find_atm_files
from tqdm import tqdm
import os
import cmor
import cdms2
import logging
import xarray as xr
import numpy as np
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
            'logdir': kwargs.get('logdir')
        }

        pool_res.append(
            pool.apipe(
                handler_method,
                input_paths,
                tables_path,
                metadata_path,
                **_kwargs))

    # wait for each result to complete
    pbar = tqdm(total=len(pool_res))
    num_success = 0
    num_handlers = len(handlers)

    for idx, res in enumerate(pool_res):
        try:
            out = res.get(9999999)
            if out:
                num_success += 1
                msg = 'Finished {handler}, {done}/{total} jobs complete'.format(
                    handler=out,
                    done=idx + 1,
                    total=num_handlers)
            else:
                msg = 'Error running handler {}'.format(handlers[idx]['name'])
                print_message(msg, 'error')

            logger.info(msg)
            pbar.update(1)
        except Exception as e:
            print_debug(e)
            return 1

    pbar.close()
    terminate(pool)
    print_message("{} of {} handlers complete".format(
        num_success, num_handlers), 'ok')
    return 0
# ------------------------------------------------------------------


def my_dynamic_message(self, progress, data):
    """
    Make the progressbar not crash, and also give a nice custom message
    """
    val = data['dynamic_messages'].get('running')
    if val:
        return 'Running: {0: <16}'.format(data['dynamic_messages'].get('running'))
    else:
        return 'Running: ' + 16 * '-'
# ------------------------------------------------------------------


def run_serial(handlers, input_path, tables_path, metadata_path, map_path=None,
               mode='atm', logdir=None, simple=False, outpath=None):
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
                input_paths = {var: [x for x in os.listdir(input_path) if x[-3:] == '.nc']
                               for var in handler_variables}
            else:
                input_paths = {var: find_mpas_files(var, input_path,
                                                    map_path)
                               for var in handler_variables}

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
                unit_conversion=unit_conversion)

            if name is not None:
                num_success += 1
                msg = 'Finished {handler}, {done}/{total} jobs complete'.format(
                    handler=name,
                    done=num_success,
                    total=num_handlers)
            else:
                msg = 'Error running handler {}'.format(handler['name'])
                print_message(msg, 'error')
            logger.info(msg)

            if mode != 'atm':
                pbar.update(1)
        if mode != 'atm':
            pbar.close()

    except Exception as error:
        print_debug(error)
        return 1
    else:
        print_message("{} of {} handlers complete".format(
            num_success, num_handlers), 'ok')
        return 0
# ------------------------------------------------------------------


def handle_variables(infiles, raw_variables, write_data, outvar_name, outvar_units, table, tables, metadata_path, serial=None, positive=None, levels=None, axis=None, logdir=None, simple=False, outpath=None):

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

    if not simple:
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
        if simple:
            loaded_one = False
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
                # dims = tuple(x for x in new_data.keys() of x != var_name and x != 'time2' and 'bnds' not in x)
                dims = ('time', 'lat', 'lon')
                if 'lev' in new_data.keys():
                    dims = ('time', 'lev', 'lat', 'lon')
                ds[outvar_name] = (dims, new_data[var_name])
                for d in dims:
                    ds.coords[d] = new_data[d][:]
                # ds.coords['time'] = new_data['time'][:]
                # ds.coords['lat'] = new_data['lat'][:]
                # ds.coords['lon'] = new_data['lon'][:]

        if not simple:
            logger.info(f'{outvar_name}: loading axes')

            # create the cmor variable and axis
            axis_ids, ips = load_axis(data=data, levels=levels)

            if ips:
                data['ips'] = ips

            if positive:
                varid = cmor.variable(outvar_name, outvar_units,
                                      axis_ids, positive=positive)
            else:
                varid = cmor.variable(outvar_name, outvar_units, axis_ids)
        else:
            varid = 0

        # write out the data
        msg = f"{outvar_name}: time {data['time_bnds'][0][0]:1.1f} - {data['time_bnds'][-1][-1]:1.1f}"
        logger.info(msg)

        if serial:
            pbar = tqdm(total=len(data['time']))

        # import ipdb; ipdb.set_trace()
        for index, val in enumerate(data['time']):
            outdata = write_data(
                varid=varid,
                data=data,
                timeval=val,
                timebnds=[data['time_bnds'][index, :]],
                index=index,
                raw_variables=raw_variables,
                simple=simple)
            if simple:
                # pnds = nds.sel(time=val)
                # pnds[outvar_name] = (('lat', 'lon'), outdata)
                # ds = xr.concat([ds, pnds], dim='time')
                ds[outvar_name][index] = outdata
            if serial:
                pbar.update(1)
                pbar.set_description(msg)
        if serial:
            pbar.close()

    if simple:
        output_file_path = os.path.join(outpath, f'{outvar_name}.nc')
        msg = f'writing out variable to file {output_file_path}'
        print_message(msg, 'ok')
        ds.to_netcdf(path=output_file_path)
    else:
        msg = '{}: write complete, closing'.format(outvar_name)
        logger.debug(msg)

        cmor.close()

    msg = '{}: file close complete'.format(outvar_name)
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

    fp = cdms2.open(filename)

    # load the data for each variable
    variable_data = fp(variable)

    # load
    data.update({
        variable: variable_data
    })

    # atm uses "time_bnds" but the lnd component uses "time_bounds"
    time_bounds_name = 'time_bnds' if 'time_bnds' in fp.variables.keys() else 'time_bounds'

    # load the lon and lat info & bounds
    # load time & time bounds
    if get_dims:
        data.update({
            'lat': variable_data.getLatitude(),
            'lon': variable_data.getLongitude(),
            'lat_bnds': fp('lat_bnds'),
            'lon_bnds': fp('lon_bnds'),
            'time': variable_data.getTime(),
            'time2': variable_data.getTime(),
            'time_bnds': fp(time_bounds_name)
        })

        try:
            index = variable_data.getAxisIds().index('levgrnd')
        except:
            pass
        else:
            data.update({
                'levgrnd': variable_data.getAxis(index)
            })

        # load level and level bounds
        if levels is not None:
            if levels.get('name') == 'standard_hybrid_sigma' or levels.get('name') == 'standard_hybrid_sigma_half':
                data.update({
                    'lev': fp.getAxis('lev')[:]/1000,
                    'ilev': fp.getAxis('ilev')[:]/1000,
                    'ps': fp('PS'),
                    'p0': fp('P0'),
                    'hyam': fp('hyam'),
                    'hyai': fp('hyai'),
                    'hybm': fp('hybm'),
                    'hybi': fp('hybi'),
                })
            else:
                name = levels.get('e3sm_axis_name')
                if name in fp.listdimension():
                    data[name] = fp.getAxis(name)[:]
                else:
                    raise IOError("Unable to find e3sm_axis_name")

                bnds = levels.get('e3sm_axis_bnds')
                if bnds:
                    if bnds in fp.listdimension():
                        data[bnds] = fp.getAxis(bnds)[:]
                    elif bnds in fp.variables.keys():
                        data[bnds] = fp(bnds)[:]
                    else:
                        raise IOError("Unable to find e3sm_axis_bnds")
    return data
# ------------------------------------------------------------------


def load_axis(data, levels=None):
    # use the special name for time if it exists
    if levels and levels.get('time_name'):
        name = levels.get('time_name')
        units = data[levels.get('time_name')].units
        time = cmor.axis(name, units=units)
    # else add the normal time name
    else:
        time = cmor.axis('time', units=data['time'].units)
    
    # use whatever level name this handler requires
    if levels:
        name = levels.get('name')
        units = levels.get('units')
        coord_vals = data[levels.get('e3sm_axis_name')][:]

        axis_bnds = levels.get('e3sm_axis_bnds')
        if axis_bnds:
            lev = cmor.axis(name,
                            units=units,
                            cell_bounds=data[axis_bnds][:],
                            coord_vals=coord_vals)
        else:
            lev = cmor.axis(name,
                            units=units,
                            coord_vals=coord_vals)
    
    # add lon/lat
    lat = cmor.axis('latitude',
                    units=data['lat'].units,
                    coord_vals=data['lat'][:],
                    cell_bounds=data['lat_bnds'][:])

    lon = cmor.axis('longitude',
                    units=data['lon'].units,
                    coord_vals=data['lon'][:],
                    cell_bounds=data['lon_bnds'][:])

    if levels:
        axes = [time, lev, lat, lon]
    else:
        axes = [time, lat, lon]

    ips = None
    # add hybrid level formula terms
    if levels and levels.get('name') in ['standard_hybrid_sigma', 'standard_hybrid_sigma_half']:
        cmor.zfactor(
            zaxis_id=lev,
            zfactor_name='a',
            axis_ids=[lev, ],
            zfactor_values=data['hyam'][:],
            zfactor_bounds=data['hyai'][:])
        cmor.zfactor(
            zaxis_id=lev,
            zfactor_name='b',
            axis_ids=[lev, ],
            zfactor_values=data['hybm'][:],
            zfactor_bounds=data['hybi'][:])
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
