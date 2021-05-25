"""
CLDICE to cli converter
"""
from __future__ import absolute_import, division, print_function, unicode_literals

import cmor
import logging
import os
import xarray as xr
from tqdm import tqdm
from e3sm_to_cmip.util import print_message, reconstructPressureFromHybrid

# list of raw variable names needed
RAW_VARIABLES = [str('hybi'), str('hyai'), str('hyam'), str('hybm'), str('PS')]
VAR_NAME = str('phalf')
VAR_UNITS = str('Pa')
TABLE = str('CMIP6_Amon.json')
LEVELS = {
    'name': 'atmosphere_sigma_coordinate',
    'units': '1',
    'e3sm_axis_name': 'lev',
    'e3sm_axis_bnds': 'ilev',
    'time_name': 'time2'
}


def write_data(varid, data, timeval, timebnds, index, **kwargs):
    """
    phalf = P0*hyai + PS*hybi
    """
    outdata = reconstructPressureFromHybrid(
        data['PS'][index, :], data['hyai'], data['hybi'], 100000)
    cmor.write(
        varid,
        outdata.values,
        time_vals=timeval,
        time_bnds=timebnds)
    cmor.write(
        data['ips'],
        data['ps'].values,
        time_vals=timeval,
        time_bnds=timebnds,
        store_with=varid)
# ------------------------------------------------------------------


def handle(infiles, tables, user_input_path, **kwargs):
    """
    Parameters
    ----------
        infiles (List): a list of strings of file names for the raw input data
        tables (str): path to CMOR tables
        user_input_path (str): path to user input json file
    Returns
    -------
        var name (str): the name of the processed variable after processing is complete
    """
    logger = logging.getLogger()
    msg = '{}: Starting'.format(VAR_NAME)
    logger.info(msg)

    serial = kwargs.get('serial')
    logdir = kwargs.get('logdir')
    if kwargs.get('simple'):
        msg = f"{VAR_NAME} is not supported for simple conversion"
        print_message(msg)
        return

    # check that we have some input files for every variable
    zerofiles = False
    for variable in RAW_VARIABLES:
        if len(infiles[variable]) == 0:
            msg = '{}: Unable to find input files for {}'.format(
                VAR_NAME, variable)
            print_message(msg)
            logging.error(msg)
            zerofiles = True
    if zerofiles:
        return None

    # Create the logging directory and setup cmor
    logdir = kwargs.get('logdir')
    if logdir:
        logpath = logdir
    else:
        outpath, _ = os.path.split(logger.__dict__['handlers'][0].baseFilename)
        logpath = os.path.join(outpath, 'cmor_logs')
    os.makedirs(logpath, exist_ok=True)

    logfile = os.path.join(logpath, VAR_NAME + '.log')

    cmor.setup(
        inpath=tables,
        netcdf_file_action=cmor.CMOR_REPLACE,
        logfile=logfile)

    cmor.dataset_json(str(user_input_path))
    cmor.load_table(str(TABLE))

    msg = '{}: CMOR setup complete'.format(VAR_NAME)
    logging.info(msg)

    data = {}

    # assuming all year ranges are the same for every variable
    num_files_per_variable = len(infiles[RAW_VARIABLES[0]])

    # sort the input files for each variable
    for var_name in RAW_VARIABLES:
        infiles[var_name].sort()

    for index in range(num_files_per_variable):

        # load data for each variable
        for var_name in RAW_VARIABLES:

            # extract data from the input file
            msg = '{name}: loading {variable}'.format(
                name=VAR_NAME,
                variable=var_name)
            logger.info(msg)

            filename = infiles[var_name][index]
            new_data = {}

            if not os.path.exists(filename):
                raise IOError("File not found: {}".format(filename))

            ds = xr.open_dataset(filename, decode_times=False)
            data[variable] = ds[var_name]


            # load the lon and lat info & bounds
            # load time & time bounds
            if var_name == 'PS':
                data.update({
                    'ps': ds['PS'],
                    'lat': ds['lat'],
                    'lon': ds['lon'],
                    'lat_bnds': ds['lat_bnds'],
                    'lon_bnds': ds['lon_bnds'],
                    'time2': ds['time'],
                    'time_bnds': ds['time_bnds']
                })

            if 'lev' in ds.dims and 'ilev' in ds.dims:
                data.update({
                    'lev': ds['lev'].values/1000,
                    'ilev': ds['ilev'].values/1000
                })
            new_data = {i: ds[i] for i in ['hyam', 'hybm', 'hyai', 'hybi'] 
                                 if i in ds.data_vars
                       }

            data.update(new_data)

        msg = '{name}: loading axes'.format(name=VAR_NAME)
        logger.info(msg)
        
        # create the cmor variable and axis
        axes = [{
            str('table_entry'): 'time2',
            str('units'): data['time'].units
        }, {
            str('table_entry'): str('standard_hybrid_sigma'),
            str('units'): str('1'),
            str('coord_vals'): data['lev'],
            str('cell_bounds'): data['ilev']
        }, {
            str('table_entry'): str('latitude'),
            str('units'): ds['lat'].units,
            str('coord_vals'): data['lat'].values,
            str('cell_bounds'): data['lat_bnds'].values
        }, {
            str('table_entry'): str('longitude'),
            str('units'): ds['lon'].units,
            str('coord_vals'): data['lon'].values,
            str('cell_bounds'): data['lon_bnds'].values
        }]

        axis_ids = list()
        for axis in axes:
            axis_id = cmor.axis(**axis)
            axis_ids.append(axis_id)

        # add hybrid level formula terms
        cmor.zfactor(
            zaxis_id=axis_ids[1],
            zfactor_name='a_half',
            axis_ids=[axis_ids[1], ],
            zfactor_values=data['hyam'].values)
        cmor.zfactor(
            zaxis_id=axis_ids[1],
            zfactor_name='b_half',
            axis_ids=[axis_ids[1], ],
            zfactor_values=data['hybm'].values)
        cmor.zfactor(
            zaxis_id=axis_ids[1],
            zfactor_name='p0',
            units='Pa',
            zfactor_values=100000)
        ips = cmor.zfactor(
            zaxis_id=axis_ids[1],
            zfactor_name='ps2',
            axis_ids=[0, 2, 3],
            units='Pa')

        data['ips'] = ips

        varid = cmor.variable(VAR_NAME, VAR_UNITS, axis_ids[:4])

        # write out the data
        msg = "{}: time {:1.1f} - {:1.1f}".format(
            VAR_NAME,
            data['time_bnds'].values[0][0],
            data['time_bnds'].values[-1][-1])
        logger.info(msg)

        if serial:
            pbar = tqdm(total=ds['time2'].shape[0])

        for index, val in enumerate(data['time2'].values):
            write_data(
                varid=varid,
                data=data,
                timeval=val,
                timebnds=[data['time_bnds'].values[index, :]],
                index=index,
                RAW_VARIABLES=RAW_VARIABLES)
            if serial:
                pbar.update(1)
        if serial:
            pbar.close()

    msg = '{}: write complete, closing'.format(VAR_NAME)
    logger.debug(msg)

    cmor.close()

    msg = '{}: file close complete'.format(VAR_NAME)
    logger.debug(msg)

    return 'phalf'

# ------------------------------------------------------------------
