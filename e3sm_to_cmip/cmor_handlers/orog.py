"""
PHIS to orog converter
"""
from __future__ import absolute_import, division, print_function, unicode_literals

import cmor
import os
import logging
import cdms2
from e3sm_to_cmip.util import print_message
from e3sm_to_cmip.lib import handle_variables

# list of raw variable names needed
RAW_VARIABLES = [str('PHIS')]
VAR_NAME = str('orog')
VAR_UNITS = str('m')
TABLE = str('CMIP6_fx.json')


def handle(infiles, tables, user_input_path, **kwargs):
    logger = logging.getLogger()
    msg = '{}: Starting'.format(VAR_NAME)
    logger.info(msg)

    logdir = kwargs.get('logdir')
    serial = kwargs.get('serial')

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

    # extract data from the input file
    msg = 'orog: loading PHIS'
    logger.info(msg)

    filename = infiles['PHIS'][0]

    if not os.path.exists(filename):
        raise IOError("File not found: {}".format(filename))

    f = cdms2.open(filename)

    # load the data for each variable
    variable_data = f('PHIS')

    if not variable_data.any():
        raise IOError("Variable data not found: {}".format(variable))

    # load the lon and lat info & bounds
    data = {
        'lat': variable_data.getLatitude(),
        'lon': variable_data.getLongitude(),
        'lat_bnds': f('lat_bnds'),
        'lon_bnds': f('lon_bnds'),
        'PHIS': f('PHIS')
    }

    msg = '{name}: loading axes'.format(name=VAR_NAME)
    logger.info(msg)

    axes = [{
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

    msg = 'orog: running CMOR'
    logging.info(msg)

    axis_ids = list()
    for axis in axes:
        axis_id = cmor.axis(**axis)
        axis_ids.append(axis_id)

    varid = cmor.variable(VAR_NAME, VAR_UNITS, axis_ids)

    g = 9.80616

    outdata = data['PHIS'] / g
    cmor.write(
        varid,
        outdata)

    msg = '{}: write complete, closing'.format(VAR_NAME)
    logger.debug(msg)

    cmor.close()

    msg = '{}: file close complete'.format(VAR_NAME)
    logger.debug(msg)

    return 'orog'
