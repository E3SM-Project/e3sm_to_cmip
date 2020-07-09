"""
TSOI to tsl converter
"""
from __future__ import absolute_import, division, print_function, unicode_literals

import os
import cmor
import cdms2
import logging
from tqdm import tqdm
logger = logging.getLogger()

from e3sm_to_cmip.util import print_message, setup_cmor, get_levgrnd_bnds


# list of raw variable names needed
RAW_VARIABLES = [str('TSOI')]
VAR_NAME = str('tsl')
VAR_UNITS = str('K')
TABLE = 'CMIP6_Lmon.json'
LEVELS = {
    'name': 'sdepth',
    'units': 'm',
    'e3sm_axis_name': 'levgrnd'
}


def write_data(varid, data, timeval, timebnds, index, **kwargs):
    cmor.write(
        varid,
        data['TSOI'][index, :],
        time_vals=timeval,
        time_bnds=timebnds)
# ------------------------------------------------------------------


def handle(infiles, tables, user_input_path, **kwargs):
    """
    Transform E3SM.TSOI into CMIP.tsl

    Parameters
    ----------
        infiles (List): a list of strings of file names for the raw input data
        tables (str): path to CMOR tables
        user_input_path (str): path to user input json file
    Returns
    -------
        var name (str): the name of the processed variable after processing is complete
    """
    msg = f'{VAR_NAME}: Starting'
    logger.info(msg)

    nonzero = False
    for variable in RAW_VARIABLES:
        if len(infiles[variable]) == 0:
            msg = f'{VAR_NAME}: Unable to find input files for {variable}'
            print_message(msg)
            logging.error(msg)
            nonzero = True
    if nonzero:
        return None

    msg = f'{VAR_NAME}: running with input files: {infiles}'
    logger.debug(msg)

    # setup cmor
    logdir = kwargs.get('logdir')
    if logdir:
        logfile = logfile = os.path.join(logdir, f"{VAR_NAME}.log")
    else:
        logfile = os.path.join(os.getcwd(), 'logs')
        if not os.path.exists(logfile):
            os.makedirs(logfile)
        logfile = os.path.join(logfile, f"{VAR_NAME}.log")

    cmor.setup(
        inpath=tables,
        netcdf_file_action=cmor.CMOR_REPLACE,
        logfile=logfile)
    cmor.dataset_json(user_input_path)
    cmor.load_table(TABLE)

    msg = f'{VAR_NAME}: CMOR setup complete'
    logger.info(msg)

    data = {}

    # assuming all year ranges are the same for every variable
    num_files_per_variable = len(infiles['TSOI'])

    # sort the input files for each variable
    infiles['TSOI'].sort()

    for index in range(num_files_per_variable):

        f = cdms2.open(infiles['TSOI'][index])

        # load the data for each variable
        variable_data = f('TSOI')

        levgrnd = variable_data.getAxis(1)[:]
        levgrnd_bnds = get_levgrnd_bnds()

        # load
        data = {
            'TSOI': variable_data,
            'lat': variable_data.getLatitude(),
            'lon': variable_data.getLongitude(),
            'lat_bnds': f('lat_bnds'),
            'lon_bnds': f('lon_bnds'),
            'time': variable_data.getTime(),
            'time_bnds': f('time_bounds'),
            'levgrnd': levgrnd,
            'levgrnd_bnds': levgrnd_bnds
        }

        # create the cmor variable and axis
        axes = [{
            str('table_entry'): str('time'),
            str('units'): data['time'].units
        }, {
            str('table_entry'): str('sdepth'),
            str('units'): str('m'),
            str('coord_vals'): levgrnd,
            str('cell_bounds'): levgrnd_bnds
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

        axis_ids = list()
        for axis in axes:
            axis_id = cmor.axis(**axis)
            axis_ids.append(axis_id)

        varid = cmor.variable(VAR_NAME, VAR_UNITS, axis_ids)

        # write out the data
        msg = f"{VAR_NAME}: writing {data['time_bnds'][0][0]} - {data['time_bnds'][-1][-1]}"
        logger.info(msg)

        serial = kwargs.get('serial')
        if serial:
            pbar = tqdm(total=len(data['time']))
            pbar.set_description(msg)
        
        for index, val in enumerate(data['time']):
            cmor.write(
                varid,
                data['TSOI'][index, :],
                time_vals=val,
                time_bnds=[data['time_bnds'][index, :]])
            if serial:
                pbar.update(1)
        if serial:
            pbar.close()

    msg = f'{VAR_NAME}: write complete, closing'
    logger.info(msg)

    cmor.close()
    msg = f'{VAR_NAME}: file close complete'
    logger.info(msg)

    return VAR_NAME
# ------------------------------------------------------------------
