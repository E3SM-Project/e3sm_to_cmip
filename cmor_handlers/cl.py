"""
CLOUD to cl converter
"""
from __future__ import absolute_import, division, print_function, \
    unicode_literals

import os
import cmor
import cdms2
import logging

from e3sm_to_cmip.util import print_message

# list of raw variable names needed
RAW_VARIABLES = ['CLOUD']

# output variable name
VAR_NAME = 'cl'
VAR_UNITS = '%'

def handle(infiles, tables, user_input_path):
    """
    Transform E3SM.CLOUD into CMIP.cl

    Parameters
    ----------
        infiles (List): a list of strings of file names for the raw input data
        tables (str): path to CMOR tables
        user_input_path (str): path to user input json file
    Returns
    -------
        var name (str): the name of the processed variable after processing is complete
    """
    msg = 'Starting {name}'.format(name=__name__)
    logging.info(msg)

    # extract data from the input file
    f = cdms2.open(infiles[0])
    cloud = f(RAW_VARIABLES[0])
    lat = cloud.getLatitude()[:]
    lon = cloud.getLongitude()[:]
    lat_bnds = f('lat_bnds')
    lon_bnds = f('lon_bnds')
    time = cloud.getTime()
    time_bnds = f('time_bnds')
    f.close()

    # setup cmor
    logfile = os.path.join(os.getcwd(), 'logs')
    if not os.path.exists(logfile):
        os.makedirs(logfile)
    logfile = os.path.join(logfile, VAR_NAME + '.log')
    cmor.setup(
        inpath=tables,
        netcdf_file_action=cmor.CMOR_REPLACE,
        logfile=logfile)
    cmor.dataset_json(user_input_path)
    table = 'CMIP6_Amon.json'
    try:
        cmor.load_table(table)
    except (Exception, BaseException):
        raise Exception('Unable to load table from {}'.format(__name__))

    # create axes
    axes = [{
        'table_entry': 'time',
        'units': time.units
    }, {
        'table_entry': 'latitude',
        'units': 'degrees_north',
        'coord_vals': lat[:],
        'cell_bounds': lat_bnds[:]
    }, {
        'table_entry': 'longitude',
        'units': 'degrees_east',
        'coord_vals': lon[:],
        'cell_bounds': lon_bnds[:]
    }]
    axis_ids = list()
    for axis in axes:
        axis_id = cmor.axis(**axis)
        axis_ids.append(axis_id)

    # create the cmor variable
    varid = cmor.variable(VAR_NAME, VAR_UNITS, axis_ids)

    # write out the data
    try:
        for index, val in enumerate(cloud.getTime()[:]):
            data = cloud[index, :] * 100.0
            cmor.write(
                varid,
                data,
                time_vals=val,
                time_bnds=[time_bnds[index, :]])
    except Exception as error:
        logging.error("Error in {}".format(VAR_NAME))
        return ""
    finally:
        cmor.close(varid)
    return VAR_NAME
