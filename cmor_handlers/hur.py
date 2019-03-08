"""
RELHUM to hur converter
"""
from __future__ import absolute_import, division, print_function, \
    unicode_literals

import os
import cmor
import cdms2
import logging
import numpy

from e3sm_to_cmip.util import print_message, plev19, hybrid_to_plevs

# list of raw variable names needed
RAW_VARIABLES = ['RELHUM']

# output variable name
VAR_NAME = 'hur'
VAR_UNITS = '1.0'


def handle(infiles, tables, user_input_path):
    """
    Transform E3SM.RELHUM into CMIP.hur

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
    data = f(RAW_VARIABLES[0])
    lat = data.getLatitude()[:]
    lon = data.getLongitude()[:]
    lat_bnds = f('lat_bnds')
    lon_bnds = f('lon_bnds')
    time = data.getTime()
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

    # convert the pressure levels
    lev = numpy.array([100000, 92500, 85000, 70000, 60000, 50000, 40000, 30000,
                       25000, 20000, 15000, 10000, 7000, 5000, 3000, 2000, 1000, 500, 100])
    data_plev = hybrid_to_plevs(data, f('hyam'), f('hybm'), f('PS'), lev)

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
    }, {
        'table_entry': 'plev19',
        'units': 'Pa',
        'coord_vals': lev
    }]
    axis_ids = list()
    for axis in axes:
        axis_id = cmor.axis(**axis)
        axis_ids.append(axis_id)

    # create the cmor variable
    varid = cmor.variable(VAR_NAME, VAR_UNITS, axis_ids)

    # write out the data
    try:
        for index, val in enumerate(data.getTime()[:]):
            cmor.write(
                varid,
                data[index, :],
                time_vals=val,
                time_bnds=[time_bnds[index, :]])
    except Exception as error:
        logging.error("Error in {}".format(VAR_NAME))
    finally:
        cmor.close(varid)
    return VAR_NAME
