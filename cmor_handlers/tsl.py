"""
TSOI to tsl converter
"""
import os
import cmor
import cdms2
import logging

from lib.util import print_message
from resources.levgrnd_bnds import levgrnd_bnds

# list of raw variable names needed
RAW_VARIABLES = ['TSOI']

# output variable name
VAR_NAME = 'tsl'
VAR_UNITS = 'K'

def handle(infiles, tables, user_input_path):
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

    msg = 'Starting {name}'.format(name=__name__)
    logging.info(msg)

    # open input file and pull out the bounds info
    f = cdms2.open(infiles[0])
    tsoi = f(RAW_VARIABLES[0])
    lat = tsoi.getLatitude()[:]
    lon = tsoi.getLongitude()[:]
    lat_bnds = f('lat_bnds')
    lon_bnds = f('lon_bnds')
    time = tsoi.getTime()
    time_bnds = f('time_bounds')
    levgrnd = tsoi.getAxis(1)[:]
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
    table = 'CMIP6_Lmon.json'
    try:
        cmor.load_table(table)
    except:
        raise Exception('Unable to load table from {}'.format(__name__))

    # create axes
    axes = [{
        'table_entry': 'time',
        'units': time.units
    }, {
        'table_entry': 'sdepth',
        'units': 'm',
        'coord_vals': tsoi.getAxis(1)[:],
        'cell_bounds': levgrnd_bnds
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
        for index, val in enumerate(tsoi.getTime()[:]):
            data = tsoi[index, :]
            cmor.write(
                varid,
                data,
                time_vals=val,
                time_bnds=[time_bnds[index, :]])
    except Exception as error:
        logging.error("Error in {}".format(VAR_NAME))
    finally:
        cmor.close(varid)
    return VAR_NAME
