"""
QSOIL to evspsblsoi converter
"""
from __future__ import absolute_import, division, print_function, unicode_literals

import os
import cmor
import cdms2
import logging

from e3sm_to_cmip.util import print_message, setup_cmor

# list of raw variable names needed
RAW_VARIABLES = ['QSOIL']

# output variable name
VAR_NAME = 'evspsblsoi'.encode('ascii')
VAR_UNITS = 'kg m-2 s-1'.encode('ascii')

def handle(infiles, tables, user_input_path):
    """
    Transform E3SM.QSOIL into CMIP.evspsblsoi

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
    qsoil = f(RAW_VARIABLES[0])
    lat = qsoil.getLatitude()[:]
    lon = qsoil.getLongitude()[:]
    lat_bnds = f('lat_bnds')
    lon_bnds = f('lon_bnds')
    time = qsoil.getTime()
    time_bnds = f('time_bounds')
    f.close()

    # setup cmor
    setup_cmor(
        VAR_NAME,
        tables,
        'CMIP6_Lmon.json',
        user_input_path)

    # create axes
    axes = [{
        str('table_entry'): str('time'),
        str('units'): time.units
    }, {
        str('table_entry'): str('latitude'),
        str('units'): str('degrees_north'),
        str('coord_vals'): lat[:],
        str('cell_bounds'): lat_bnds[:]
    }, {
        str('table_entry'): str('longitude'),
        str('units'): str('degrees_east'),
        str('coord_vals'): lon[:],
        str('cell_bounds'): lon_bnds[:]
    }]
    axis_ids = list()
    for axis in axes:
        axis_id = cmor.axis(**axis)
        axis_ids.append(axis_id)

    # create the cmor variable
    varid = cmor.variable(VAR_NAME, VAR_UNITS, axis_ids, positive='up')

    # write out the data
    try:
        for index, val in enumerate(qsoil.getTime()[:]):
            data = qsoil[index, :]
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
