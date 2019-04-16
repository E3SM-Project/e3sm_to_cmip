"""
CLOUD to cl converter
"""
from __future__ import absolute_import, division, print_function, unicode_literals

import os
import cmor
import cdms2
import logging

from tqdm import tqdm
from e3sm_to_cmip.util import print_message
from e3sm_to_cmip.util import get_dimension_data
from e3sm_to_cmip.util import setup_cmor
from e3sm_to_cmip.util import load_axis
from e3sm_to_cmip.lib import handle_variables

# list of raw variable names needed
RAW_VARIABLES = [str('CLOUD')]
VAR_NAME = str('cl')
VAR_UNITS = str('%')
TABLE = str('CMIP6_Amon.json')

def handle(infiles, tables, user_input_path, position=None):
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
    dims_loaded = False

    # setup cmor
    setup_cmor(
        VAR_NAME,
        tables,
        'CMIP6_Amon.json',
        user_input_path)
    
    msg = '{}: CMOR setup complete'.format(__name__)
    logging.info(msg)
    
    try:
        for infile in sorted(infiles[RAW_VARIABLES[0]]):
        # extract data from the input file
        f = cdms2.open(infile)

        data = f[RAW_VARIABLES[0]]

        lat = cloud.getLatitude()
        lon = cloud.getLongitude()

        lat_bnds = f('lat_bnds')
        lon_bnds = f('lon_bnds')
        
        time = cloud.getTime()
        time_bnds = f('time_bnds')
        
        lev = f.getAxis('lev')[:]/1000
        ilev = f.getAxis('ilev')[:]/1000
        ps = f('PS')[:]
        p0 = f('P0')
        hyam = f('hyam')
        hyai = f('hyai')
        hybm = f('hybm')
        hybi = f('hybi')

        f.close()


        # create axes
        axes = [{
            str('table_entry'): str('time'),
            str('units'): time.units
        }, {
            str('table_entry'): str('standard_hybrid_sigma'),
            str('units'): str('1'),
            str('coord_vals'): lev,
            str('cell_bounds'): ilev
        }, {
            str('table_entry'): str('latitude'),
            str('units'): str(lat.units),
            str('coord_vals'): lat[:],
            str('cell_bounds'): lat_bnds[:]
        }, {
            str('table_entry'): str('longitude'),
            str('units'): str(lon.units),
            str('coord_vals'): lon[:],
            str('cell_bounds'): lon_bnds[:]
        }]

        axis_ids = list()
        for axis in axes:
            axis_id = cmor.axis(**axis)
            axis_ids.append(axis_id)

        cmor.zfactor(
            zaxis_id=axis_ids[1],
            zfactor_name=str('a'),
            axis_ids=[axis_ids[1], ],
            zfactor_values=hyam,
            zfactor_bounds=hyai)
        cmor.zfactor(
            zaxis_id=axis_ids[1],
            zfactor_name=str('b'),
            axis_ids=[axis_ids[1], ],
            zfactor_values=hybm,
            zfactor_bounds=hybi)
        cmor.zfactor(
            zaxis_id=axis_ids[1],
            zfactor_name=str('p0'),
            units=str('Pa'),
            zfactor_values=p0)
        ips = cmor.zfactor(
            zaxis_id=axis_ids[1],
            zfactor_name=str('ps'),
            axis_ids=[axis_ids[0], axis_ids[2], axis_ids[3]],
            units=str('Pa'))

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
                cmor.write(
                    ips, 
                    ps, 
                    time_vals=val,
                    time_bnds=[time_bnds[index, :]],
                    store_with=varid)
        except Exception as error:
            logging.error("Error in {}".format(VAR_NAME))
            return ""
        finally:
            cmor.close(varid)
    return VAR_NAME
