"""
CLDLIQ to clw converter
"""
from __future__ import absolute_import, division, print_function, unicode_literals

import os
import cmor
import cdms2
import logging


from e3sm_to_cmip.util import print_message
from e3sm_to_cmip.lib import get_dimension_data
from e3sm_to_cmip.util import setup_cmor
from e3sm_to_cmip.lib import load_axis
from e3sm_to_cmip.lib import handle_variables

# list of raw variable names needed
RAW_VARIABLES = [str('CLDLIQ')]
VAR_NAME = str('clw')
VAR_UNITS = str('kg kg-1')
TABLE = str('CMIP6_Amon.json')
LEVELS = {
    'name': 'standard_hybrid_sigma',
    'units': '1'
}

def write_data(varid, data, timeval, timebnds, index):
    """
    clw = CLDLIQ
    """
    outdata = data['CLDLIQ'][index, :]
    cmor.write(
        varid,
        outdata,
        time_vals=timeval,
        time_bnds=timebnds)
    cmor.write(
        data['ips'],
        data['ps'],
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

    handle_variables(
        metadata_path=user_input_path,
        tables=tables,
        table=TABLE,
        infiles=infiles,
        raw_variables=RAW_VARIABLES,
        write_data=write_data,
        outvar_name=VAR_NAME,
        outvar_units=VAR_UNITS,
        serial=kwargs.get('serial'),
        levels=LEVELS)

    return VAR_NAME
# ------------------------------------------------------------------
