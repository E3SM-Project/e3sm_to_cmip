"""
T to ta converter
"""
from __future__ import absolute_import, division, print_function, unicode_literals

import cmor
import cdms2
import logging
logger = logging.getLogger()

from e3sm_to_cmip.lib import handle_variables
from e3sm_to_cmip.lib import load_axis

# list of raw variable names needed
RAW_VARIABLES = [str('T')]
VAR_NAME = str('ta')
VAR_UNITS = str("K")
TABLE = str('CMIP6_Amon.json')
LEVELS = {
    'name': str('plev19'),
    'units': str('Pa'),
    'e3sm_axis_name': 'plev'
}

def write_data(varid, data, timeval, timebnds, index, **kwargs):
    """
    ta = T
    """
    cmor.write(
        varid,
        data['T'][index, :],
        time_vals=timeval,
        time_bnds=timebnds)


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
