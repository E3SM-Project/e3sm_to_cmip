"""
TSOI to tsl converter
"""
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import cmor
import numpy as np
from e3sm_to_cmip.cmor_handlers import FILL_VALUE
from e3sm_to_cmip.lib import handle_variables

# list of raw variable names needed
RAW_VARIABLES = ['TSOI']
VAR_NAME = 'tsl'
VAR_UNITS = 'K'
TABLE = 'CMIP6_Lmon.json'
LEVELS = {
    'name': 'sdepth',
    'units': 'm',
    'e3sm_axis_name': 'levgrnd',
    'e3sm_axis_bnds': 'levgrnd_bnds'
}


def write_data(varid, data, timeval, timebnds, index, **kwargs):
    outdata = data['TSOI'][index, :].values
    outdata[np.isnan(outdata)] = FILL_VALUE

    if kwargs.get('simple'):
        return outdata
    cmor.write(
        varid,
        outdata,
        time_vals=timeval,
        time_bnds=timebnds)

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
    return handle_variables(
        metadata_path=user_input_path,
        tables=tables,
        table=kwargs.get('table', TABLE),
        infiles=infiles,
        raw_variables=RAW_VARIABLES,
        write_data=write_data,
        outvar_name=VAR_NAME,
        outvar_units=VAR_UNITS,
        serial=kwargs.get('serial'),
        levels=LEVELS,
        logdir=kwargs.get('logdir'),
        simple=kwargs.get('simple'),
        outpath=kwargs.get('outpath'))
