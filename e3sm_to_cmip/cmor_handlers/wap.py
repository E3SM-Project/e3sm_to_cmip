"""
OMEGA to wap converter
"""
from __future__ import absolute_import, division, print_function, unicode_literals

import cmor
import numpy as np
from e3sm_to_cmip.lib import handle_variables
from e3sm_to_cmip.cmor_handlers import FILL_VALUE

# list of raw variable names needed
RAW_VARIABLES = [str('OMEGA')]
VAR_NAME = str('wap')
VAR_UNITS = str("Pa s-1")
TABLE = str('CMIP6_Amon.json')
LEVELS = {
    'name': str('plev19'),
    'units': str('Pa'),
    'e3sm_axis_name': 'plev'
}


def write_data(varid, data, timeval, timebnds, index, **kwargs):
    outdata = data[RAW_VARIABLES[0] ][index, :]
    outdata[np.isnan(outdata)] = FILL_VALUE
    if kwargs.get('simple'):
        return outdata
    cmor.write(
        varid,
        outdata,
        time_vals=timeval,
        time_bnds=timebnds)
# ------------------------------------------------------------------


def handle(infiles, tables, user_input_path, **kwargs):

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
# ------------------------------------------------------------------
