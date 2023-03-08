"""
U10 to uas converter
"""
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import cmor
import numpy as np
from e3sm_to_cmip.cmor_handlers import FILL_VALUE
from e3sm_to_cmip.lib import handle_variables

# list of raw variable names needed
RAW_VARIABLES = [str('U10')]
VAR_NAME = str('uas')
VAR_UNITS = str('m/s')
TABLE = str('QBOi_mon.json')
##POSITIVE = str('up')

def write_data(varid, data, timeval, timebnds, index, **kwargs):
    """
    uas = U10 for eastward
    """
    outdata = data['U10'][index, :].values
    outdata[np.isnan(outdata)] = FILL_VALUE

    if kwargs.get('simple'):
        return outdata
    cmor.write(
        varid,
        outdata,
        time_vals=timeval,
        time_bnds=timebnds)

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
        logdir=kwargs.get('logdir'),
        simple=kwargs.get('simple'),
        outpath=kwargs.get('outpath'))
# ------------------------------------------------------------------
