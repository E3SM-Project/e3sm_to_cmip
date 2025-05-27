"""
FLUT to rlutconverter
"""
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import cmor
import numpy as np
from e3sm_to_cmip.cmor_handlers import FILL_VALUE
from e3sm_to_cmip.lib import handle_variables

# list of raw variable names needed
RAW_VARIABLES = [str('FLUT')]
VAR_NAME = str('rlut')
VAR_UNITS = str('W m-2')
TABLE = str('QUOCA_day.json')
POSITIVE = str('up')

def write_data(varid, data, timeval, timebnds, index, **kwargs):
    """
    rlut = FLUT
    """
    outdata = data['FLUT'][index, :].values
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
        positive=POSITIVE,
        serial=kwargs.get('serial'),
        logdir=kwargs.get('logdir'),
        simple=kwargs.get('simple'),
        outpath=kwargs.get('outpath'))
# ------------------------------------------------------------------
