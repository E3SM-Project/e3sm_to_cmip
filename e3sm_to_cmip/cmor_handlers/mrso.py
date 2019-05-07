"""
SOILICE, SOILIQ to mrso converter
"""
from __future__ import absolute_import, division, print_function, unicode_literals

import os
import cmor
import cdms2
import logging
import numpy as np

from e3sm_to_cmip.lib import handle_variables

# list of raw variable names needed
RAW_VARIABLES = [str('SOILICE'), str('SOILLIQ')]
VAR_NAME = str('mrso')
VAR_UNITS = str('kg m-2')
TABLE = str('CMIP6_Lmon.json')


def write_data(varid, data, timeval, timebnds, index, **kwargs):
    """
    mrso = verticalSum(SOILICE + SOILLIQ, capped_at=5000)
    """
    icemask = np.greater(data['SOILICE'][index, :], 0.0)
    liqmask = np.greater(data['SOILLIQ'][index, :], 0.0)
    total_mask = np.logical_or(icemask, liqmask)

    outdata = np.sum(
        data['SOILICE'][index, :] + data['SOILLIQ'][index, :],
        axis=0)
    capped = np.where(
        np.greater(outdata, 5000.0),
        5000.0,
        outdata)
    outdata = np.where(
        total_mask,
        capped,
        outdata)
    cmor.write(
        varid,
        outdata,
        time_vals=timeval,
        time_bnds=timebnds)


def handle(infiles, tables, user_input_path, **kwargs):
    return handle_variables(
        metadata_path=user_input_path,
        tables=tables,
        table=TABLE,
        infiles=infiles,
        raw_variables=RAW_VARIABLES,
        write_data=write_data,
        outvar_name=VAR_NAME,
        outvar_units=VAR_UNITS,
        serial=kwargs.get('serial'))
# ------------------------------------------------------------------
