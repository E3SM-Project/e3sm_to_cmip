"""
SOILICE to mrfso converter
"""
from __future__ import absolute_import, division, print_function, unicode_literals

import cmor
import cdms2
import logging
import numpy as np

from e3sm_to_cmip.lib import handle_variables

# list of raw variable names needed
RAW_VARIABLES = [str('SOILICE')]
VAR_NAME = str('mrfso')
VAR_UNITS = str('kg m-2')
TABLE = str('CMIP6_Lmon.json')


def write_data(varid, data, timeval, timebnds, index, **kwargs):
    """
    mrfso = verticalSum(SOILICE, capped_at=5000)
    """
    # we only care about data with a value greater then 0
    mask = np.greater(data['SOILICE'][index, :], 0.0)

    # sum the data over the levgrnd axis
    outdata = np.sum(
        data['SOILICE'][index, :],
        axis=0)
    
    # replace all values greater then 5k with 5k
    capped = np.where(
        np.greater(outdata, 5000.0),
        5000.0,
        outdata)
    outdata = np.where(
        mask,
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
