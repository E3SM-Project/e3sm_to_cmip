"""
so4_a1, so4_c1, so4_a2, so4_c2, so4_a3, so4_c3 to mmrso4 converter
"""
from __future__ import absolute_import, division, print_function, unicode_literals

import cmor
import cdms2
import logging
import numpy as np

from e3sm_to_cmip.lib import handle_variables

# list of raw variable names needed
RAW_VARIABLES = [str('so4_a1'), str('so4_c1'), str('so4_a2'), str('so4_c2'), str('so4_a3'), str('so4_c3')]
VAR_NAME = str('mmrso4')
VAR_UNITS = str('kg kg-1')
TABLE = str('CMIP6_AERmon.json')
LEVELS = {
    'name': str('plev19'),
    'units': str('Pa'),
    'e3sm_axis_name': 'plev'
}


def write_data(varid, data, timeval, timebnds, index, **kwargs):
    """
    mmrso4 = so4_a1+so4_c1+so4_a2+so4_c2+so4_a3+so4_c3
    """
    outdata = data['so4_a1'][index, :] + data['so4_c1'][index, :] + data['so4_a2'][index, :] + data['so4_c2'][index, :] + data['so4_a3'][index, :] + data['so4_c3'][index, :]

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
        table=TABLE,
        infiles=infiles,
        raw_variables=RAW_VARIABLES,
        write_data=write_data,
        outvar_name=VAR_NAME,
        outvar_units=VAR_UNITS,
	levels=LEVELS,
        serial=kwargs.get('serial'),
        logdir=kwargs.get('logdir'),
        simple=kwargs.get('simple'),
        outpath=kwargs.get('outpath'))
# ------------------------------------------------------------------
