"""
SFbc_a4, bc_a4_CLXF to emibc converter
"""
from __future__ import absolute_import, division, print_function, unicode_literals

import cmor
import logging
import numpy as np

from e3sm_to_cmip.lib import handle_variables

# list of raw variable names needed
RAW_VARIABLES = [str('SFbc_a4'), str('bc_a4_CLXF')]
VAR_NAME = str('emibc')
VAR_UNITS = str('kg m-2 s-1')
TABLE = str('CMIP6_AERmon.json')

def write_data(varid, data, timeval, timebnds, index, **kwargs):
    """
    emibc = SFbc_a4 (surface emission kg/m2/s) + bc_a4_CLXF (vertically integrated molec/cm2/s) x 12.011 / 6.02214e+22
    """
    outdata = data['SFbc_a4'][index, :] + data['bc_a4_CLXF'][index, :] * 12.011/6.02214e22
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
        serial=kwargs.get('serial'),
        logdir=kwargs.get('logdir'),
        simple=kwargs.get('simple'),
        outpath=kwargs.get('outpath'))
# ------------------------------------------------------------------
