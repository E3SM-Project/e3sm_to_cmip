"""
LAISHA, LAISUN to lai converter
"""
from __future__ import absolute_import, division, print_function, unicode_literals

import cmor
from e3sm_to_cmip.lib import handle_variables

# list of raw variable names needed
RAW_VARIABLES = [str('LAISHA'), str('LAISUN')]
VAR_NAME = str('lai')
VAR_UNITS = str('1')
TABLE = str('CMIP6_Lmon.json')


def write_data(varid, data, timeval, timebnds, index, **kwargs):
    """
    lai = LAISHA + LAISUN
    """
    outdata = data['LAISHA'][index, :] + data['LAISUN'][index, :]
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
