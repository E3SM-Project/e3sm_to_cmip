"""
CLDICE to cli converter
"""
from __future__ import absolute_import, division, print_function, unicode_literals

import cmor
from e3sm_to_cmip.lib import handle_variables
from cdutil.vertical import reconstructPressureFromHybrid

# list of raw variable names needed
RAW_VARIABLES = [str('CLOUD'), str('PS')]
VAR_NAME = str('phalf')
VAR_UNITS = str('Pa')
TABLE = str('CMIP6_Amon.json')
LEVELS = {
    'name': 'standard_hybrid_sigma_half',
    'units': '1',
    'e3sm_axis_name': 'lev',
    'e3sm_axis_bnds': 'ilev',
    'time_name': 'time2'
}


def write_data(varid, data, timeval, timebnds, index, **kwargs):
    """
    phalf = P0*hyai + PS*hybi
    """
    outdata = reconstructPressureFromHybrid(
        data['PS'][index, :], data['hyai'], data['hybi'], data['p0'])
    cmor.write(
        varid,
        outdata,
        time_vals=timeval,
        time_bnds=timebnds)
# ------------------------------------------------------------------


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
        levels=LEVELS,
        logdir=kwargs.get('logdir'))
# ------------------------------------------------------------------
