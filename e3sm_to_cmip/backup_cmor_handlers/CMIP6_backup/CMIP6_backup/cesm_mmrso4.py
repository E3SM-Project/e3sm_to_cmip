"""
so4_a1, so4_c1, so4_a2, so4_c2, so4_a3, so4_c3 to mmrso4 converter
"""
from __future__ import absolute_import, division, print_function, unicode_literals

import cmor
from e3sm_to_cmip.lib import handle_variables

# list of raw variable names needed
RAW_VARIABLES = [str('so4_a1'), str('so4_c1'), str('so4_a2'), str('so4_c2'), str('so4_a3'), str('so4_c3')]
VAR_NAME = str('mmrso4')
VAR_UNITS = str('kg kg-1')
TABLE = str('CMIP6_AERmon.json')
LEVELS = {
    'name': 'standard_hybrid_sigma',
    'units': '1',
    'e3sm_axis_name': 'lev',
    'e3sm_axis_bnds': 'ilev'
}


def write_data(varid, data, timeval, timebnds, index, **kwargs):
    """
    mmrso4 = so4_a1+so4_c1+so4_a2+so4_c2+so4_a3+so4_c3
    """
    outdata = data['so4_a1'][index, :] + data['so4_c1'][index, :] + data['so4_a2'][index, :] \
	+ data['so4_c2'][index, :] + data['so4_a3'][index, :] + data['so4_c3'][index, :]

    if kwargs.get('simple'):
        return outdata
    cmor.write(
        varid,
        outdata,
        time_vals=timeval,
        time_bnds=timebnds)
    cmor.write(
        data['ips'],
        data['ps'],
        time_vals=timeval,
        time_bnds=timebnds,
        store_with=varid)
    return outdata
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
        logdir=kwargs.get('logdir'),
        simple=kwargs.get('simple'),
        outpath=kwargs.get('outpath'))
# ------------------------------------------------------------------
