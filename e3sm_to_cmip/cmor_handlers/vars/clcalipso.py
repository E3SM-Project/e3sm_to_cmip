"""
CLD_CAL to clcalipso converter
"""
from __future__ import absolute_import, division, print_function, unicode_literals

import cmor

from e3sm_to_cmip.lib import handle_variables

# list of raw variable names needed
RAW_VARIABLES = [str("CLD_CAL")]
VAR_NAME = str("clcalipso")
VAR_UNITS = str("%")
TABLE = str("CMIP6_CFmon.json")
LEVELS = {
    "name": "alt40",
    "units": "m",
    "e3sm_axis_name": "cosp_ht",
    "e3sm_axis_bnds": "cosp_ht_bnds",
}


def write_data(varid, data, timeval, timebnds, index, **kwargs):
    """
    clcalipso = CLD_CAL with alt40 levels
    """
    if kwargs.get("simple"):
        return data[RAW_VARIABLES[0]][index, :].values
    cmor.write(
        varid,
        data[RAW_VARIABLES[0]][index, :].values,
        time_vals=timeval,
        time_bnds=timebnds,
    )
    return data[RAW_VARIABLES[0]][index, :].values


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
        table=kwargs.get("table", TABLE),
        infiles=infiles,
        raw_variables=RAW_VARIABLES,
        write_data=write_data,
        outvar_name=VAR_NAME,
        outvar_units=VAR_UNITS,
        serial=kwargs.get("serial"),
        levels=LEVELS,
        logdir=kwargs.get("logdir"),
        simple=kwargs.get("simple"),
        outpath=kwargs.get("outpath"),
    )


# ------------------------------------------------------------------
