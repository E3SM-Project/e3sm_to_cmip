"""
PRECC to prc converter
"""
from __future__ import absolute_import, division, print_function, unicode_literals

import os
import cmor
import cdms2
import logging

from tqdm import tqdm
from e3sm_to_cmip.util import print_message
from e3sm_to_cmip.util import get_dimension_data
from e3sm_to_cmip.util import setup_cmor
from e3sm_to_cmip.util import load_axis
from e3sm_to_cmip.lib import handle_variables

# list of raw variable names needed
RAW_VARIABLES = [str('PRECC')]
VAR_NAME = str('prc')
VAR_UNITS = str('kg m-2 s-1')
TABLE = str('CMIP6_Amon.json')

def write_pr(varid, data, timeval, timebnds, index):
    """
    prc = PRECC * 1000.0
    """
    out_data = data['PRECC'][index, :] * 1000.0
    cmor.write(
        varid,
        out_data,
        time_vals=timeval,
        time_bnds=timebnds)

def handle(infiles, tables, user_input_path, serial=None):
    """
    Transform E3SM.TS into CMIP.ts

    Parameters
    ----------
        infiles (List): a list of strings of file names for the raw input data
        tables (str): path to CMOR tables
        user_input_path (str): path to user input json file
    Returns
    -------
        var name (str): the name of the processed variable after processing is complete
    """

    handle_variables(
        metadata_path=user_input_path,
        tables=tables,
        table=TABLE,
        infiles=infiles,
        raw_variables=RAW_VARIABLES,
        write_data=write_pr,
        outvar_name=VAR_NAME,
        outvar_units=VAR_UNITS,
        serial=serial)

    return VAR_NAME
# ------------------------------------------------------------------