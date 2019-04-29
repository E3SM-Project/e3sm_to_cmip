"""
Convert from E3SM variable name to CMIP6 equivalent, no data transformation
"""
from __future__ import absolute_import, division, print_function, unicode_literals

import cmor

from e3sm_to_cmip.lib import handle_variables

# ------------------------------------------------------------------


def handle_default(infiles, tables, user_input_path, **kwargs):
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
    
    def write_data(varid, data, timeval, timebnds, index):
        """
        Write out whatever the first raw variable listed is named
        """
        cmor.write(
            varid,
            data[ kwargs.get('raw_variables')[0] ][index, :],
            time_vals=timeval,
            time_bnds=timebnds)

    handle_variables(
        metadata_path=user_input_path,
        tables=tables,
        infiles=infiles,
        write_data=write_data,
        table=kwargs.get('table'),
        serial=kwargs.get('serial'),
        positive=kwargs.get('positive'),
        outvar_name=kwargs.get('name'),
        outvar_units=kwargs.get('units'),
        raw_variables=kwargs.get('raw_variables'))

    return kwargs.get('var_name')
# ------------------------------------------------------------------
