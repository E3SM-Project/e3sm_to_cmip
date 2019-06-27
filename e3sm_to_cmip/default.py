from __future__ import absolute_import, division, print_function, unicode_literals
from e3sm_to_cmip.lib import handle_variables
import cmor


def default_handler(infiles, tables, user_input_path, **kwargs):
    RAW_VARIABLES = kwargs['raw_variables']

    def write_data(varid, data, timeval, timebnds, index, **kwargs):
        cmor.write(
            varid,
            data[ RAW_VARIABLES[0] ][index, :],
            time_vals=timeval,
            time_bnds=timebnds)
            
    return handle_variables(
        metadata_path=user_input_path,
        tables=tables,
        table=kwargs['table'],
        infiles=infiles,
        raw_variables=RAW_VARIABLES,
        write_data=write_data,
        outvar_name=kwargs['name'],
        outvar_units=kwargs['units'],
        serial=kwargs.get('serial'),
        positive=kwargs.get('positive'),
        logdir=kwargs.get('logdir'))
# ------------------------------------------------------------------