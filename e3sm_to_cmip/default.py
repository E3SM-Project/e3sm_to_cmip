from __future__ import absolute_import, division, print_function, unicode_literals
from e3sm_to_cmip.lib import handle_variables
import cmor

def unit_convert(data, conversion):
    if conversion == 'g-to-kg':
        return data / 1000.0
    elif conversion == '1-to-%':
        return data * 100.0
    elif conversion == 'm/s-to-kg/ms':
        return data * 1000.0
    elif conversion == '-1':
        return data * -1
    else:
        raise ValueError(f"{conversion} isnt a supported unit conversion for default variables")

def default_handler(infiles, tables, user_input_path, **kwargs):
    RAW_VARIABLES = kwargs['raw_variables']
    unit_conversion = kwargs.get('unit_conversion')
    
    def write_data(varid, data, timeval, timebnds, index, **kwargs):
        if unit_conversion:
            outdata = unit_convert(data[RAW_VARIABLES[0] ][index, :], unit_conversion)
        else:
            outdata = data[ RAW_VARIABLES[0] ][index, :]

        if kwargs.get('simple'):
            return outdata
        cmor.write(
            varid,
            outdata,
            time_vals=timeval,
            time_bnds=timebnds)
        return outdata
            
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
        logdir=kwargs.get('logdir'),
        simple=kwargs.get('simple'),
        outpath=kwargs.get('outpath'))
# ------------------------------------------------------------------