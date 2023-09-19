from __future__ import absolute_import, division, print_function, unicode_literals

import cmor
import numpy as np

from e3sm_to_cmip.lib import handle_variables


def default_handler(var_to_filepaths, tables, user_input_path, **kwargs):  # noqa: C901
    RAW_VARIABLES = kwargs["raw_variables"]
    unit_conversion = kwargs.get("unit_conversion")

    def write_data(varid, data, timeval=None, timebnds=None, index=None, **kwargs):
        if timeval is not None:
            if unit_conversion is not None:
                if unit_conversion == "g-to-kg":
                    outdata = data[RAW_VARIABLES[0]].values[index, :] / 1000.0
                elif unit_conversion == "1-to-%":
                    outdata = data[RAW_VARIABLES[0]].values[index, :] * 100.0
                elif unit_conversion == "m/s-to-kg/ms":
                    outdata = data[RAW_VARIABLES[0]].values[index, :] * 1000
                elif unit_conversion == "-1":
                    outdata = data[RAW_VARIABLES[0]].values[index, :] * -1
                else:
                    raise ValueError(
                        f"{unit_conversion} isn't a supported unit conversion for default variables"
                    )
            else:
                outdata = data[RAW_VARIABLES[0]].values[index, :]
        else:
            if unit_conversion is not None:
                if unit_conversion == "g-to-kg":
                    outdata = data[RAW_VARIABLES[0]].values / 1000.0
                elif unit_conversion == "1-to-%":
                    outdata = data[RAW_VARIABLES[0]].values * 100.0
                elif unit_conversion == "m/s-to-kg/ms":
                    outdata = data[RAW_VARIABLES[0]].values * 1000
                elif unit_conversion == "-1":
                    outdata = data[RAW_VARIABLES[0]].values * -1
                else:
                    raise ValueError(
                        f"{unit_conversion} isn't a supported unit conversion for default variables"
                    )
            else:
                outdata = data[RAW_VARIABLES[0]].values

        outdata[np.isnan(outdata)] = 1.0e20

        if kwargs.get("simple"):
            return outdata

        if timeval:
            cmor.write(varid, outdata, time_vals=timeval, time_bnds=timebnds)
        else:
            cmor.write(varid, outdata)

        return outdata

    return handle_variables(
        metadata_path=user_input_path,
        tables=tables,
        table=kwargs["table"],
        var_to_filepaths=var_to_filepaths,
        raw_variables=RAW_VARIABLES,
        write_data=write_data,
        outvar_name=kwargs["name"],
        outvar_units=kwargs["units"],
        serial=kwargs.get("serial"),
        positive=kwargs.get("positive"),
        logdir=kwargs.get("logdir"),
        simple=kwargs.get("simple"),
        outpath=kwargs.get("outpath"),
    )


# ------------------------------------------------------------------
