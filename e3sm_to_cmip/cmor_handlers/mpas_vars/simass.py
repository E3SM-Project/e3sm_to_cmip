"""
Sea-ice mass per area, simass
"""

from __future__ import absolute_import, division, print_function

import xarray

from e3sm_to_cmip import mpas, util
from e3sm_to_cmip._logger import _setup_child_logger

# 'MPAS' as a placeholder for raw variables needed
RAW_VARIABLES = ["MPASSI", "MPAS_mesh", "MPAS_map"]

# output variable name
VAR_NAME = "simass"
VAR_UNITS = "kg m-2"
TABLE = "CMIP6_SImon.json"


logger = _setup_child_logger(__name__)


def handle(infiles, tables, user_input_path, cmor_log_dir, **kwargs):
    """
    Transform MPASSI timeMonthly_avg_iceAreaCell and
    timeMonthly_avg_iceVolumeCell into CMIP.simass

    Parameters
    ----------
    infiles : dict
        a dictionary with namelist, mesh and time series file names

    tables : str
        path to CMOR tables

    user_input_path : str
        path to user input json file

    Returns
    -------
    varname : str
        the name of the processed variable after processing is complete
    """
    msg = "Starting {name}".format(name=__name__)
    logger.info(msg)

    mappingFileName = infiles["MPAS_map"]
    timeSeriesFiles = infiles["MPASSI"]

    rhoi = 917.0

    variableList = [
        "timeMonthly_avg_iceAreaCell",
        "timeMonthly_avg_iceVolumeCell",
        "xtime_startMonthly",
        "xtime_endMonthly",
    ]

    ds = xarray.Dataset()
    with mpas.open_mfdataset(timeSeriesFiles, variableList) as dsIn:
        ds[VAR_NAME] = rhoi * dsIn.timeMonthly_avg_iceVolumeCell
        ds["timeMonthly_avg_iceAreaCell"] = dsIn.timeMonthly_avg_iceAreaCell
        ds = mpas.add_time(ds, dsIn)
        ds.compute()

    ds = mpas.remap(ds, "mpasseaice", mappingFileName)

    util.setup_cmor(
        var_name=VAR_NAME,
        table_path=tables,
        table_name=TABLE,
        user_input_path=user_input_path,
        cmor_log_dir=cmor_log_dir,
    )

    # create axes
    axes = [
        {"table_entry": "time", "units": ds.time.units},
        {
            "table_entry": "latitude",
            "units": "degrees_north",
            "coord_vals": ds.lat.values,
            "cell_bounds": ds.lat_bnds.values,
        },
        {
            "table_entry": "longitude",
            "units": "degrees_east",
            "coord_vals": ds.lon.values,
            "cell_bounds": ds.lon_bnds.values,
        },
    ]

    try:
        mpas.write_cmor(axes, ds, VAR_NAME, VAR_UNITS)
    except Exception as err:
        util.print_message(err)
    return VAR_NAME
