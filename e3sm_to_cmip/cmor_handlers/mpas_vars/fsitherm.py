"""
compute Water Flux into Sea Water due to Sea Ice Thermodynamics, fsitherm
"""

from __future__ import absolute_import, division, print_function

import xarray

from e3sm_to_cmip import mpas, util
from e3sm_to_cmip._logger import _setup_child_logger
from e3sm_to_cmip.util import print_message

# 'MPAS' as a placeholder for raw variables needed
RAW_VARIABLES = ["MPASO", "MPAS_map"]

# output variable name
VAR_NAME = "fsitherm"
VAR_UNITS = "kg m-2 s-1"
TABLE = "CMIP6_Omon.json"


logger = _setup_child_logger(__name__)


def handle(infiles, tables, user_input_path, cmor_log_dir, **kwargs):
    """
    Transform MPASO timeMonthly_avg_seaIceFreshWaterFlux into CMIP.fsitherm

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
    if kwargs.get("simple"):
        print_message(f"Simple CMOR output not supported for {VAR_NAME}", "error")
        return None

    logger.info(f"Starting {VAR_NAME}")

    mappingFileName = infiles["MPAS_map"]
    timeSeriesFiles = infiles["MPASO"]

    variableList = [
        "timeMonthly_avg_seaIceFreshWaterFlux",
        "xtime_startMonthly",
        "xtime_endMonthly",
    ]

    ds = xarray.Dataset()
    with mpas.open_mfdataset(timeSeriesFiles, variableList) as dsIn:
        ds[VAR_NAME] = dsIn.timeMonthly_avg_seaIceFreshWaterFlux

        ds = mpas.add_time(ds, dsIn)
        ds.compute()

    logger.info(f"  Completed ds.compute cycles for {VAR_NAME}")
    logger.info(f"  Calling mpas.remap for {VAR_NAME}")

    ds = mpas.remap(ds, "mpasocean", mappingFileName)
    logger.info(f"  Completed mpas.remap for {VAR_NAME}")

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
    logger.info(f"  Calling mpas.write_cmor for {VAR_NAME}")
    try:
        mpas.write_cmor(axes, ds, VAR_NAME, VAR_UNITS)
    except Exception:
        return ""
    return VAR_NAME
