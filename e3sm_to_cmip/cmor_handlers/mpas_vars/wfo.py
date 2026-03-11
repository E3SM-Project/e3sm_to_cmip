"""
compute Water Flux into Sea Water, wfo
"""

import xarray

from e3sm_to_cmip import mpas, util
from e3sm_to_cmip._logger import _setup_child_logger
from e3sm_to_cmip.util import print_message

# 'MPAS' as a placeholder for raw variables needed
RAW_VARIABLES = ["MPASO", "MPAS_map"]

# output variable name
VAR_NAME = "wfo"
VAR_UNITS = "kg m-2 s-1"
TABLE = ["CMIP6_Omon.json", "MIP_OPmon.json"]


logger = _setup_child_logger(__name__)


def handle(infiles, tables, user_input_path, cmor_log_dir, mip_era, **kwargs):
    """
    Transform MPASO timeMonthly_avg_seaIceFreshWaterFlux,
    timeMonthly_avg_riverRunoffFlux, timeMonthly_avg_iceRunoffFlux,
    timeMonthly_avg_rainFlux, and timeMonthly_avg_snowFlux into CMIP.wfo

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
        msg = f"{VAR_NAME} is not supported for simple conversion"
        print_message(msg)
        return

    msg = "Starting {name}".format(name=__name__)
    logger.info(msg)

    mappingFileName = infiles["MPAS_map"]
    timeSeriesFiles = infiles["MPASO"]

    variableList = [
        "timeMonthly_avg_seaIceFreshWaterFlux",
        "timeMonthly_avg_riverRunoffFlux",
        "timeMonthly_avg_iceRunoffFlux",
        "timeMonthly_avg_rainFlux",
        "timeMonthly_avg_snowFlux",
        "xtime_startMonthly",
        "xtime_endMonthly",
    ]

    ds = xarray.Dataset()
    with mpas.open_mfdataset(timeSeriesFiles, variableList) as dsIn:
        ds[VAR_NAME] = (
            dsIn.timeMonthly_avg_seaIceFreshWaterFlux
            + dsIn.timeMonthly_avg_riverRunoffFlux
            + dsIn.timeMonthly_avg_iceRunoffFlux
            + dsIn.timeMonthly_avg_rainFlux
            + dsIn.timeMonthly_avg_snowFlux
        )

        ds = mpas.add_time(ds, dsIn)
        ds.compute()

    ds = mpas.remap(ds, "mpasocean", mappingFileName)

    util.setup_cmor(
        var_name=VAR_NAME,
        table_path=tables,
        table_name=TABLE,
        user_input_path=user_input_path,
        cmor_log_dir=cmor_log_dir,
        mip_era=mip_era,
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
        mpas.write_cmor(
            axes,
            ds,
            VAR_NAME,
            VAR_UNITS,
            comment="Computed as the water flux into the ocean "
            "divided by the area of the ocean portion of "
            "the grid cell. This is the sum of sea-ice"
            "freshwater, river runoff, ice runoff, rain,"
            "and snow fluxes.",
        )
    except Exception:
        return ""
    return VAR_NAME
