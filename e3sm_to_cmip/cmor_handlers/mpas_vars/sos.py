"""
compute Sea Surface Salinity, sos
"""

from __future__ import absolute_import, division, print_function

import xarray
from e3sm_to_cmip import _logger

import xarray

from e3sm_to_cmip import mpas, util
from e3sm_to_cmip.util import print_message

# 'MPAS' as a placeholder for raw variables needed
RAW_VARIABLES = ["MPASO", "MPAS_mesh", "MPAS_map"]

# output variable name
VAR_NAME = "sos"
VAR_UNITS = "0.001"
TABLE = "CMIP6_Omon.json"

logger = _logger.e2c_logger(
    name=__name__, log_level=_logger.INFO, to_logfile=True, propagate=False
)


def handle(infiles, tables, user_input_path, **kwargs):
    """
    Transform MPASO timeMonthly_avg_activeTracers_salinity into CMIP.sos

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

    meshFileName = infiles["MPAS_mesh"]
    mappingFileName = infiles["MPAS_map"]
    timeSeriesFiles = infiles["MPASO"]

    dsMesh = xarray.open_dataset(meshFileName, mask_and_scale=False)
    cellMask2D, _ = mpas.get_mpaso_cell_masks(dsMesh)

    variableList = [
        "timeMonthly_avg_activeTracers_salinity",
        "xtime_startMonthly",
        "xtime_endMonthly",
    ]

    ds = xarray.Dataset()
    with mpas.open_mfdataset(timeSeriesFiles, variableList) as dsIn:
        thetao = dsIn.timeMonthly_avg_activeTracers_salinity
        ds[VAR_NAME] = thetao.isel(nVertLevels=0).squeeze(drop=True)
        ds = mpas.add_time(ds, dsIn)
        ds.compute()
    ds = mpas.add_mask(ds, cellMask2D)
    ds.compute()

    ds = mpas.remap(ds, "mpasocean", mappingFileName)

    util.setup_cmor(
        var_name=VAR_NAME,
        table_path=tables,
        table_name=TABLE,
        user_input_path=user_input_path,
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
    except Exception:
        return ""
    return VAR_NAME
