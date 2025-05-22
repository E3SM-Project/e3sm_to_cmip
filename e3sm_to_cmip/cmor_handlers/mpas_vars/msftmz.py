"""
compute Ocean Meridional Overturning Mass Streamfunction, msftmz
"""

from __future__ import absolute_import, division, print_function

import xarray

from e3sm_to_cmip import mpas, util
from e3sm_to_cmip._logger import _setup_child_logger
from e3sm_to_cmip.util import print_message

# 'MPAS' as a placeholder for raw variables needed
RAW_VARIABLES = ["MPASO", "MPAS_mesh", "MPASO_MOC_regions", "MPASO_namelist"]

# output variable name
VAR_NAME = "msftmz"
VAR_UNITS = "kg s-1"
TABLE = "CMIP6_Omon.json"


logger = _setup_child_logger(__name__)


def handle(infiles, tables, user_input_path, cmor_log_dir, **kwargs):
    """
    Transform MPASO timeMonthly_avg_normalVelocity,
    timeMonthly_avg_normalGMBolusVelocity, timeMonthly_avg_vertVelocityTop,
    timeMonthly_avg_vertGMBolusVelocityTop, and timeMonthly_avg_layerThickness
    into CMIP.msftmz

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
    timeSeriesFiles = infiles["MPASO"]
    regionMaskFileName = infiles["MPASO_MOC_regions"]
    namelistFileName = infiles["MPASO_namelist"]

    namelist = mpas.convert_namelist_to_dict(namelistFileName)
    config_density0 = float(namelist["config_density0"])

    dsMesh = xarray.open_dataset(meshFileName, mask_and_scale=False)
    dsMesh = dsMesh.isel(Time=0)

    dsMasks = xarray.open_dataset(regionMaskFileName, mask_and_scale=False)

    variableList = [
        "timeMonthly_avg_normalVelocity",
        "timeMonthly_avg_normalGMBolusVelocity",
        "timeMonthly_avg_vertVelocityTop",
        "timeMonthly_avg_vertGMBolusVelocityTop",
        "timeMonthly_avg_layerThickness",
        "xtime_startMonthly",
        "xtime_endMonthly",
    ]

    with mpas.open_mfdataset(timeSeriesFiles, variableList) as dsIn:
        showProgress = "serial" in kwargs and kwargs["serial"]
        ds = config_density0 * mpas.compute_moc_streamfunction(
            dsIn, dsMesh, dsMasks, showProgress=showProgress
        )

    ds = ds.rename({"moc": VAR_NAME})

    util.setup_cmor(
        var_name=VAR_NAME,
        table_path=tables,
        table_name=TABLE,
        user_input_path=user_input_path,
        cmor_log_dir=cmor_log_dir,
    )

    region = ["global_ocean", "atlantic_arctic_ocean"]

    # create axes
    axes = [
        {"table_entry": "time", "units": ds.time.units},
        {"table_entry": "basin", "units": "", "coord_vals": region},
        {
            "table_entry": "depth_coord",
            "units": "m",
            "coord_vals": ds.depth.values,
            "cell_bounds": ds.depth_bnds.values,
        },
        {
            "table_entry": "latitude",
            "units": "degrees_north",
            "coord_vals": ds.lat.values,
            "cell_bounds": ds.lat_bnds.values,
        },
    ]
    try:
        mpas.write_cmor(axes, ds, VAR_NAME, VAR_UNITS)
    except Exception:
        return ""
    return VAR_NAME
