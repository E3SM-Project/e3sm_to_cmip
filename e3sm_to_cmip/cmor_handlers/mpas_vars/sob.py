"""
compute Sea Water Salinity at Sea Floor, sob
"""

import xarray

from e3sm_to_cmip import mpas, util
from e3sm_to_cmip._logger import _setup_child_logger

# 'MPAS' as a placeholder for raw variables needed
RAW_VARIABLES = ["MPASO", "MPAS_mesh", "MPAS_map"]

# output variable name
VAR_NAME = "sob"
VAR_UNITS = "0.001"
TABLE = ["CMIP6_Omon.json", "MIP_OPmon.json"]


logger = _setup_child_logger(__name__)


def handle(infiles, tables, user_input_path, cmor_log_dir, mip_era, **kwargs):
    """
    Transform MPASO timeMonthly_avg_activeTracers_salinity into CMIP.sob

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
        util.print_message(msg)
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
        ds[VAR_NAME] = dsIn.timeMonthly_avg_activeTracers_salinity
        ds = mpas.get_sea_floor_values(ds, dsMesh)
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
            comment="Model prognostic salinity at bottom-most "
            "model grid cell on the Practical Salinity "
            "Scale of 1978.",
        )
    except Exception:
        return ""
    return VAR_NAME
