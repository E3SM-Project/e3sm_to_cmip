"""
compute Heat Flux into Sea Water due to Frazil Ice Formation, hfsifrazil
"""

import xarray

from e3sm_to_cmip import mpas, util
from e3sm_to_cmip._logger import _setup_child_logger
from e3sm_to_cmip.util import print_message

# 'MPAS' as a placeholder for raw variables needed
RAW_VARIABLES = ["MPASO", "MPASO_namelist", "MPAS_mesh", "MPAS_map"]

# output variable name
VAR_NAME = "hfsifrazil"
VAR_UNITS = "W m-2"
TABLE = "CMIP6_Omon.json"


logger = _setup_child_logger(__name__)


def handle(infiles, tables, user_input_path, cmor_log_dir, **kwargs):
    """
    Transform MPASO timeMonthly_avg_frazilLayerThicknessTendency into
    CMIP.hfsifrazil

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

    timeSeriesFiles = infiles["MPASO"]
    mappingFileName = infiles["MPAS_map"]
    meshFileName = infiles["MPAS_mesh"]
    namelistFileName = infiles["MPASO_namelist"]

    namelist = mpas.convert_namelist_to_dict(namelistFileName)
    config_density0 = float(namelist["config_density0"])
    config_frazil_heat_of_fusion = float(namelist["config_frazil_heat_of_fusion"])

    dsMesh = xarray.open_dataset(meshFileName, mask_and_scale=False)
    _, cellMask3D = mpas.get_mpaso_cell_masks(dsMesh)

    variableList = [
        "timeMonthly_avg_frazilLayerThicknessTendency",
        "xtime_startMonthly",
        "xtime_endMonthly",
    ]

    ds = xarray.Dataset()
    with mpas.open_mfdataset(timeSeriesFiles, variableList) as dsIn:
        ds[VAR_NAME] = (
            -config_density0
            * config_frazil_heat_of_fusion
            * dsIn.timeMonthly_avg_frazilLayerThicknessTendency
        )

        ds = mpas.add_time(ds, dsIn)
        ds.compute()

    ds = mpas.add_mask(ds, cellMask3D)
    ds = mpas.add_depth(ds, dsMesh)
    ds.compute()

    ds = mpas.remap(ds, "mpasocean", mappingFileName)

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
