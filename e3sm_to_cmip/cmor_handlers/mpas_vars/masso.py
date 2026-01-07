"""
compute Sea Water Mass, masso
"""

import xarray

from e3sm_to_cmip import mpas, util
from e3sm_to_cmip._logger import _setup_child_logger
from e3sm_to_cmip.util import print_message

# 'MPAS' as a placeholder for raw variables needed
RAW_VARIABLES = ["MPASO", "MPASO_namelist", "MPAS_mesh"]

# output variable name
VAR_NAME = "masso"
VAR_UNITS = "kg"
TABLE = ["CMIP6_Omon.json", "MIP_OPmon.json"]


logger = _setup_child_logger(__name__)


def handle(infiles, tables, user_input_path, cmor_log_dir, mip_era, **kwargs):
    """
    Transform MPASO timeMonthly_avg_layerThickness into CMIP.masso

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

    namelistFileName = infiles["MPASO_namelist"]
    meshFileName = infiles["MPAS_mesh"]
    timeSeriesFiles = infiles["MPASO"]

    namelist = mpas.convert_namelist_to_dict(namelistFileName)
    config_density0 = float(namelist["config_density0"])

    dsMesh = xarray.open_dataset(meshFileName, mask_and_scale=False)
    _, cellMask3D = mpas.get_mpaso_cell_masks(dsMesh)

    variableList = [
        "timeMonthly_avg_layerThickness",
        "xtime_startMonthly",
        "xtime_endMonthly",
    ]

    ds = xarray.Dataset()
    with mpas.open_mfdataset(timeSeriesFiles, variableList) as dsIn:
        ds[VAR_NAME] = (
            config_density0
            * dsIn.timeMonthly_avg_layerThickness.where(cellMask3D, 0.0)
            * dsMesh.areaCell
        ).sum(dim=["nVertLevels", "nCells"])
        ds = mpas.add_time(ds, dsIn)
        ds.compute()

    util.setup_cmor(
        var_name=VAR_NAME,
        table_path=tables,
        table_name=TABLE,
        user_input_path=user_input_path,
        cmor_log_dir=cmor_log_dir,
        mip_era=mip_era,
    )

    # create axes
    axes = [{"table_entry": "time", "units": ds.time.units}]
    try:
        mpas.write_cmor(axes, ds, VAR_NAME, VAR_UNITS)
    except Exception:
        return ""
    return VAR_NAME
