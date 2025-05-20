"""
compute Sea Surface Height Above Geoid, zos
"""

from __future__ import absolute_import, division, print_function

import logging

import xarray

from e3sm_to_cmip import mpas, util
from e3sm_to_cmip.util import print_message

# 'MPAS' as a placeholder for raw variables needed
RAW_VARIABLES = ["MPASO", "MPAS_mesh", "MPAS_map"]

# output variable name
VAR_NAME = "zos"
VAR_UNITS = "m"
TABLE = "CMIP6_Omon.json"


def handle(infiles, tables, user_input_path, **kwargs):
    """
    Transform MPASO timeMonthly_avg_pressureAdjustedSSH into CMIP.zos

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
    logging.info(msg)

    meshFileName = infiles["MPAS_mesh"]
    mappingFileName = infiles["MPAS_map"]
    timeSeriesFiles = infiles["MPASO"]

    dsMesh = xarray.open_dataset(meshFileName, mask_and_scale=False)
    cellMask2D, _ = mpas.get_mpaso_cell_masks(dsMesh)
    areaCell = dsMesh.areaCell.where(cellMask2D)

    variableList = [
        "timeMonthly_avg_pressureAdjustedSSH",
        "xtime_startMonthly",
        "xtime_endMonthly",
    ]

    ds = xarray.Dataset()
    with mpas.open_mfdataset(timeSeriesFiles, variableList) as dsIn:
        ssh = dsIn.timeMonthly_avg_pressureAdjustedSSH.where(cellMask2D)
        sshAvg = (ssh * areaCell).sum(dim="nCells") / areaCell.sum(dim="nCells")
        ds[VAR_NAME] = ssh - sshAvg

        ds = mpas.add_time(ds, dsIn)
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
