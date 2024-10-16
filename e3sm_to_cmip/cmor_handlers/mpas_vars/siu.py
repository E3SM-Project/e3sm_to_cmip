"""
X-component of sea ice velocity, siu
"""

from __future__ import absolute_import, division, print_function

import logging

import xarray

from e3sm_to_cmip import mpas, util
from e3sm_to_cmip.util import print_message

# 'MPAS' as a placeholder for raw variables needed
RAW_VARIABLES = ["MPASSI", "MPAS_mesh", "MPAS_map"]

# output variable name
VAR_NAME = "siu"
VAR_UNITS = "m s-1"
TABLE = "CMIP6_SImon.json"


def handle(infiles, tables, user_input_path, **kwargs):
    """
    Transform MPASSI timeMonthly_avg_uVelocityGeo into CMIP.siu

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
    timeSeriesFiles = infiles["MPASSI"]

    dsMesh = xarray.open_dataset(meshFileName, mask_and_scale=False)

    variableList = [
        "timeMonthly_avg_iceAreaCell",
        "timeMonthly_avg_uVelocityGeo",
        "xtime_startMonthly",
        "xtime_endMonthly",
    ]

    ds = xarray.Dataset()
    with mpas.open_mfdataset(timeSeriesFiles, variableList) as dsIn:
        ds["timeMonthly_avg_iceAreaCell"] = dsIn.timeMonthly_avg_iceAreaCell
        ds[VAR_NAME] = mpas.interp_vertex_to_cell(
            dsIn.timeMonthly_avg_uVelocityGeo, dsMesh
        )
        ds = mpas.add_time(ds, dsIn)
        ds = ds.chunk(chunks={"nCells": None, "time": 6})
        ds.compute()

    ds = mpas.remap(ds, "mpasseaice", mappingFileName)

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
    except Exception as err:
        util.print_message(err)
    return VAR_NAME
