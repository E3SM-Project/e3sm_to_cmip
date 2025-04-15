"""
compute 	Depth Below Geoid of Interfaces Between Ocean Layers, zhalfo
"""

from __future__ import absolute_import, division, print_function

import numpy
import xarray

from e3sm_to_cmip import _logger, mpas, util
from e3sm_to_cmip.util import print_message

# 'MPAS' as a placeholder for raw variables needed
RAW_VARIABLES = ["MPASO", "MPAS_mesh", "MPAS_map"]

# output variable name
VAR_NAME = "zhalfo"
VAR_UNITS = "m"
TABLE = "CMIP6_Omon.json"

logger = _logger._logger(name=__name__, to_logfile=True, propagate=False)


def handle(infiles, tables, user_input_path, **kwargs):
    """
    Transform MPASO timeMonthly_avg_layerThickness into CMIP.zhalfo

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
    _, cellMask3D = mpas.get_mpaso_cell_masks(dsMesh)

    variableList = [
        "timeMonthly_avg_layerThickness",
        "xtime_startMonthly",
        "xtime_endMonthly",
    ]

    nVertLevels = dsMesh.sizes["nVertLevels"]

    ds = xarray.Dataset()
    with mpas.open_mfdataset(timeSeriesFiles, variableList) as dsIn:
        layerThickness = dsIn.timeMonthly_avg_layerThickness
        layerThickness = layerThickness.where(cellMask3D)
        thicknessSum = layerThickness.sum(dim="nVertLevels")
        mask = cellMask3D.isel(nVertLevels=0)
        zSurface = (-dsMesh.bottomDepth + thicknessSum).where(mask)
        zSurface.compute()
        # print('done zSurface')
        slices = [zSurface]
        maskSlices = [mask]
        zLayerBot = zSurface
        for zIndex in range(nVertLevels):
            mask = cellMask3D.isel(nVertLevels=zIndex)
            zLayerBot = (zLayerBot - layerThickness.isel(nVertLevels=zIndex)).where(
                mask
            )
            zLayerBot.compute()
            # print('done zLayerBot {}/{}'.format(zIndex+1, nVertLevels))
            slices.append(zLayerBot)
            maskSlices.append(mask)
        ds[VAR_NAME] = xarray.concat(slices, dim="olevhalf")
        mask = xarray.concat(maskSlices, dim="olevhalf")
        ds = mpas.add_mask(ds, mask)
        ds = ds.transpose("Time", "olevhalf", "nCells")
        ds = mpas.add_time(ds, dsIn)
        ds.compute()

    ds = mpas.remap(ds, "mpasocean", mappingFileName)
    depth_coord_half = numpy.zeros(nVertLevels + 1)
    depth_coord_half[1:] = dsMesh.refBottomDepth.values

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
            "table_entry": "depth_coord_half",
            "units": "m",
            "coord_vals": depth_coord_half,
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
