
"""
compute Global Mean Sea Water Salinity, soga
"""

from __future__ import absolute_import, division, print_function

import xarray
from e3sm_to_cmip._logger import _logger
from e3sm_to_cmip import mpas, util
from e3sm_to_cmip.util import print_message
# 'MPAS' as a placeholder for raw variables needed
RAW_VARIABLES = ['MPASO', 'MPAS_mesh']

# output variable name
VAR_NAME = 'soga'
VAR_UNITS = '0.001'
TABLE = 'CMIP6_Omon.json'

logger = _logger(name=__name__, set_log_level="INFO", to_logfile=True, propagate=False)

def handle(infiles, tables, user_input_path, **kwargs):
    """
    Transform MPASO timeMonthly_avg_layerThickness and
    timeMonthly_avg_activeTracers_salinity into CMIP.soga

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
    if kwargs.get('simple'):
        msg = f"{VAR_NAME} is not supported for simple conversion"
        print_message(msg)
        return
        
    msg = 'Starting {name}'.format(name=__name__)
    logger.info(msg)

    meshFileName = infiles['MPAS_mesh']
    timeSeriesFiles = infiles['MPASO']

    dsMesh = xarray.open_dataset(meshFileName, mask_and_scale=False)
    _, cellMask3D = mpas.get_mpaso_cell_masks(dsMesh)

    variableList = ['timeMonthly_avg_layerThickness',
                    'timeMonthly_avg_activeTracers_salinity',
                    'xtime_startMonthly', 'xtime_endMonthly']

    ds = xarray.Dataset()
    with mpas.open_mfdataset(timeSeriesFiles, variableList) as dsIn:
        layerThickness = dsIn.timeMonthly_avg_layerThickness.where(cellMask3D)
        thetao = dsIn.timeMonthly_avg_activeTracers_salinity.where(
            cellMask3D)
        vol = layerThickness*dsMesh.areaCell
        volo = (vol).sum(dim=['nVertLevels', 'nCells'])
        ds[VAR_NAME] = (vol*thetao).sum(dim=['nVertLevels', 'nCells'])/volo

        ds = mpas.add_time(ds, dsIn)
        ds.compute()

    util.setup_cmor(var_name=VAR_NAME, table_path=tables, table_name=TABLE, user_input_path=user_input_path)

    # create axes
    axes = [{'table_entry': 'time',
             'units': ds.time.units}]
    try:
        mpas.write_cmor(axes, ds, VAR_NAME, VAR_UNITS)
    except Exception:
        return ""
    return VAR_NAME
