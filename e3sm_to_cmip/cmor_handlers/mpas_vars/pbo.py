
"""
compute Sea Water Pressure at Sea floor, pbo
"""
from __future__ import absolute_import, division, print_function

import xarray
from e3sm_to_cmip._logger import e2c_logger
logger = e2c_logger(name=__name__, set_log_level="INFO", to_logfile=True, propagate=False)

from e3sm_to_cmip import mpas, util
from e3sm_to_cmip.util import print_message
# 'MPAS' as a placeholder for raw variables needed
RAW_VARIABLES = ['MPASO', 'MPASO_namelist', 'MPAS_mesh', 'MPAS_map', 'PSL']

# output variable name
VAR_NAME = 'pbo'
VAR_UNITS = 'Pa'
TABLE = 'CMIP6_Omon.json'


def handle(infiles, tables, user_input_path, **kwargs):
    """
    Transform MPASO timeMonthly_avg_pressureAdjustedSSH, timeMonthly_avg_ssh,
    timeMonthly_avg_density, timeMonthly_avg_layerThickness, and EAM PSL into
    CMIP.pbo

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

    namelistFileName = infiles['MPASO_namelist']
    meshFileName = infiles['MPAS_mesh']
    mappingFileName = infiles['MPAS_map']
    timeSeriesFiles = infiles['MPASO']
    pslFileNames = infiles['PSL']

    namelist = mpas.convert_namelist_to_dict(namelistFileName)
    config_density0 = float(namelist['config_density0'])
    gravity = 9.80616

    dsMesh = xarray.open_dataset(meshFileName, mask_and_scale=False)
    cellMask2D, cellMask3D = mpas.get_mpaso_cell_masks(dsMesh)

    variableList = ['timeMonthly_avg_pressureAdjustedSSH',
                    'timeMonthly_avg_ssh', 'timeMonthly_avg_layerThickness',
                    'timeMonthly_avg_density', 'xtime_startMonthly',
                    'xtime_endMonthly']

    ds = xarray.Dataset()
    with mpas.open_mfdataset(timeSeriesFiles, variableList) as dsIn:
        seaIcePressure = config_density0 * gravity * \
            (dsIn.timeMonthly_avg_pressureAdjustedSSH -
             dsIn.timeMonthly_avg_ssh)
        ds[VAR_NAME] = seaIcePressure.where(cellMask2D) + gravity * \
            (dsIn.timeMonthly_avg_density *
             dsIn.timeMonthly_avg_layerThickness).where(cellMask3D).sum(
                dim='nVertLevels')

        ds = mpas.add_time(ds, dsIn)
        ds.compute()

    ds = mpas.remap(ds, 'mpasocean', mappingFileName)

    with xarray.open_mfdataset(pslFileNames, concat_dim='time') as dsIn:
        ds[VAR_NAME] = ds[VAR_NAME] + dsIn.PSL.values

    util.setup_cmor(VAR_NAME, tables, TABLE, user_input_path)

    # create axes
    axes = [{'table_entry': 'time',
             'units': ds.time.units},
            {'table_entry': 'latitude',
             'units': 'degrees_north',
             'coord_vals': ds.lat.values,
             'cell_bounds': ds.lat_bnds.values},
            {'table_entry': 'longitude',
             'units': 'degrees_east',
             'coord_vals': ds.lon.values,
             'cell_bounds': ds.lon_bnds.values}]
    try:
        mpas.write_cmor(axes, ds, VAR_NAME, VAR_UNITS)
    except Exception:
        return ""
    return VAR_NAME
