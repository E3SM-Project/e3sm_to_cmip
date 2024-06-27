
"""
compute Downward Heat Flux at Sea Water Surface, hfds
"""
from __future__ import absolute_import, division, print_function

import xarray
from e3sm_to_cmip._logger import e2c_logger
logger = e2c_logger(name=__name__, set_log_level="INFO", to_logfile=True, propagate=False)

from e3sm_to_cmip import mpas, util
from e3sm_to_cmip.util import print_message

# 'MPAS' as a placeholder for raw variables needed
RAW_VARIABLES = ['MPASO', 'MPAS_map']

# output variable name
VAR_NAME = 'hfds'
VAR_UNITS = 'W m-2'
TABLE = 'CMIP6_Omon.json'


def handle(infiles, tables, user_input_path, **kwargs):
    """
    Transform MPASO timeMonthly_avg_seaIceHeatFlux,
    timeMonthly_avg_latentHeatFlux ,timeMonthly_avg_sensibleHeatFlux,
    timeMonthly_avg_shortWaveHeatFlux, timeMonthly_avg_longWaveHeatFluxUp, and
    timeMonthly_avg_longWaveHeatFluxDown into CMIP.hfds

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
        print_message(f'Simple CMOR output not supported for {VAR_NAME}', 'error')
        return None

    logger.info(f'Starting {VAR_NAME}')

    mappingFileName = infiles['MPAS_map']
    timeSeriesFiles = infiles['MPASO']

    variableList = ['timeMonthly_avg_seaIceHeatFlux',
                    'timeMonthly_avg_latentHeatFlux',
                    'timeMonthly_avg_sensibleHeatFlux',
                    'timeMonthly_avg_shortWaveHeatFlux',
                    'timeMonthly_avg_longWaveHeatFluxUp',
                    'timeMonthly_avg_longWaveHeatFluxDown',
                    'xtime_startMonthly', 'xtime_endMonthly']

    ds = xarray.Dataset()
    with mpas.open_mfdataset(timeSeriesFiles, variableList) as dsIn:
        ds[VAR_NAME] = (dsIn.timeMonthly_avg_seaIceHeatFlux +
                        dsIn.timeMonthly_avg_latentHeatFlux +
                        dsIn.timeMonthly_avg_sensibleHeatFlux +
                        dsIn.timeMonthly_avg_shortWaveHeatFlux +
                        dsIn.timeMonthly_avg_longWaveHeatFluxUp +
                        dsIn.timeMonthly_avg_longWaveHeatFluxDown)

        ds = mpas.add_time(ds, dsIn)
        ds.compute()

    ds = mpas.remap(ds, 'mpasocean', mappingFileName)

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
        mpas.write_cmor(axes, ds, VAR_NAME, VAR_UNITS, positive='down')
    except Exception:
        return ""
    return VAR_NAME
