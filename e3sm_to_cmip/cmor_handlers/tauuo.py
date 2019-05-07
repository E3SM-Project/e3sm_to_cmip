
"""
compute Surface Downward X Stress, tauuo
"""
from __future__ import absolute_import, division, print_function

import xarray
import logging

from e3sm_to_cmip import mpas

# 'MPAS' as a placeholder for raw variables needed
RAW_VARIABLES = ['MPASO', 'MPAS_map']

# output variable name
VAR_NAME = 'tauuo'
VAR_UNITS = 'N m-2'
TABLE = 'CMIP6_Omon.json'


def handle(infiles, tables, user_input_path, **kwargs):
    """
    Transform MPASO timeMonthly_avg_windStressZonal into CMIP.tauuo

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
    msg = 'Starting {name}'.format(name=__name__)
    logging.info(msg)

    mappingFileName = infiles['MPAS_map']
    timeSeriesFiles = infiles['MPASO']

    variableList = ['timeMonthly_avg_windStressZonal',
                    'xtime_startMonthly', 'xtime_endMonthly']

    ds = xarray.Dataset()
    with mpas.open_mfdataset(timeSeriesFiles, variableList) as dsIn:
        ds[VAR_NAME] = dsIn.timeMonthly_avg_windStressZonal

        ds = mpas.add_time(ds, dsIn)
        ds.compute()

    ds = mpas.remap(ds, mappingFileName)

    mpas.setup_cmor(VAR_NAME, tables, user_input_path, component='ocean')

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
