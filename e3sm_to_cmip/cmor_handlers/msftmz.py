
"""
compute Ocean Meridional Overturning Mass Streamfunction, msftmz
"""
from __future__ import absolute_import, division, print_function

import xarray
import logging

from e3sm_to_cmip import mpas

# 'MPAS' as a placeholder for raw variables needed
RAW_VARIABLES = ['MPASO', 'MPAS_mesh', 'MPASO_MOC_regions']

# output variable name
VAR_NAME = 'msftmz'
VAR_UNITS = 'kg s-1'


def handle(infiles, tables, user_input_path, **kwargs):
    """
    Transform MPASO timeMonthly_avg_normalVelocity,
    timeMonthly_avg_normalGMBolusVelocity, timeMonthly_avg_vertVelocityTop,
    timeMonthly_avg_vertGMBolusVelocityTop, and timeMonthly_avg_layerThickness
    into CMIP.msftmz

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

    meshFileName = infiles['MPAS_mesh']
    timeSeriesFiles = infiles['MPASO']
    regionMaskFileName = infiles['MPASO_MOC_regions']

    dsMesh = xarray.open_dataset(meshFileName, mask_and_scale=False)
    dsMesh = dsMesh.isel(Time=0)

    dsMasks = xarray.open_dataset(regionMaskFileName, mask_and_scale=False)

    variableList = ['timeMonthly_avg_normalVelocity',
                    'timeMonthly_avg_normalGMBolusVelocity',
                    'timeMonthly_avg_vertVelocityTop',
                    'timeMonthly_avg_vertGMBolusVelocityTop',
                    'timeMonthly_avg_layerThickness',
                    'xtime_startMonthly', 'xtime_endMonthly']

    with mpas.open_mfdataset(timeSeriesFiles, variableList) as dsIn:
        showProgress = 'serial' in kwargs and kwards['serial']
        ds = mpas.compute_moc_streamfunction(dsIn, dsMesh, dsMasks,
                                             showProgress=showProgress)
    ds = ds.rename({'moc': VAR_NAME})

    mpas.setup_cmor(VAR_NAME, tables, user_input_path, component='ocean')

    region = ['global_ocean',
              'atlantic_arctic_ocean']

    # create axes
    axes = [{'table_entry': 'time',
             'units': ds.time.units},
            {'table_entry': 'basin',
             'units': '',
             'coord_vals': region},
            {'table_entry': 'depth_coord',
             'units': 'm',
             'coord_vals': ds.depth.values,
             'cell_bounds': ds.depth_bnds.values},
            {'table_entry': 'latitude',
             'units': 'degrees_north',
             'coord_vals': ds.lat.values,
             'cell_bounds': ds.lat_bnds.values}]
    try:
        mpas.write_cmor(axes, ds, VAR_NAME, VAR_UNITS)
    except Exception:
        return ""
    return VAR_NAME
