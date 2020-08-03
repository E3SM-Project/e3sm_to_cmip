
"""
compute Ocean Grid-Cell Mass per area, masscello
"""
from __future__ import absolute_import, division, print_function

import xarray
import logging
import netCDF4

from e3sm_to_cmip import mpas

# 'MPAS' as a placeholder for raw variables needed
RAW_VARIABLES = ['MPASO', 'MPASO_namelist', 'MPAS_mesh', 'MPAS_map']

# output variable name
VAR_NAME = 'masscello'
VAR_UNITS = 'kg m-2'
TABLE = 'CMIP6_Omon.json'


def handle(infiles, tables, user_input_path, **kwargs):
    """
    Transform MPASO timeMonthly_avg_layerThickness into CMIP.masscello

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
    logging.info(msg)

    namelistFileName = infiles['MPASO_namelist']
    meshFileName = infiles['MPAS_mesh']
    mappingFileName = infiles['MPAS_map']
    timeSeriesFiles = infiles['MPASO']

    namelist = mpas.convert_namelist_to_dict(namelistFileName)
    config_density0 = float(namelist['config_density0'])

    dsMesh = xarray.open_dataset(meshFileName, mask_and_scale=False)
    _, cellMask3D = mpas.get_cell_masks(dsMesh)

    variableList = ['timeMonthly_avg_layerThickness', 'xtime_startMonthly',
                    'xtime_endMonthly']

    ds = xarray.Dataset()
    with mpas.open_mfdataset(timeSeriesFiles, variableList) as dsIn:
        ds[VAR_NAME] = config_density0 * \
            dsIn.timeMonthly_avg_layerThickness.where(cellMask3D, 0.)
        ds = mpas.add_time(ds, dsIn)
        ds.compute()

    ds = mpas.add_depth(ds, dsMesh)
    ds.compute()

    ds = mpas.remap(ds, mappingFileName)

    # set masked values (where there are no MPAS grid cells) to zero
    ds[VAR_NAME] = ds[VAR_NAME].where(
        ds[VAR_NAME] != netCDF4.default_fillvals['f4'], 0.)

    mpas.setup_cmor(VAR_NAME, tables, user_input_path, component='ocean')

    # create axes
    axes = [{'table_entry': 'time',
             'units': ds.time.units},
            {'table_entry': 'depth_coord',
             'units': 'm',
             'coord_vals': ds.depth.values,
             'cell_bounds': ds.depth_bnds.values},
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
