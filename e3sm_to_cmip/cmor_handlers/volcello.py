"""
compute Ocean Grid-Cell Volume, volcello
"""

import xarray
import logging
import netCDF4

from e3sm_to_cmip import mpas
from e3sm_to_cmip.util import print_message, setup_cmor
# 'MPAS' as a placeholder for raw variables needed
RAW_VARIABLES = ['MPASO', 'MPAS_mesh', 'MPAS_map']

# output variable name
VAR_NAME = 'volcello'
VAR_UNITS = 'm3'
TABLE = 'CMIP6_Omon.json'


def handle(infiles, tables, user_input_path, **kwargs):
    """
    Transform MPASO timeMonthly_avg_layerThickness into CMIP.thkcello

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

    meshFileName = infiles['MPAS_mesh']
    mappingFileName = infiles['MPAS_map']
    timeSeriesFiles = infiles['MPASO']

    dsMesh = xarray.open_dataset(meshFileName, mask_and_scale=False)
    earth_radius = dsMesh.attrs['sphere_radius']
    _, cellMask3D = mpas.get_mpaso_cell_masks(dsMesh)

    variableList = ['timeMonthly_avg_layerThickness', 'xtime_startMonthly',
                    'xtime_endMonthly']

    ds = xarray.Dataset()
    with mpas.open_mfdataset(timeSeriesFiles, variableList) as dsIn:
        ds[VAR_NAME] = \
            dsIn.timeMonthly_avg_layerThickness.where(cellMask3D, 0.)
        ds = mpas.add_time(ds, dsIn)
        ds.compute()

    ds = mpas.add_depth(ds, dsMesh)
    ds.compute()

    ds = mpas.remap(ds, 'mpasocean', mappingFileName)

    # set masked values (where there are no MPAS grid cells) to zero
    ds[VAR_NAME] = ds[VAR_NAME].where(
        ds[VAR_NAME] != netCDF4.default_fillvals['f4'], 0.)

    # the result above is just a mask of area fraction.  We need to multiply
    # by the area on the output grid
    dsMap = xarray.open_dataset(mappingFileName)
    area_b = dsMap.area_b.values
    dst_grid_dims = dsMap.dst_grid_dims.values
    area_b = area_b.reshape((dst_grid_dims[1], dst_grid_dims[0]))
    area_b = xarray.DataArray(data=area_b, dims=('lat', 'lon'),
                              coords=(ds.coords['lat'], ds.coords['lon']))

    # area_b is in square radians, so need to multiply by the earth_radius**2
    # multiply variables in this order so they don't get transposed
    ds[VAR_NAME] = ds[VAR_NAME]*earth_radius**2*area_b

    setup_cmor(var_name=VAR_NAME, table_path=tables, table_name=TABLE,
               user_input_path=user_input_path)

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
