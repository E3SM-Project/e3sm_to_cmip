"""
compute  Grid-Cell Area for Ocean Variables areacello
"""
import xarray

from e3sm_to_cmip import mpas, util
from e3sm_to_cmip.util import print_message
from e3sm_to_cmip._logger import e2c_logger
logger = e2c_logger(name=__name__, set_log_level="INFO", to_logfile=True, propagate=False)

# 'MPAS' as a placeholder for raw variables needed
RAW_VARIABLES = ['MPAS_mesh', 'MPAS_map']

# output variable name
VAR_NAME = 'areacello'
VAR_UNITS = 'm2'
TABLE = 'CMIP6_Ofx.json'


def handle(infiles, tables, user_input_path, **kwargs):
    """
    Transform MPASO cellArea into CMIP.areacello
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
    mappingFileName = infiles['MPAS_map']

    dsMesh = xarray.open_dataset(meshFileName, mask_and_scale=False)
    earth_radius = dsMesh.attrs['sphere_radius']
    cellMask2D, _ = mpas.get_mpaso_cell_masks(dsMesh)

    ds = xarray.Dataset()
    ds[VAR_NAME] = ('nCells', cellMask2D.astype(float).data)
    ds = mpas.remap(ds, 'mpasocean', mappingFileName)

    # the result above is just a mask of area fraction.  We need to multiply
    # by the area on the output grid
    dsMap = xarray.open_dataset(mappingFileName)
    area_b = dsMap.area_b.values
    dst_grid_dims = dsMap.dst_grid_dims.values
    area_b = area_b.reshape((dst_grid_dims[1], dst_grid_dims[0]))
    area_b = xarray.DataArray(data=area_b, dims=ds[VAR_NAME].dims)

    # area_b is in square radians, so need to multiply by the earth_radius**2
    ds[VAR_NAME] = earth_radius**2*area_b*ds[VAR_NAME]

    util.setup_cmor(VAR_NAME, tables, TABLE, user_input_path)

    # create axes
    axes = [{'table_entry': 'latitude',
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
