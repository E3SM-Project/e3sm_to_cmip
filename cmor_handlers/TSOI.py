import os
import cmor
import cdms2


def handle(infile, tables, user_input_path):
    """
    Transform E3SM.TSOI into CMIP.tsl
    """
    # extract data from the input file
    f = cdms2.open(infile)
    tsoi = f('TSOI')
    lat = tsoi.getLatitude()[:]
    lon = tsoi.getLongitude()[:]
    lat_bnds = f('lat_bnds')
    lon_bnds = f('lon_bnds')
    time = tsoi.getTime()
    time_bnds = f('time_bounds')
    levgrnd = tsoi.getAxis(1)[:]
    f.close()

    # setup cmor
    logfile = os.path.join(os.getcwd(), 'logs')
    if not os.path.exists(logfile):
        os.makedirs(logfile)
    _, tail = os.path.split(infile)
    logfile = os.path.join(logfile, tail.replace('.nc', '.log'))
    cmor.setup(
        inpath=tables,
        netcdf_file_action=cmor.CMOR_REPLACE, 
        logfile=logfile)
    cmor.dataset_json(user_input_path)
    table = 'CMIP6_Lmon.json'
    try:
        cmor.load_table(table)
    except:
        raise Exception('Unable to load table from {}'.format(__name__))

    # create axes
    axes = [{
        'table_entry': 'time',
        'units': time.units
    }, {
        'table_entry': 'latitude',
        'units': 'degrees_north',
        'coord_vals': lat[:],
        'cell_bounds': lat_bnds[:]
    }, {
        'table_entry': 'longitude',
        'units': 'degrees_east',
        'coord_vals': lon[:],
        'cell_bounds': lon_bnds[:]
    }, {
        'table_entry': 'sdepth',
        'units': 'm',
        'coord_vals': levgrnd
    }]
    axis_ids = list()
    for axis in axes:
        axis_id = cmor.axis(**axis)
        axis_ids.append(axis_id)

    # create the cmor variable
    varid = cmor.variable('tsl', 'K', axis_ids)

    # write out the data
    try:
        for index, val in enumerate(tsoi.getTime()[:]):
            data = tsoi[index, :]
            cmor.write(
                varid,
                data,
                time_vals=val,
                time_bnds=[time_bnds[index, :]])
    except:
        raise
    finally:
        cmor.close(varid)
    return 'TSOI'
