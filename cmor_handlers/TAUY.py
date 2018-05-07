import os
import cmor
import cdms2


def handle(infile, tables, user_input_path):
    """
    Transform E3SM.TAUX into CMIP.tauv

    """
    # extract data from the input file
    f = cdms2.open(infile)
    tauy = f('TAUY')
    lat = tauy.getLatitude()[:]
    lon = tauy.getLongitude()[:]
    lat_bnds = f('lat_bnds')
    lon_bnds = f('lon_bnds')
    time = tauy.getTime()
    time_bnds = f('time_bnds')
    f.close()

    # setup cmor
    tables_path = os.path.join(tables, 'Tables')
    cmor.setup(inpath=tables_path, netcdf_file_action=cmor.CMOR_REPLACE)
    logfile = os.path.join(os.getcwd(), 'logs')
    if not os.path.exists(logfile):
        os.makedirs(logfile)
    _, tail = os.path.split(infile)
    logfile = os.path.join(logfile, tail.replace('.nc', '.log'))
    cmor.setup(
        inpath=tables_path,
        netcdf_file_action=cmor.CMOR_REPLACE, 
        logfile=logfile)
    cmor.dataset_json(user_input_path)
    table = 'CMIP6_Amon.json'
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
    }]
    axis_ids = list()
    for axis in axes:
        axis_id = cmor.axis(**axis)
        axis_ids.append(axis_id)

    # create the cmor variable
    varid = cmor.variable('tauy', 'Pa', axis_ids)

    # write out the data
    try:
        for index, val in enumerate(data.getTime()[:]):
            data = tauy[index, :]
            cmor.write(
                varid,
                data,
                time_vals=val,
                time_bnds=[time_bnds[index, :]])
    except:
        raise
    finally:
        cmor.close(varid)
    return 'TAUY'
