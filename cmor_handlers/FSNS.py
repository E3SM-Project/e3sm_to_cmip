import os
import cmor
import cdms2


def handle(infile, tables, user_input_path):
    """
    Transform E3SM.FSNS into CMIP.rsns and rsus

    """
    # extract data from the input file
    f = cdms2.open(infile)
    fsns = f('FSNS')
    lat = fsns.getLatitude()[:]
    lon = fsns.getLongitude()[:]
    lat_bnds = f('lat_bnds')
    lon_bnds = f('lon_bnds')
    time = fsns.getTime()
    time_bnds = f('time_bnds')
    f.close()

    f = cdms2.open(infile.replace('FSNS', 'FSDS'))
    fsds = f('FSDS')
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

    # write out derived varible
    varid = cmor.variable('rsus', 'W m-2', axis_ids, positive='up')
    try:
        for index, val in enumerate(fsns.getTime()[:]):
            data = fsds[index, :] - fsns[index, :]
            cmor.write(
                varid,
                data,
                time_vals=val,
                time_bnds=[time_bnds[index, :]])
    except:
        raise
    finally:
        cmor.close(varid)

    return 'FSNS'
