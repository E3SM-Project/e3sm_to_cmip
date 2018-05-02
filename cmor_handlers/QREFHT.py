import os
import cmor
import cdms2


def handle(infile=None, tables_dir=None, user_input_path=None):
    """
    Transform E3SM.QREFHT into CMIP.huss



    CMIP5_Amon
        hurs
        relative_humidity
        longitude latitude time height2m
        atmos
        1
        RHREFHT
        RHREFHT no change
    """
    if not infile:
        return "hello from {}".format(__name__)

    # extract data from the input file
    f = cdms2.open(infile)
    data = f('QREFHT')
    lat = data.getLatitude()[:]
    lon = data.getLongitude()[:]
    lat_bnds = f('lat_bnds')
    lon_bnds = f('lon_bnds')
    time = data.getTime()
    time_bnds = f('time_bnds')
    f.close()

    # setup cmor
    tables_path = os.path.join(tables_dir, 'Tables')
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
    except Exception as e:
        print 'Unable to load table from {}'.format(__name__)

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
    varid = cmor.variable('huss', '1.0', axis_ids)

    # write out the data
    try:
        for index, val in enumerate(data.getTime()[:]):
            cmor.write(varid, data[index, :], time_vals=val,
                       time_bnds=[time_bnds[index, :]])
    except:
        raise
    finally:
        cmor.close(varid)
