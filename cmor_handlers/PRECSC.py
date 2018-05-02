import os
import cmor
import cdms2


def handle(infile=None, tables_dir=None, user_input_path=None):
    """
    Transform E3SM.PRECSC + E3SM.PRECSL into CMIP.prsn



    CMIP5_Amon
        prsn
        snowfall_flux
        longitude latitude time
        atmos
        2
        PRECSC PRECSL
        PRECSC + PRECSL and unit conversion
    """
    if not infile:
        return "hello from {}".format(__name__)

    # extract data from the input file
    precc_path = infile.replace('PRECSC', 'PRECSL')
    PRECC = cdms2.open(precc_path)
    precc = PRECC('PRECSL')
    lat = precc.getLatitude()[:]
    lon = precc.getLongitude()[:]
    lat_bnds = PRECC('lat_bnds')
    lon_bnds = PRECC('lon_bnds')
    time = precc.getTime()
    time_bnds = PRECC('time_bnds')
    PRECC.close()

    PRECL = cdms2.open(infile)
    precl = PRECL('PRECSC')
    lat = precl.getLatitude()[:]
    lon = precl.getLongitude()[:]
    lat_bnds = PRECL('lat_bnds')
    lon_bnds = PRECL('lon_bnds')
    time_bnds = PRECL('time_bnds')
    PRECL.close()    

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
    varid = cmor.variable('prsn', 'kg m-2 s-1', axis_ids)

    # write out the data
    try:
        for index, val in enumerate(precc.getTime()[:]):
            data = (precc[index, :] + precl[index, :]) * 1000
            cmor.write(varid, data, time_vals=val,
                       time_bnds=[time_bnds[index, :]])
    except:
        raise
    finally:
        cmor.close(varid)
