import os
import cmor
import cdms2


def handle(infile, tables, user_input_path):
    """
    Transform E3SM.SOILWATER_10CM into CMIP.mrsos

    float QOVER(time, lat, lon) ;
        QOVER:long_name = "surface runoff" ;
        QOVER:units = "mm/s" ;
        QOVER:cell_methods = "time: mean" ;
        QOVER:_FillValue = 1.e+36f ;
        QOVER:missing_value = 1.e+36f ;
        QOVER:cell_measures = "area: area" ;
    """
    # extract data from the input file
    f = cdms2.open(infile)
    qover = f('QOVER')
    lat = qover.getLatitude()[:]
    lon = qover.getLongitude()[:]
    lat_bnds = f('lat_bnds')
    lon_bnds = f('lon_bnds')
    time = qover.getTime()
    time_bnds = f('time_bounds')
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
    }]
    axis_ids = list()
    for axis in axes:
        axis_id = cmor.axis(**axis)
        axis_ids.append(axis_id)

    # create the cmor variable
    varid = cmor.variable('mrros', 'kg m-2 s-1', axis_ids)

    # write out the data
    try:
        for index, val in enumerate(qover.getTime()[:]):
            data = qover[index, :]
            cmor.write(
                varid,
                data,
                time_vals=val,
                time_bnds=[time_bnds[index, :]])
    except:
        raise
    finally:
        cmor.close(varid)
    return 'QINTR'
