# -*- coding: future_fstrings -*-
import os
import cmor
import cdms2
import logging
from lib.util import print_message

def handle(infile, tables, user_input_path):
    """
    Transform E3SM.SOILWATER_10CM into CMIP.mrsos

    float QINTR(time, lat, lon) ;
        QINTR:long_name = "interception" ;
        QINTR:units = "mm/s" ;
        QINTR:cell_methods = "time: mean" ;
        QINTR:_FillValue = 1.e+36f ;
        QINTR:missing_value = 1.e+36f ;
        QINTR:cell_measures = "area: area" ;
    """
    msg = f'Starting {__name__} with {infile}'
    logging.info(msg)
    print_message(msg, 'ok')
    # extract data from the input file
    f = cdms2.open(infile)
    qintr = f('QINTR')
    lat = qintr.getLatitude()[:]
    lon = qintr.getLongitude()[:]
    lat_bnds = f('lat_bnds')
    lon_bnds = f('lon_bnds')
    time = qintr.getTime()
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
    varid = cmor.variable('prveg', 'kg m-2 s-1', axis_ids)

    # write out the data
    try:
        for index, val in enumerate(qintr.getTime()[:]):
            data = qintr[index, :]
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
