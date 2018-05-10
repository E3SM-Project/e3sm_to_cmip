# -*- coding: future_fstrings -*-
import os
import cmor
import cdms2
import logging
from lib.util import print_message


def handle(infile, tables, user_input_path):
    """
    Transform E3SM.FLDS and E3SM.FLNS into rlus and rlns
    """
    msg = f'Starting {__name__} with {infile}'
    logging.info(msg) 
    print_message(msg, 'ok')
    # extract data from the input file
    f = cdms2.open(infile)
    flds = f('FLDS')
    lat = flds.getLatitude()[:]
    lon = flds.getLongitude()[:]
    lat_bnds = f('lat_bnds')
    lon_bnds = f('lon_bnds')
    time = flds.getTime()
    time_bnds = f('time_bnds')
    f.close()

    f = cdms2.open(infile.replace('FLDS', 'FLNS'))
    flns = f('FLNS')
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

    # create the cmor variable
    varid = cmor.variable('rlds', 'W m-2', axis_ids, positive='down')
    varid2 = cmor.variable('rlus', 'W m-2', axis_ids, positive='up')

    # write out the data
    try:
        for index, val in enumerate(flds.getTime()[:]):
            data = flds[index, :]
            cmor.write(
                varid,
                data,
                time_vals=val,
                time_bnds=[time_bnds[index, :]])
            
            data = flds[index, :] + flns[index, :]
            cmor.write(
                varid2,
                data,
                time_vals=val,
                time_bnds=[time_bnds[index, :]])
    except:
        raise
    finally:
        cmor.close(varid)
        cmor.close(varid2)
    return 'FLDS'
