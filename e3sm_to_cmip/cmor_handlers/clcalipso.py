"""
CLD_CAL to clcalipso converter
"""
from __future__ import absolute_import, division, print_function, unicode_literals

import os
import cmor
import cdms2
import logging

from progressbar import ProgressBar

from e3sm_to_cmip.util import print_message
from e3sm_to_cmip.util import setup_cmor
from e3sm_to_cmip.lib import handle_variables

# list of raw variable names needed
RAW_VARIABLES = [str('CLD_CAL')]
VAR_NAME = str('clcalipso')
VAR_UNITS = str('%')
TABLE = str('CMIP6_CFmon.json')


def write_data(varid, data, timeval, timebnds, index):
    """
    clcalipso = CLD_CAL with alt40 levels
    """
    cmor.write(
        varid,
        data['CLD_CAL'][index, :],
        time_vals=timeval,
        time_bnds=timebnds)
# ------------------------------------------------------------------


def handle(infiles, tables, user_input_path, **kwargs):
    """
    Transform E3SM.TS into CMIP.ts

    Parameters
    ----------
        infiles (List): a list of strings of file names for the raw input data
        tables (str): path to CMOR tables
        user_input_path (str): path to user input json file
    Returns
    -------
        var name (str): the name of the processed variable after processing is complete
    """
    serial = kwargs.get('serial')

    msg = '{}: Starting'.format(VAR_NAME)

    if serial:
        print(msg)
    nonzero = False
    for variable in RAW_VARIABLES:
        if len(infiles[variable]) == 0:
            msg = '{}: Unable to find input files for {}'.format(VAR_NAME, variable)
            print_message(msg)
            nonzero = True
    if nonzero:
        return

    msg = '{}: running with input files: {}'.format(
        VAR_NAME,
        infiles)
    logging.info(msg)

    # setup cmor
    setup_cmor(
        VAR_NAME,
        tables,
        TABLE,
        user_input_path)

    msg = '{}: CMOR setup complete'.format(VAR_NAME)

    if serial:
        print(msg)

    data = {}

    # assuming all year ranges are the same for every variable
    num_files_per_variable = len(infiles['CLD_CAL'])

    # sort the input files for each variable
    infiles['CLD_CAL'].sort()

    for index in range(num_files_per_variable):

        f = cdms2.open(infiles['CLD_CAL'][index])

        # load the data for each variable
        variable_data = f('CLD_CAL')

        # load
        data = {
            'CLD_CAL': variable_data,
            'lat': variable_data.getLatitude(),
            'lon': variable_data.getLongitude(),
            'lat_bnds': f('lat_bnds'),
            'lon_bnds': f('lon_bnds'),
            'time': variable_data.getTime(),
            'time_bnds': f('time_bnds'),
            'cosp_ht': variable_data.getAxis(1)[:],
            'cosp_ht_bnds': f('cosp_ht_bnds')[:]
        }

        # create the cmor variable and axis
        axes = [{
            str('table_entry'): str('time'),
            str('units'): data['time'].units
        }, {
            str('table_entry'): str('alt40'),
            str('units'): str('m'),
            str('coord_vals'): data['cosp_ht'],
            str('cell_bounds'): data['cosp_ht_bnds']
        },{
            str('table_entry'): str('latitude'),
            str('units'): data['lat'].units,
            str('coord_vals'): data['lat'][:],
            str('cell_bounds'): data['lat_bnds'][:]
        }, {
            str('table_entry'): str('longitude'),
            str('units'): data['lon'].units,
            str('coord_vals'): data['lon'][:],
            str('cell_bounds'): data['lon_bnds'][:]
        }]

        axis_ids = list()
        for axis in axes:
            axis_id = cmor.axis(**axis)
            axis_ids.append(axis_id)

        varid = cmor.variable(VAR_NAME, VAR_UNITS, axis_ids)

        # write out the data
        msg = "{}: writing {} - {}".format(
            VAR_NAME,
            data['time_bnds'][0][0],
            data['time_bnds'][-1][-1])
        if logging:
            logging.info(msg)
        if serial:
            print(msg)
            pbar = ProgressBar(maxval=len(data['time']))
            pbar.start()
            for index, val in enumerate(data['time']):

                write_data(
                    varid=varid,
                    data=data,
                    timeval=val,
                    timebnds=[data['time_bnds'][index, :]],
                    index=index)
                pbar.update(index)
            pbar.finish()
        else:
            for index, val in enumerate(data['time']):
                write_data(
                    varid=varid,
                    data=data,
                    timeval=val,
                    timebnds=[data['time_bnds'][index, :]],
                    index=index)
    msg = '{}: write complete, closing'.format(VAR_NAME)

    if serial:
        print(msg)
    cmor.close()
    msg = '{}: file close complete'.format(VAR_NAME)

    if serial:
        print(msg)

    return VAR_NAME
# ------------------------------------------------------------------
