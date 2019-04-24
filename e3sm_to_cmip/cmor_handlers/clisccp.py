"""
FISCCP1_COSP to clisccp converter
"""
from __future__ import absolute_import, division, print_function, unicode_literals

import os
import cmor
import cdms2
import logging

from progressbar import ProgressBar

from e3sm_to_cmip.util import print_message
from e3sm_to_cmip.lib import get_dimension_data
from e3sm_to_cmip.util import setup_cmor
from e3sm_to_cmip.lib import load_axis
from e3sm_to_cmip.lib import handle_variables

# list of raw variable names needed
RAW_VARIABLES = [str('FISCCP1_COSP')]
VAR_NAME = str('clisccp')
VAR_UNITS = str('%')
TABLE = str('CMIP6_CFmon.json')


def write_data(varid, data, timeval, timebnds, index):
    """
    clisccp = FISCCP1_COSP with plev7c, and tau
    """
    cmor.write(
        varid,
        data['FISCCP1_COSP'][index, :],
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
    num_files_per_variable = len(infiles['FISCCP1_COSP'])

    # sort the input files for each variable
    infiles['FISCCP1_COSP'].sort()

    for index in range(num_files_per_variable):

        f = cdms2.open(infiles['FISCCP1_COSP'][index])

        # load the data for each variable
        variable_data = f('FISCCP1_COSP')

        tau = variable_data.getAxis(2)[:]
        tau[-1] = 100.0
        tau_bnds = f.variables['cosp_tau_bnds'][:]
        tau_bnds[-1] = [60.0, 100000.0]

        # load
        data = {
            'FISCCP1_COSP': variable_data,
            'lat': variable_data.getLatitude(),
            'lon': variable_data.getLongitude(),
            'lat_bnds': f('lat_bnds'),
            'lon_bnds': f('lon_bnds'),
            'time': variable_data.getTime(),
            'time_bnds': f('time_bnds'),
            'plev7c': variable_data.getAxis(1)[:] * 100.0,
            'plev7c_bnds': f.variables['cosp_prs_bnds'][:] * 100.0,
            'tau': tau,
            'tau_bnds': tau_bnds
        }

        # create the cmor variable and axis
        axes = [{
            str('table_entry'): str('time'),
            str('units'): data['time'].units
        }, {
            str('table_entry'): str('plev7c'),
            str('units'): str('Pa'),
            str('coord_vals'): data['plev7c'],
            str('cell_bounds'): data['plev7c_bnds']
        },{
            str('table_entry'): str('tau'),
            str('units'): str('1'),
            str('coord_vals'): data['tau'],
            str('cell_bounds'): data['tau_bnds']
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
