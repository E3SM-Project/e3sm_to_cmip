"""
CLDICE to cli converter
"""
from __future__ import absolute_import, division, print_function, unicode_literals

import os
import cmor
import cdms2
import logging

from tqdm import tqdm
from e3sm_to_cmip.util import print_message
from e3sm_to_cmip.util import get_dimension_data
from e3sm_to_cmip.util import setup_cmor
from e3sm_to_cmip.util import load_axis

# list of raw variable names needed
RAW_VARIABLES = [str('CLDICE')]

# output variable name
VAR_NAME = str('cli')
VAR_UNITS = str('kg kg-1')


def handle(infiles, tables, user_input_path, position=None):
    """
    Transform E3SM.CLDICE into CMIP.cli

    Parameters
    ----------
        infiles (List): a list of strings of file names for the raw input data
        tables (str): path to CMOR tables
        user_input_path (str): path to user input json file
    Returns
    -------
        var name (str): the name of the processed variable after processing is complete
    """

    msg = '{name}: Starting'.format(name=__name__)
    print(msg)
    logging.info(msg)
    get_dims = True
    
    # setup cmor
    setup_cmor(
        VAR_NAME,
        tables,
        'CMIP6_Amon.json',
        user_input_path)

    msg = '{}: CMOR setup complete'.format(__name__)
    print(msg)
    logging.info(msg)

    try:
        data = {}

        # num_variables = len(RAW_VARIABLES)
        num_files_per_variable = len(infiles[RAW_VARIABLES[0]]) # assuming all year ranges are the same for every variable

        # sort the input files for each variable
        for var_name in RAW_VARIABLES:
            infiles[var_name].sort()

        for index in range(num_files_per_variable):

            get_dims = True
            # load data for each variable
            for var_name in RAW_VARIABLES:
                
                # extract data from the input file
                msg = '{name}: loading {variable}'.format(
                    name=__name__, 
                    variable=var_name)
                logging.info(msg)

                new_data = get_dimension_data(
                    filename=infiles[var_name][index],
                    variable=var_name,
                    levels=True,
                    get_dims=get_dims)
                data.update(new_data)
                get_dims = False 
            
            msg = '{name}: loading axes'.format(name=__name__)
            logging.info(msg)
            axis_ids, ips = load_axis(
                data=data,
                levels=True)
            # create the cmor variable
            varid = cmor.variable(VAR_NAME, VAR_UNITS, axis_ids)

            # write out the data
            for index, val in enumerate(
                tqdm(
                    data['time'], 
                    position=position,
                    desc="{}: {} - {}".format(
                        __name__, 
                        data['time_bnds'][0][0], 
                        data['time_bnds'][-1][-1]))):
                
                cmor.write(
                    varid,
                    data['CLDICE'][index, :],
                    time_vals=val,
                    time_bnds=[ data['time_bnds'][index, :] ])
                cmor.write(
                    ips,
                    data['ps'],
                    time_vals=val,
                    time_bnds=[ data['time_bnds'][index, :] ],
                    store_with=varid)
    finally:
        cmor.close()

    return VAR_NAME
