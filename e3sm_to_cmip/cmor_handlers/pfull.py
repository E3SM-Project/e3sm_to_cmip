"""
CLDICE to cli converter
"""
from __future__ import absolute_import, division, print_function, unicode_literals

import cmor
import cdms2
import logging
import os
import progressbar
from e3sm_to_cmip.lib import handle_variables
from e3sm_to_cmip.util import print_message
from cdutil.vertical import reconstructPressureFromHybrid

# list of raw variable names needed
RAW_VARIABLES = [str('CLOUD'), str('PS')]
VAR_NAME = str('pfull')
VAR_UNITS = str('Pa')
TABLE = str('CMIP6_Amon.json')
LEVELS = {
    'name': 'standard_hybrid_sigma',
    'units': '1',
    'e3sm_axis_name': 'lev',
    'e3sm_axis_bnds': 'ilev',
    'time_name': 'time2'
}


def write_data(varid, data, timeval, timebnds, index, **kwargs):

    outdata = reconstructPressureFromHybrid(
        data['PS'][index, :], data['hyam'], data['hybm'], data['p0'])
    cmor.write(
        varid,
        outdata,
        time_vals=timeval,
        time_bnds=timebnds)
    cmor.write(
        data['ips'],
        data['ps'],
        time_vals=timeval,
        time_bnds=timebnds,
        store_with=varid)
# ------------------------------------------------------------------


def my_dynamic_message(self, progress, data):
    """
    Make the progressbar not crash, and also give a nice custom message
    """
    val = data['dynamic_messages'].get('running')
    if val:
        return 'Running: {0: <16}'.format(data['dynamic_messages'].get('running'))
    else:
        return 'Running: ' + 16 * '-'
# ------------------------------------------------------------------


def handle(infiles, tables, user_input_path, **kwargs):

    logger = logging.getLogger()
    msg = '{}: Starting'.format(VAR_NAME)
    logger.info(msg)

    serial = kwargs.get('serial')
    logdir = kwargs.get('logdir')

    # check that we have some input files for every variable
    zerofiles = False
    for variable in RAW_VARIABLES:
        if len(infiles[variable]) == 0:
            msg = '{}: Unable to find input files for {}'.format(
                VAR_NAME, variable)
            print_message(msg)
            logging.error(msg)
            zerofiles = True
    if zerofiles:
        return None

    # Create the logging directory and setup cmor
    logdir = kwargs.get('logdir')
    if logdir:
        logpath = logdir
    else:
        outpath, _ = os.path.split(logger.__dict__['handlers'][0].baseFilename)
        logpath = os.path.join(outpath, 'cmor_logs')
    os.makedirs(logpath, exist_ok=True)

    logfile = os.path.join(logpath, VAR_NAME + '.log')

    cmor.setup(
        inpath=tables,
        netcdf_file_action=cmor.CMOR_REPLACE,
        logfile=logfile)

    cmor.dataset_json(str(user_input_path))
    cmor.load_table(str(TABLE))

    msg = '{}: CMOR setup complete'.format(VAR_NAME)
    logging.info(msg)

    data = {}

    # assuming all year ranges are the same for every variable
    num_files_per_variable = len(infiles[RAW_VARIABLES[0]])

    # sort the input files for each variable
    for var_name in RAW_VARIABLES:
        infiles[var_name].sort()

    for index in range(num_files_per_variable):

        # load data for each variable
        for var_name in RAW_VARIABLES:

            # extract data from the input file
            msg = '{name}: loading {variable}'.format(
                name=VAR_NAME,
                variable=var_name)
            logger.info(msg)

            filename = infiles[var_name][index]
            new_data = {}

            if not os.path.exists(filename):
                raise IOError("File not found: {}".format(filename))

            f = cdms2.open(filename)

            # load the data for each variable
            variable_data = f(variable)

            if not variable_data.any():
                raise IOError("Variable data not found: {}".format(variable))

            data.update({
                variable: variable_data
            })

            # load the lon and lat info & bounds
            # load time & time bounds
            data.update({
                'lat': variable_data.getLatitude(),
                'lon': variable_data.getLongitude(),
                'lat_bnds': f('lat_bnds'),
                'lon_bnds': f('lon_bnds'),
                'time': variable_data.getTime(),
                'time2': variable_data.getTime(),
                'time_bnds': f('time_bnds')
            })

            # load level and level bounds
            data.update({
                'ps': f('PS')
            })
            if 'lev' in f.listdimension() and 'ilev' in f.listdimension():
                data.update({
                    'lev': f.getAxis('lev')[:]/1000,
                    'ilev': f.getAxis('ilev')[:]/1000
                })
            if 'P0' in f.variables:
                data.update({
                    'p0': f('P0')
                })
            new_data = {i: f(i) for i in [
                'hyam', 'hybm', 'hyai', 'hybi'] if i in f.variables}

            data.update(new_data)

        msg = '{name}: loading axes'.format(name=VAR_NAME)
        logger.info(msg)
        
        # create the cmor variable and axis
        axes = [{
            str('table_entry'): str('longitude'),
            str('units'): data['lon'].units,
            str('coord_vals'): data['lon'][:],
            str('cell_bounds'): data['lon_bnds'][:]
        }, {
            str('table_entry'): str('latitude'),
            str('units'): data['lat'].units,
            str('coord_vals'): data['lat'][:],
            str('cell_bounds'): data['lat_bnds'][:]
        }, {
            str('table_entry'): str('standard_hybrid_sigma'),
            str('units'): str('1'),
            str('coord_vals'): data['lev'][:],
            str('cell_bounds'): data['ilev'][:]
        }, {
            str('table_entry'): 'time2',
            str('units'): data['time2'].units
        }, {
            str('table_entry'): 'time',
            str('units'): data['time'].units
        }]

        axis_ids = list()
        for axis in axes:
            axis_id = cmor.axis(**axis)
            axis_ids.append(axis_id)

        ips = None
        import ipdb; ipdb.set_trace()
        # add hybrid level formula terms
        cmor.zfactor(
            zaxis_id=axis_ids[2],
            zfactor_name=str('a'),
            axis_ids=[axis_ids[2], ],
            zfactor_values=data['hyam'][:],
            zfactor_bounds=data['hyai'][:])
        cmor.zfactor(
            zaxis_id=axis_ids[2],
            zfactor_name=str('b'),
            axis_ids=[axis_ids[2], ],
            zfactor_values=data['hybm'][:],
            zfactor_bounds=data['hybi'][:])
        cmor.zfactor(
            zaxis_id=axis_ids[2],
            zfactor_name=str('p0'),
            units=str('Pa'),
            zfactor_values=data['p0'])
        ips = cmor.zfactor(
            zaxis_id=axis_ids[2],
            zfactor_name=str('ps'),
            axis_ids=[0, 1, 4],
            units=str('Pa'))

        data['ips'] = ips

        # import ipdb; ipdb.set_trace()
        varid = cmor.variable(VAR_NAME, VAR_UNITS, axis_ids[:4])

        # write out the data
        msg = "{}: time {:1.1f} - {:1.1f}".format(
            VAR_NAME,
            data['time_bnds'][0][0],
            data['time_bnds'][-1][-1])
        logger.info(msg)

        if serial:
            myMessage = progressbar.DynamicMessage('running')
            myMessage.__call__ = my_dynamic_message
            widgets = [
                progressbar.DynamicMessage('running'), ' [',
                progressbar.Timer(), '] ',
                progressbar.Bar(),
                ' (', progressbar.ETA(), ') '
            ]
            progressbar.DynamicMessage.__call__ = my_dynamic_message
            pbar = progressbar.ProgressBar(
                maxval=len(data['time']), widgets=widgets)
            pbar.start()

        for index, val in enumerate(data['time']):
            if serial:
                pbar.update(index, running=msg)
            write_data(
                varid=varid,
                data=data,
                timeval=val,
                timebnds=[data['time_bnds'][index, :]],
                index=index,
                RAW_VARIABLES=RAW_VARIABLES)
        if serial:
            pbar.finish()

    msg = '{}: write complete, closing'.format(VAR_NAME)
    logger.debug(msg)

    cmor.close()

    msg = '{}: file close complete'.format(VAR_NAME)
    logger.debug(msg)

# ------------------------------------------------------------------
# def handle(infiles, tables, user_input_path, **kwargs):
#     """
#     Parameters
#     ----------
#         infiles (List): a list of strings of file names for the raw input data
#         tables (str): path to CMOR tables
#         user_input_path (str): path to user input json file
#     Returns
#     -------
#         var name (str): the name of the processed variable after processing is complete
#     """

#     return handle_variables(
#         metadata_path=user_input_path,
#         tables=tables,
#         table=TABLE,
#         infiles=infiles,
#         RAW_VARIABLES=RAW_VARIABLES,
#         write_data=write_data,
#         VAR_NAME=VAR_NAME,
#         outvar_units=VAR_UNITS,
#         serial=kwargs.get('serial'),
#         levels=LEVELS,
#         logdir=kwargs.get('logdir'))
