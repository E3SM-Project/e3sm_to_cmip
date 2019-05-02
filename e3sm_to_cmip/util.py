from __future__ import absolute_import, division, print_function, unicode_literals

import sys
import traceback
import cmor
import os
import re
import argparse
import imp
import cdms2

from progressbar import ProgressBar


def print_debug(e):
    """
    Return a string of an exceptions relavent information
    """
    _, _, tb = sys.exc_info()
    traceback.print_tb(tb)
    print(e)
# ------------------------------------------------------------------


class colors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
# ------------------------------------------------------------------


def print_message(message, status='error'):
    """
    Prints a message with either a green + or a red -

    Parameters:
        message (str): the message to print
        status (str): th"""
    if status == 'error':
        print(colors.FAIL + '[-] ' + colors.ENDC + colors.BOLD + str(message)
              + colors.ENDC)
    elif status == 'ok':
        print(colors.OKGREEN + '[+] ' + colors.ENDC + str(message))
    elif status == 'debug':
        print(colors.OKBLUE + '[*] ' + colors.ENDC + str(message)
              + colors.OKBLUE + ' [*]' + colors.ENDC)
# ------------------------------------------------------------------


def setup_cmor(var_name, table_path, table_name, user_input_path):
    """
    Sets up cmor and logging for a single handler
    """
    var_name = str(var_name)
    table_path = str(table_path)
    table_name = str(table_name)
    user_input_path = str(user_input_path)

    logfile = os.path.join(os.getcwd(), 'logs')
    if not os.path.exists(logfile):
        os.makedirs(logfile)

    logfile = os.path.join(logfile, var_name + '.log')
    cmor.setup(
        inpath=table_path,
        netcdf_file_action=cmor.CMOR_REPLACE,
        logfile=logfile)

    cmor.dataset_json(user_input_path)
    cmor.load_table(table_name)
# ------------------------------------------------------------------


def parse_argsuments():
    parser = argparse.ArgumentParser(
        description='Convert ESM model output into CMIP compatible format',
        prog='e3sm_to_cmip',
        usage='%(prog)s [-h]')
    parser.add_argument(
        '-v', '--var-list',
        nargs='+',
        required=True,
        metavar='',
        help='space seperated list of variables to convert from e3sm to cmip. Use \'all\' to convert all variables')
    parser.add_argument(
        '-i', '--input-path',
        metavar='',
        required=True,
        help='path to directory containing e3sm time series data files. Additionally namelist, restart, and mappings files if handling MPAS data.')
    parser.add_argument(
        '-o', '--output-path',
        metavar='',
        required=True,
        help='where to store cmorized output')
    parser.add_argument(
        '-u', '--user-metadata',
        required=True,
        metavar='<user_input_json_path>',
        help='path to user json file for CMIP6 metadata')
    parser.add_argument(
        '-t', '--tables-path',
        required=True,
        metavar='<tables-path>',
        help="Path to directory containing CMOR Tables directory")
    parser.add_argument(
        '--map',
        metavar='<map_mpas_to_std_grid>',
        help="The path to an mpas remapping file. Must be used when using the --mpaso or --mpassi options")
    parser.add_argument(
        '-n', '--num-proc',
        metavar='<nproc>',
        default=6,
        type=int,
        help='optional: number of processes, default = 6')
    parser.add_argument(
        '-H', '--handlers',
        metavar='<handler_path>',
        default=None,
        help='path to cmor handlers directory, default = e3sm_to_cmip/cmor_handlers')
    parser.add_argument(
        '--no-metadata',
        help='Do not add E3SM metadata to the output',
        action='store_true')
    parser.add_argument(
        '-s', '--serial',
        help='Run in serial mode, usefull for debugging purposes',
        action="store_true")
    parser.add_argument(
        '--debug',
        help='Set output level to debug',
        action='store_true')
    parser.add_argument(
        '--mode',
        metavar='<mode>',
        help="The component to analyze, atm, lnd, ocn or ice")
    parser.add_argument(
        '--version',
        help='print the version number and exit',
        action='version',
        version='%(prog)s 0.0.2')
    try:
        _args = sys.argv[1:]
    except (Exception, BaseException):
        parser.print_help()
        sys.exit(1)
    else:
        _args = parser.parse_args(_args)
        if _args.mode == 'ocn' and not _args.map:
            print("MPAS ocean handling requires a map file")
        if _args.mode == 'ice' and not _args.map:
            print("MPAS sea-ice handling requires a map file")
    return _args
# ------------------------------------------------------------------


def load_handlers(handlers_path, var_list, debug=None):
    """
    load the cmor handler modules

    Params:
    -------
        handlers_path (str): the path to the python module to load handlers from
        var_list (list(str)): A list of strings with the names of cmip6 variables to convert to, optionally "all" to run all handlers found
    Returns:
    --------
        handlers (list(dict()): A list of dictionaries mapping module names
        (which are the cmip6 output variable name), to a tuple of (function pointer,
        list of required input variables)
    """

    handlers = list()

    # load the more complex handlers
    for handler in os.listdir(handlers_path):

        if not handler.endswith('.py'):
            continue
        if handler == "__init__.py":
            continue

        module_name, _ = handler.rsplit('.', 1)
        if module_name not in var_list and 'all' not in var_list:
            continue

        to_break = False
        for h in handlers:
            if h['name'] == module_name:
                to_break = True
        if to_break:
            break

        module_path = os.path.join(handlers_path, handler)

        # load the module, and extract the "handle" method and required variables
        module = imp.load_source(module_name, module_path)

        handlers.append({
            'name': module_name,
            'method': module.handle,
            'raw_variables': module.RAW_VARIABLES,
            'units': module.VAR_UNITS,
            'table': module.TABLE,
            'positive': module.POSITIVE if hasattr(module, 'POSITIVE') else None
        })

    for handler in handlers:
        msg = 'Loaded {}'.format(handler['name'])
        if debug:
            print_message(msg, 'debug')

    return handlers
# ------------------------------------------------------------------


def copy_user_metadata(input_path, output_path):
    """
    write out the users input file for cmor into the output directory
    and replace the output line with the path to the output directory

    Params:
    -------
        input_path (str): A path to the users original cmor metadata json file
        output_path (str): The new path for the updated metadata file with the output path changed
    """
    try:
        fin = open(input_path, "r")
    except IOError as error:
        print("Unable to write out metadata")
        raise error
    try:
        fout = open(os.path.join(output_path, 'user_metadata.json'), "w")
    except IOError as error:
        print("Unable to open output location for custom user metadata")
        raise error
    try:
        for line in fin:
            if 'outpath' in line:
                fout.write('\t"outpath": "{}",\n'.format(
                    output_path))
            else:
                fout.write(line)
    except IOError as error:
        print("Write failure for user metadata")
        raise error
    finally:
        fin.close()
        fout.close()
# ------------------------------------------------------------------


def add_metadata(file_path, var_list):
    """
    Recurses down a file try, adding metadata to any netcdf files in the tree
    that are on the variable list.

    Parameters
    ----------
        file_path (str): the root directory to search for files under
        var_list (list(str)): a list of cmip6 variable names
    """
    filepaths = list()

    print_message('Adding additional metadata to output files', 'ok')
    for root, dirs, files in os.walk(file_path, topdown=False):
        for name in files:
            if name[-3:] != '.nc':
                continue
            index = name.find('_')
            if index != -1 and name[:index] in var_list or 'all' in var_list:
                filepaths.append(os.path.join(root, name))

    pbar = ProgressBar(maxval=len(filepaths))
    pbar.start()
    for idx, filepath in enumerate(filepaths):
        datafile = cdms2.open(filepath, 'a')
        try:
            datafile.e3sm_source_code_doi = str('10.11578/E3SM/dc.20180418.36')
            datafile.e3sm_paper_reference = str(
                'https://doi.org/10.1029/2018MS001603')
            datafile.e3sm_source_code_reference = str(
                'https://github.com/E3SM-Project/E3SM/releases/tag/v1.0.0')
            datafile.doe_acknowledgement = str(
                'This research was supported as part of the Energy Exascale Earth System Model (E3SM) project, funded by the U.S. Department of Energy, Office of Science, Office of Biological and Environmental Research.')
            datafile.computational_acknowledgement = str(
                'The data were produced using resources of the National Energy Research Scientific Computing Center, a DOE Office of Science User Facility supported by the Office of Science of the U.S. Department of Energy under Contract No. DE-AC02-05CH11231.')
            datafile.ncclimo_generation_command = str(
                """ncclimo --var=${var} -7 --dfl_lvl=1 --no_cll_msr --no_frm_trm --no_stg_grd --yr_srt=1 --yr_end=500 --ypf=500 --map=map_ne30np4_to_cmip6_180x360_aave.20181001.nc """)
            datafile.ncclimo_version = str('4.7.9')
            pbar.update(idx)
        finally:
            datafile.close()
    pbar.finish()
# ------------------------------------------------------------------


def find_atm_files(var, path):
    """
    Looks in the given path for all files that match that match VAR_\d{6}_\d{6}.nc

    Params:
    -------
        var (str): the name of the variable to look for
        path (str): the path of the directory to look in
    Returns:
    --------
        files (list(str)): A list of paths to the matching files
    """
    contents = os.listdir(path)
    files = list()
    pattern = '{}'.format(var) + r'\_\d{6}\_\d{6}.nc'
    for item in contents:
        if re.match(pattern=pattern, string=item):
            files.append(item)
    return files
# ------------------------------------------------------------------


def find_mpas_files(component, path, map_path):
    """
    Looks in the path given for MPAS monthly-averaged files

    Params:
    -------
        component (str): Either the mpaso or mpassi component name
        path (str): The path of the directory to search for files in
    """
    # save original in case it's an atm var
    var = str(component)
    component = component.lower()
    contents = os.listdir(path)

    if component in ['mpaso', 'mpassi']:

        pattern = '{}.hist.am.timeSeriesStatsMonthly.'.format(component) + \
            r'\d{4}-\d{2}-\d{2}.nc'
        result = [os.path.join(path, x) for x in contents if re.match(
            pattern=pattern, string=x)]

        if component == 'mpassi' and len(result) == 0:
            pattern = r'mpascice.hist.am.timeSeriesStatsMonthly.\d{4}-\d{2}-' \
                '\d{2}.nc'
            result = [os.path.join(path, x) for x in contents if re.match(
                pattern=pattern, string=x)]
        return sorted(result)

    elif component == 'mpaso_namelist':

        pattern = 'mpaso_in'
        for infile in contents:
            if re.match(pattern, infile):
                return os.path.abspath(os.path.join(path, infile))
        pattern = 'mpas-o_in'
        for infile in contents:
            if re.match(pattern, infile):
                return os.path.abspath(os.path.join(path, infile))

        raise IOError("Unable to find MPASO_namelist in the input directory")

    elif component == 'mpassi_namelist':

        pattern = 'mpassi_in'
        for infile in contents:
            if re.match(pattern, infile):
                return os.path.abspath(os.path.join(path, infile))
        pattern = 'mpas-cice_in'
        for infile in contents:
            if re.match(pattern, infile):
                return os.path.abspath(os.path.join(path, infile))

        raise IOError("Unable to find MPASSI_namelist in the input directory")

    elif component == 'mpas_mesh':

        pattern = r'mpaso.rst.\d{4}-\d{2}-\d{2}_\d{5}.nc'
        for infile in contents:
            if re.match(pattern, infile):
                return os.path.abspath(os.path.join(path, infile))

    elif component == 'mpas_map':

        return os.path.abspath(map_path)

    elif component == 'mpaso_moc_regions':

        pattern = '_region_'
        for infile in contents:
            if pattern in infile:
                return os.path.abspath(os.path.join(path, infile))

    else:
        files = find_atm_files(var, path)
        if len(files) > 0:
            files = [os.path.join(path, name) for name in files]
            return files
        else:
            raise ValueError("Unrecognized component {}, unable to find input "
                             "files".format(component))

# ------------------------------------------------------------------


def terminate(pool, debug=False):
    """
    Terminates the process pool

    Params:
    -------
        pool (multiprocessing.Pool): the pool to shutdown
        debug (bool): if we're running in debug mode
    Returns:
    --------
        None
    """
    if debug:
        print_message('Shutting down process pool', 'debug')
    pool.close()
    pool.terminate()
    pool.join()
# ------------------------------------------------------------------
