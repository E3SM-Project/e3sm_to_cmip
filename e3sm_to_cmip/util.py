from __future__ import absolute_import, division, print_function, unicode_literals

import sys
import traceback
import cmor
import os
import re
import argparse
import imp
import yaml
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


def parse_argsuments(version):
    parser = argparse.ArgumentParser(
        description='Convert ESM model output into CMIP compatible format',
        prog='e3sm_to_cmip',
        usage='%(prog)s [-h]')
    parser.add_argument(
        '-v', '--var-list',
        nargs='+',
        required=True,
        metavar='',
        help='space seperated list of variables to convert from e3sm to cmip. Use \'all\' to convert all variables or the name of a CMIP6 table to run all handlers from that table')
    parser.add_argument(
        '-i', '--input-path',
        metavar='',
        required=False,
        help='path to directory containing e3sm time series data files. Additionally namelist, restart, and mappings files if handling MPAS data.')
    parser.add_argument(
        '-o', '--output-path',
        metavar='',
        required=True,
        help='where to store cmorized output')
    parser.add_argument(
        '-u', '--user-metadata',
        required=False,
        metavar='<user_input_json_path>',
        help='path to user json file for CMIP6 metadata')
    parser.add_argument(
        '-t', '--tables-path',
        required=False,
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
        '--only-metadata',
        help='Update the metadata for any files found and exit',
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
        '--logdir',
        help="Where to put the logging output from CMOR")
    parser.add_argument(
        '--no-rm-tmpdir',
        help='Dont remove the temp dir on exit',
        action='store_true')
    parser.add_argument(
        '--timeout',
        help='Exit with code -1 if execution time exceeds given time in seconds')
    parser.add_argument(
        '--precheck',
        help="Check for each variable if its already in the output CMIP6 directory, only run variables that dont have CMIP6 output",
        action="store_true")
    parser.add_argument(
        '--version',
        help='print the version number and exit',
        action='version',
        version='%(prog)s {}'.format(version))
    try:
        _args = sys.argv[1:]
    except (Exception, BaseException):
        parser.print_help()
        sys.exit(1)
    else:
        _args = parser.parse_args(_args)
        if _args.mode == 'mpaso' and not _args.map:
            print("MPAS ocean handling requires a map file")
        if _args.mode == 'mpassi' and not _args.map:
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
    from e3sm_to_cmip.default import default_handler

    handlers = list()

    if debug:
        print_message(
            "looking for handlers for: {}".format(" ".join(var_list)))

    if 'all' not in var_list:
        table_names = ['CFmon', 'Amon', 'Lmon',
                       'Omon', 'AERmon', 'SImon', 'LImon']
        load_tables = list()
        for variable in var_list:
            if variable in table_names:
                load_tables.append(variable)

    # load default handlers if they're in the variable list
    defaults_path = os.path.join(
        handlers_path,
        'default_handler_info.yaml')
    with open(defaults_path, 'r') as infile:

        defaults = yaml.load(infile)

        for default in defaults:

            table = default.get('table').split('.')[0].split('_')[-1]
            if default.get('cmip_name') in var_list or 'all' in var_list or table in load_tables:

                handlers.append({
                    'name': default.get('cmip_name'),
                    'method': default_handler,
                    'raw_variables': [default.get('e3sm_name')],
                    'units': default.get('units'),
                    'table': default.get('table'),
                    'positive': default.get('positive')
                })
            elif debug:
                print_message("{} not loaded".format(default.get('cmip_name')))

    # load the more complex handlers
    for handler in os.listdir(handlers_path):

        if not handler.endswith('.py'):
            continue
        if handler == "__init__.py":
            continue

        module_name, _ = handler.rsplit('.', 1)

        dup = False
        for h in handlers:
            if h['name'] == module_name:
                dup = True
                break
        if dup:
            continue

        module_path = os.path.join(handlers_path, handler)

        # load the module, and extract the "handle" method and required variables
        module = imp.load_source(module_name, module_path)

        # pull the table name out from the format CMIP6_Amon.json
        table = module.TABLE.split('.')[0].split('_')[-1]

        if module_name in var_list or 'all' in var_list or table in load_tables:

            handlers.append({
                'name': module_name,
                'method': module.handle,
                'raw_variables': module.RAW_VARIABLES,
                'units': module.VAR_UNITS,
                'table': module.TABLE,
                'positive': module.POSITIVE if hasattr(module, 'POSITIVE') else None
            })
        elif debug:
            print_message("{} not loaded".format(module_name))

    if debug:
        for handler in handlers:
            msg = 'Loaded {}'.format(handler['name'])
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
    Recurses down a file tree, adding metadata to any netcdf files in the tree
    that are on the variable list.

    Parameters
    ----------
        file_path (str): the root directory to search for files under
        var_list (list(str)): a list of cmip6 variable names
    """
    filepaths = list()

    print_message('Adding additional metadata to output files', 'ok')
    for root, _, files in os.walk(file_path, topdown=False):
        for name in files:
            if name[-3:] != '.nc':
                continue
            index = name.find('_')
            if index != -1 and name[:index] in var_list or 'all' in var_list:
                filepaths.append(os.path.join(root, name))

    pbar = ProgressBar(maxval=len(filepaths))
    pbar.start()

    for idx, filepath in enumerate(filepaths):

        datafile = cdms2.open(filepath, 'r+')
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
            """ncclimo --var=${var} -7 --dfl_lvl=1 --no_cll_msr --no_frm_trm --no_stg_grd --yr_srt=1 --yr_end=500 --ypf=25 --map=map_ne30np4_to_cmip6_180x360_aave.20181001.nc """)
        datafile.ncclimo_version = str('4.8.1-alpha04')

        # picontrol specific
        # datafile.base_year = str("1850")

        datafile.close()
        pbar.update(idx)

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


def find_mpas_files(component, path, map_path=None):
    """
    Looks in the path given for MPAS monthly-averaged files

    Params:
    -------
        component (str): Either the mpaso or mpassi component name or variable name
        path (str): The path of the directory to search for files in
    """
    # save original in case it's an atm var
    var = str(component)
    component = component.lower()
    contents = os.listdir(path)

    if component == 'mpaso':

        pattern = r'mpaso.hist.am.timeSeriesStatsMonthly.\d{4}-\d{2}-\d{2}.nc'
        results = [os.path.join(path, x) for x in contents if re.match(
            pattern=pattern, string=x)]
        return sorted(results)

    if component == 'mpassi':

        patterns = [r'mpassi.hist.am.timeSeriesStatsMonthly.\d{4}-\d{2}-\d{2}.nc', 
                    r'mpascice.hist.am.timeSeriesStatsMonthly.\d{4}-\d{2}-\d{2}.nc']
        for pattern in patterns:
            results = [os.path.join(path, x) for x in contents if re.match(
                pattern=pattern, string=x)]
            if results:
                return sorted(results)
        raise IOError("Unable to find mpassi in the input directory")

    elif component == 'mpaso_namelist':

        patterns = ['mpaso_in', 'mpas-o_in']
        for pattern in patterns:
            for infile in contents:
                if re.match(pattern, infile):
                    return os.path.abspath(os.path.join(path, infile))
        raise IOError("Unable to find MPASO_namelist in the input directory")

    elif component == 'mpassi_namelist':

        patterns = ['mpassi_in', 'mpas-cice_in']
        for pattern in patterns:
            for infile in contents:
                if re.match(pattern, infile):
                    return os.path.abspath(os.path.join(path, infile))
        raise IOError("Unable to find MPASSI_namelist in the input directory")

    elif component == 'mpas_mesh':

        pattern = r'mpaso.rst.\d{4}-\d{2}-\d{2}_\d{5}.nc'
        for infile in contents:
            if re.match(pattern, infile):
                return os.path.abspath(os.path.join(path, infile))
        raise IOError("Unable to find mpas_mesh in the input directory")

    elif component == 'mpas_map':
        if not map_path:
            raise ValueError("No map path given")
        map_path = os.path.abspath(map_path)
        if os.path.exists(map_path):
            return map_path
        else:
            raise IOError("Unable to find mpas_map in the input directory")

    elif component == 'mpaso_moc_regions':

        pattern = '_region_'
        for infile in contents:
            if pattern in infile:
                return os.path.abspath(os.path.join(path, infile))
        raise IOError(
            "Unable to find mpaso_moc_regions in the input directory")

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


def get_levgrnd_bnds():
    return [0, 0.01751106046140194, 0.045087261125445366, 0.09055273048579693, 0.16551261954009533, 0.28910057805478573, 0.4928626772016287, 0.8288095649331808, 1.3826923426240683, 2.2958906944841146, 3.801500206813216, 6.28383076749742, 10.376501685008407, 17.124175196513534, 28.249208575114608, 42.098968505859375]
# ------------------------------------------------------------------


def get_years_from_raw(path, mode, var):
    """
    given a file path, return the start and end years for the data
    Parameters:
    -----------
        path (str): the directory to look in for data
        mode (str): the type of data to look for, i.e atm, lnd, mpaso, mpassi
    """
    start = 0
    end = 0
    if mode in ['atm', 'lnd']:
        contents = sorted([f for f in os.listdir(path)
                           if f.endswith("nc") and 
                           var in f])
        p = var + r'\d{6}_\d{6}.nc'
        s = re.match(pattern=p, string=contents[0])
        start = int(contents[0][ s.start(): s.start()+4 ])
        s = re.search(pattern=p, string=contents[-1])
        end = int(contents[-1][ s.start(): s.start()+4 ])
    elif mode in ['mpassi', 'mpaso']:
        
        files = sorted(find_mpas_files(mode, path))
        p = r'\d{4}-\d{2}-\d{2}.nc'
        s = re.search(pattern=p, string=files[0])
        start = int(files[0][s.start(): s.start() + 4])
        s = re.search(pattern=p, string=files[1])
        end = int(files[-1][s.start(): s.start() + 4])
        
    else:
        raise ValueError("Invalid mode")
    return start, end


def get_year_from_cmip(filename):
    """
    Given a file name, assuming its a cmip file, return the start and end year
    """
    p = r'\d{6}-\d{6}\.nc'
    s = re.search(pattern=p, string=filename)
    if not s:
        raise ValueError("unable to match file years for {}".format(filename))

    start, end = [int(x[:-2]) if not x.endswith('.nc') else int(x[:-5])
                  for x in filename[s.span()[0]: s.span()[1]].split('-')]
    return start, end


def precheck(inpath, outpath, variables, mode):
    """
    Check if the data has already been produced and skip

    returns a list of variable names that were not found in the output directory with matching years
    """

    # First check the inpath for the start and end years
    start, end = get_years_from_raw(inpath, mode, variables[0])
    var_map = [{'found': False, 'name': var} for var in variables]

    # then check the output tree for files with the correct variables for those years
    if mode in ['mpaso', 'mpassi']:
        for val in var_map:
            for _, _, files in os.walk(outpath, topdown=False):
                if files and val['name'] in files[0]:
                    files = [x for x in sorted(files) if x.endswith('.nc')]
                    for f in files:
                        cmip_start, cmip_end = get_year_from_cmip(f)
                        if cmip_start == start and cmip_end == end:
                            val['found'] = True
                            break
                    if val['found'] == True:
                        break
        
        return [x['name'] for x in var_map if not x['found']]
    else:
        raise ValueError("still working on it")
