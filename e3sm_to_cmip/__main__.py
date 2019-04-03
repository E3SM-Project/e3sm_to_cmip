#!/usr/bin/env python
"""
A python command line tool to turn E3SM model output into CMIP6 compatable data
"""

from __future__ import absolute_import, division, print_function, unicode_literals

import os
import sys
import argparse
import re
import logging
import imp
import cdms2

from multiprocessing import cpu_count, Pool
from time import sleep

from e3sm_to_cmip.util import format_debug, print_message

import numpy as np
np.warnings.filterwarnings('ignore')


class Cmorizer(object):
    """
    A utility class to cmorize e3sm time series files
    """

    def __init__(self, var_list, input_path, user_input_path, tables_path, num_proc=None, output_path='.', handlers=None, no_metadata=False, serial=False, debug=False, **kwargs):
        """
        Parameters:
            var_list (list(str)): a list of strings of variable names to extract, or 'all' to extract all possible
            input_path (str): path to the input data
            user_input_path (str): path the user supplied input.json file required by CMOR
            tables_path (str): path to the tables directory supplied by CMOR
            output_path (str): path to where to store the output
            nproc (int): number of parallel processes
            proc_vars (bool): should we create as many processes as there are variables?
            handlers (str): path to directory containing cmor handlers
        """
        self._var_list = var_list
        self._input_path = input_path
        self._tables_path = tables_path

        if isinstance(self._tables_path, unicode):
            self._tables_path = self._tables_path.encode('ascii', 'ignore')

        self._nproc = num_proc
        self._output_path = output_path
        self._no_metadata = no_metadata
        self._serial = serial

        self._user_input_path = os.path.join(
            self._output_path, 
            'user_input.json')
        if isinstance(self._user_input_path, unicode):
            self._user_input_path = self._user_input_path.encode('ascii', 'ignore')

        if handlers is not None:
            self._handlers_path = handlers
        else:
            self._handlers_path = os.path.join(
                sys.prefix,
                'share',
                'e3sm_to_cmip',
                'cmor_handlers')

        self._handlers_path = os.path.abspath(self._handlers_path)
        self._debug = debug
        self._pool = None
        self._pool_res = None
        self._handlers = list()

        if not os.path.exists(self._output_path):
            os.makedirs(self._output_path)

        logging.basicConfig(
            format='%(asctime)s:%(levelname)s: %(message)s',
            datefmt='%m/%d/%Y %I:%M:%S %p',
            filename=os.path.join(self._output_path, 'converter.log'),
            filemode='w',
            level=logging.ERROR)

        # write out the users input file for cmor into the output directory
        # and replace the output line with the path to the output directory
        with open(user_input_path, "r") as fin:
            with open(self._user_input_path, "w") as fout:
                for line in fin:
                    if 'outpath' in line:
                        fout.write('\t"outpath": "{}",\n'.format(
                            self._output_path))
                    else:
                        fout.write(line)

    def load_handlers(self):
        """
        load the cmor handler modules
        """

        handler_names = os.listdir(self._handlers_path)

        # if the handler director doesnt have an __init__.py file create one
        if "__init__.py" not in handler_names:
            with open(os.path.join(self._handlers_path, '__init__.py'), 'w') as fp:
                fp.write('\n')

        for handler in handler_names:
            if not handler.endswith('.py'):
                continue
            if handler == "__init__.py":
                continue

            module_name, _ = handler.rsplit('.', 1)

            # ignore handlers for variables that werent requested
            if 'all' not in self._var_list:
                if module_name not in self._var_list:
                    continue

            module_path = os.path.join(self._handlers_path, handler)

            # load the module, and extract the "handle" method and required variables
            try:
                module = imp.load_source(module_name, module_path)
                method = module.handle
                raw_variables = module.RAW_VARIABLES
            except ImportError as e:
                msg = format_debug(e)
                print_message(
                    'Error loading handler for {}'.format(module_path))
                print_message(msg)
                logging.error(msg)
                continue
            else:
                msg = 'Loaded {}'.format(module_name)
                if self._debug:
                    print_message(msg, 'debug')
                logging.info(msg)
            self._handlers.append({module_name: (method, raw_variables)})

    def run(self):
        """
        run all the requested CMOR handlers
        """
        self.load_handlers()
        if len(self._handlers) == 0:
            print_message('No handlers loaded')
            sys.exit(1)

        if self._serial:
            print_message('Running CMOR handlers in serial', 'ok')
            self.run_serial()
        else:
            self.run_parallel()

        if self._no_metadata:
            print_message('Not adding additional metadata', 'ok')
        else:
            self.add_metadata()

    def run_serial(self):

        for handler in self._handlers:
            for _, handler_info in handler.items():

                handler_method = handler_info[0]
                handler_variables = handler_info[1]
                # find the input files this handler needs
                input_files = list()
                for variable in handler_variables:
                    input_file = self.find_variable_file(
                        var=variable,
                        path=self._input_path)
                    if input_file is None:
                        continue
                    var_path = os.path.join(
                        self._input_path,
                        input_file)
                    input_files.append(var_path)

                handler_method(
                    input_files,
                    self._tables_path,
                    self._user_input_path)

    def run_parallel(self):

        print_message(
            'running with {} processes'.format(self._nproc),
            'debug')

        # create process pool and results list
        self._pool = Pool(self._nproc)
        self._pool_res = list()

        for handler in self._handlers:
            for _, handler_info in handler.items():

                handler_method = handler_info[0]
                handler_variables = handler_info[1]
                # find the input files this handler needs
                input_files = list()
                for variable in handler_variables:
                    input_file = self.find_variable_file(
                        var=variable,
                        path=self._input_path)
                    if input_file is None:
                        continue
                    var_path = os.path.join(
                        self._input_path,
                        input_file)
                    input_files.append(var_path)
                kwds = {
                    'infiles': input_files,
                    'tables': self._tables_path,
                    'user_input_path': self._user_input_path
                }

                _args = (kwds['infiles'],
                         kwds['tables'],
                         kwds['user_input_path'])
                self._pool_res.append(
                    self._pool.apply_async(
                        handler_method,
                        args=_args,
                        kwds={}))

        for idx, res in enumerate(self._pool_res):
            try:
                out = res.get(9999999)
                msg = 'Finished {handler}, {done}/{total} jobs complete'.format(
                    handler=out,
                    done=idx + 1,
                    total=len(self._pool_res))
                logging.info(msg)
            except Exception as e:
                logging.error(e)

        self.terminate()

    def find_variable_file(self, var, path):
        """
        Looks in the path given for the first file that matches VAR_\d{6}_\d{6}.nc
        """
        contents = os.listdir(path)
        pattern = '{}'.format(var) + r'\_\d{6}\_\d{6}.nc'
        for item in contents:
            if re.match(pattern=pattern, string=item):
                return item
        return None

    def add_metadata(self):
        """
        Add additional custom metadata to the output files

        Parameters
        ----------
            metadata (dict): The keys are the names of the metadata fields, the values the values for the fields
        """
        cmip_path = os.path.join(self._output_path, 'CMIP6')

        filepaths = list()

        print_message('Adding additional metadata to output files', 'ok')
        for root, dirs, files in os.walk(cmip_path, topdown=False):
            for name in files:
                if name[-3:] != '.nc':
                    continue
                index = name.find('_')
                if index != -1 and name[:index] in self._var_list:
                    print_message(
                        "Adding additional metadata to {}".format(name), 'ok')
                    filepaths.append(os.path.join(root, name))

        for filepath in filepaths:
            datafile = cdms2.open(filepath, 'a')
            datafile.e3sm_source_code_doi = str('10.11578/E3SM/dc.20180418.36')
            datafile.e3sm_paper_reference = str('https://doi.org/10.1029/2018MS001603')
            datafile.e3sm_source_code_reference = str('https://github.com/E3SM-Project/E3SM/releases/tag/v1.0.0')
            datafile.doe_acknowledgement = str('This research was supported as part of the Energy Exascale Earth System Model (E3SM) project, funded by the U.S. Department of Energy, Office of Science, Office of Biological and Environmental Research.')
            datafile.computational_acknowledgement = str('The data were produced using resources of the National Energy Research Scientific Computing Center, a DOE Office of Science User Facility supported by the Office of Science of the U.S. Department of Energy under Contract No. DE-AC02-05CH11231.')
            datafile.ncclimo_generation_command = str("""ncclimo --var=${var} -7 --dfl_lvl=1 --no_cll_msr --no_frm_trm --no_stg_grd --yr_srt=1 --yr_end=500 --ypf=500 --map=map_ne30np4_to_cmip6_180x360_aave.20181001.nc """)
            datafile.ncclimo_version = str('4.7.9')
            datafile.close()

    def terminate(self):
        """
        Terminates the process pool
        """
        if self._debug:
            print_message('Shutting down process pool', 'debug')
        if self._pool:
            self._pool.close()
            self._pool.terminate()
            self._pool.join()
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
        '-i', '--input',
        metavar='',
        required=True,
        help='path to directory containing e3sm data with single variables per file')
    parser.add_argument(
        '-o', '--output',
        metavar='',
        help='where to store cmorized output')
    parser.add_argument(
        '-u', '--user-input',
        required=True,
        metavar='<user_input_json_path>',
        help='path to user input json file for CMIP6 metadata')
    parser.add_argument(
        '-n', '--num-proc',
        metavar='<nproc>',
        default=6,
        type=int,
        help='optional: number of processes, default = 6')
    parser.add_argument(
        '-t', '--tables',
        required=True,
        metavar='<tables-path>',
        help="Path to directory containing CMOR Tables directory")
    parser.add_argument(
        '-H', '--handlers',
        metavar='<handler_path>',
        default=None,
        help='path to cmor handlers directory, default = ./cmor_handlers')
    parser.add_argument(
        '-N', '--proc-vars',
        action='store_true',
        help='Set the number of process to the number of variables')
    parser.add_argument(
        '--version',
        help='print the version number and exit',
        action='version',
        version='%(prog)s 0.0.2')
    parser.add_argument(
        '--debug',
        help='Set output level to debug',
        action='store_true')
    parser.add_argument(
        '--no-metadata',
        help='Do not add E3SM metadata to the output',
        action='store_true')
    parser.add_argument(
        '--serial',
        help='Run in serial mode, usefull for debugging purposes',
        action="store_true")
    try:
        _args = sys.argv[1:]
    except:
        parser.print_help()
        sys.exit(1)
    else:
        _args = parser.parse_args(_args)
    return _args


def main():
    # parse the command line arguments
    _args = parse_argsuments()

    # run the cmorizer
    cmorizer = Cmorizer(
        var_list=_args.var_list,
        input_path=_args.input,
        user_input_path=_args.user_input,
        output_path=_args.output,
        num_proc=_args.num_proc,
        proc_vars=_args.proc_vars,
        handlers=_args.handlers,
        tables_path=_args.tables,
        debug=_args.debug,
        no_metadata=_args.no_metadata,
        serial=_args.serial)
    try:
        cmorizer.run()
    except KeyboardInterrupt as e:
        print('--- caught KeyboardInterrupt event ---')
        cmorizer.terminate()


if __name__ == "__main__":
    main()
