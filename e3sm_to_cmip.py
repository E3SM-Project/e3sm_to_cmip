# -*- coding: future_fstrings -*-
import os, sys
import argparse
import cmor
import cdms2
import re
import logging

from multiprocessing import Pool
from threading import Event
from importlib import import_module
from time import sleep
from lib.util import format_debug, print_message

class Cmorizer(object):
    """
    A utility class to cmorize clm time series files
    """
    def __init__(self, var_list, input_path, user_input_path, tables_path, output_path='./', nproc=6, handlers='./cmor_handlers'):
        self._var_list = var_list
        self._user_input_path = user_input_path
        self._input_path = input_path
        self._output_path = output_path
        self._handlers_path = handlers
        self._tables_path = tables_path
        self._event = Event()
        self._pool = None
        self._pool_res = None
    
        if len(var_list) < nproc and var_list[0] != 'all':
            self._nproc = len(var_list)
        else:
            self._nproc = nproc

    def run(self):
        """
        run all the requested CMOR handlers
        """
        handlers = os.listdir(
            os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                self._handlers_path))
        self._handlers = list()
        for handler in handlers:
            if not handler.endswith('.py'):
                continue
            if handler == "__init__.py":
                continue

            module, _ = handler.rsplit('.', 1)
            # ignore handlers for variables that werent requested
            if module not in self._var_list and self._var_list[0] != 'all':
                continue
            module_path = '.'.join([self._handlers_path, module])
            # load the module, and extract the "handle" method
            try:
                mod = import_module(module_path)
                met = getattr(mod, 'handle')
            except Exception as e:
                msg = format_debug(e)
                print_message(f'Error loading handler for {module_path}')
                print_message(msg)
                continue
            self._handlers.append({module: met})

        print_message(f'--- running with {self._nproc} processes ---', 'ok')
        self._pool = Pool(self._nproc)
        self._pool_res = list()

        for handler in self._handlers:
            for key, val in handler.items():
                
                var_file = self.find_variable_file(key, self._input_path)
                if var_file is None:
                    msg = f'Unable to find {key} in {self._input_path}'
                    logging.error(msg)
                    print_message(msg, 'error')
                    continue
                var_path = os.path.join(
                    self._input_path,
                    var_file)
                kwds = {
                    'infile': var_path,
                    'tables': self._tables_path,
                    'user_input_path': self._user_input_path
                }
                msg = f'Starting {key} with {var_path}'
                logging.info(msg)
                print_message(msg, 'ok')
                _args = (kwds['infile'], kwds['tables'], kwds['user_input_path'])
                self._pool_res.append(
                    self._pool.apply_async(
                        val, args=_args, kwds={}))
        
        for res in self._pool_res:
            try:
                msg = res.get()
                print_message(msg, 'ok')
                logging.info(msg)
            except Exception as e:
                print format_debug(e)
        self.terminate()
    
    def find_variable_file(self, var, path):
        """
        Looks in the path given for the first file that matches VAR_\d{6}_\d{6}.nc
        """
        contents = os.listdir(path)
        pattern = f'{var}' + r'\_\d{6}\_\d{6}.nc'
        for item in contents:
            if re.match(pattern=pattern, string=item):
                return item
        return None

    def terminate(self):
        if self._pool:
            self._pool.close()
            self._pool.terminate()
            self._poo.join()


if __name__ == "__main__":
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
        '-u', '--user-input',
        required=True,
        metavar='<user_input_json_path>',
        help='path to user input json file for CMIP6 metadata')
    parser.add_argument(
        '-i', '--input',
        metavar='',
        required=True,
        help='path to directory containing e3sm data with single variables per file')
    parser.add_argument(
        '-o', '--output',
        metavar='',
        help='where to store cmorized outputoutput')
    parser.add_argument(
        '-n', '--num-proc',
        metavar='<nproc>',
        default=6, type=int,
        help='optional: number of processes, default = 6')
    parser.add_argument(
        '-H', '--handlers',
        metavar='<handler_path>',
        default='cmor_handlers',
        help='path to cmor handlers directory, default = ./cmor_handlers')
    parser.add_argument(
        '--version',
        help='print the version number and exit',
        action='version',
        version='%(prog)s 0.1')
    parser.add_argument(
        '-t', '--tables',
        required=True,
        metavar='<tables-path>',
        help="Path to directory containing CMOR Tables directory")
    try:
        _args = sys.argv[1:]
    except:
        parser.print_help()
        sys.exit(1)
    else:
        _args = parser.parse_args(_args)
    
    cmorizer = Cmorizer(
        var_list=_args.var_list,
        input_path=_args.input,
        user_input_path=_args.user_input,
        output_path=_args.output,
        nproc=_args.num_proc,
        handlers=_args.handlers,
        tables_path=_args.tables)
    try:
        cmorizer.run()
    except KeyboardInterrupt as e:
        print '--- caught keyboard kill event ---'
        cmorizer.terminate()
    