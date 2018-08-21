 
import os, sys
import argparse
import cmor
import cdms2
import re
import logging

from multiprocessing import cpu_count, Pool
from importlib import import_module
from time import sleep
from lib.util import format_debug, print_message

class Cmorizer(object):
    """
    A utility class to cmorize e3sm time series files
    """
    def __init__(self, var_list, input_path, user_input_path, tables_path, **kwargs):
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
        self._user_input_path = user_input_path
        self._input_path = input_path
        self._tables_path = tables_path
        self._nproc = kwargs.get('nproc') if kwargs.get('nproc') else 6
        self._proc_vars = kwargs.get('proc_vars', False)
        self._output_path = os.path.abspath(kwargs.get('output_path')) if kwargs.get('output_path') else '.'
        self._handlers_path = kwargs.get('handlers') if kwargs.get('handlers') else './cmor_handlers'
        self._debug = kwargs.get('debug', False)
        self._pool = None
        self._pool_res = None

        logging.basicConfig(
            format='%(asctime)s:%(levelname)s: %(message)s',
            datefmt='%m/%d/%Y %I:%M:%S %p',
            filename=os.path.join(self._output_path, 'converter.log'),
            filemode='w',
            level=logging.DEBUG)

    def run(self):
        """
        run all the requested CMOR handlers
        """
        handlers = os.listdir(self._handlers_path)
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
            
            contents = os.listdir(self._input_path)
            found = False
            for infile in contents:
                if module in infile:
                    found = True
                    break
            if not found:
                continue

            module_path = '.'.join([self._handlers_path, module])
            # load the module, and extract the "handle" method
            try:
                mod = import_module(module_path)
                met = getattr(mod, 'handle')
            except Exception as e:
                msg = format_debug(e)
                print_message('Error loading handler for {}'.format(module_path))
                print_message(msg)
                logging.error(msg)
                continue
            else:
                msg = 'Loaded {}'.format(mod)
                if self._debug: print_message(msg, 'debug')
                logging.info(msg)
            self._handlers.append({module: met})
        
        # Setup the number of processes that will exist in the pool
        len_handlers = len(self._handlers)
        if self._proc_vars:
            ncpu = cpu_count()
            if len_handlers >= 100:
                self._nproc = 100 if ncpu > 100 else ncpu - 1
            else:
                self._nproc = len_handlers

        # only make as many processes as needed
        self._nproc = len_handlers if self._nproc > len_handlers else self._nproc
        if self._nproc == 0:
            msg = 'No handlers found'
            print_message(msg)
            logging.error(msg)
            sys.exit(1)

        if self._debug: print_message('running with {} processes'.format(self._nproc), 'debug')
        self._pool = Pool(self._nproc)
        self._pool_res = list()

        for handler in self._handlers:
            for key, val in handler.items():
                
                var_file = self.find_variable_file(key, self._input_path)
                if var_file is None:
                    continue
                var_path = os.path.join(
                    self._input_path,
                    var_file)
                kwds = {
                    'infile': var_path,
                    'tables': self._tables_path,
                    'user_input_path': self._user_input_path
                }

                _args = (kwds['infile'], kwds['tables'], kwds['user_input_path'])
                self._pool_res.append(
                    self._pool.apply_async(
                        val, args=_args, kwds={}))
        
        for idx, res in enumerate(self._pool_res):
            try:
                out = res.get(9999999)
                msg = 'Finished {}, {idx + 1}/{len(self._pool_res)} jobs complete'.format(out)
                print_message(msg, 'ok')
                logging.info(msg)
            except Exception as e:
                print format_debug(e)
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

    def terminate(self):
        if self._debug: print_message('Shutting down process pool', 'debug')
        if self._pool:
            self._pool.close()
            self._pool.terminate()
            self._pool.join()


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
        '-i', '--input',
        metavar='',
        required=True,
        help='path to directory containing e3sm data with single variables per file')
    parser.add_argument(
        '-o', '--output',
        metavar='',
        help='where to store cmorized outputoutput')
    parser.add_argument(
        '-u', '--user-input',
        required=True,
        metavar='<user_input_json_path>',
        help='path to user input json file for CMIP6 metadata')
    parser.add_argument(
        '-n', '--num-proc',
        metavar='<nproc>',
        default=6, type=int,
        help='optional: number of processes, default = 6')
    parser.add_argument(
        '-t', '--tables',
        required=True,
        metavar='<tables-path>',
        help="Path to directory containing CMOR Tables directory")
    parser.add_argument(
        '-H', '--handlers',
        metavar='<handler_path>',
        default='cmor_handlers',
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
        proc_vars=_args.proc_vars,
        handlers=_args.handlers,
        tables_path=_args.tables,
        debug=_args.debug)
    try:
        cmorizer.run()
    except KeyboardInterrupt as e:
        print '--- caught KeyboardInterrupt event ---'
        cmorizer.terminate()
    