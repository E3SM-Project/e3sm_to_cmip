import os, sys
import argparse
import cmor
import cdms2

from multiprocessing import Pool
from threading import Event
from importlib import import_module

from lib.util import format_debug

class Cmorizer(object):
    """
    A utility class to cmorize clm time series files
    """
    def __init__(self, var_list, input_path, output_path, caseid, user_input_path, nproc=6, handlers='./cmor_handlers'):
        self._var_list = var_list
        self._user_input_path = user_input_path
        self._input_path = input_path
        self._output_path = output_path
        self._nproc = nproc
        self._handlers_path = handlers
        self._caseid = caseid
        self._event = Event()
        self._pool = None
        self._pool_res = None
    
    def run(self):
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
            if module not in self._var_list and self._var_list[0] != 'all':
                continue
            module_path = '.'.join([self._handlers_path, module])
            mod = import_module(module_path)
            met = getattr(mod, 'handle')
            self._handlers.append({module: met})

        print '--- running with {} processes ---'.format(self._nproc)
        self._pool = Pool(self._nproc)
        self._pool_res = list()

        for handler in self._handlers:
            for key, val in handler.items():
                kwds = {
                    'infile': os.path.join(
                                self._input_path, 
                                self._caseid + '.' + key + '.nc'),
                    'tables_dir': self._handlers_path,
                    'user_input_path': self._user_input_path
                }
                if not os.path.exists(kwds['infile']):
                    print 'File not found: {}'.format(kwds['infile'])
                    continue
                if not os.path.exists(kwds['user_input_path']):
                    print 'User input file not found'
                    continue
                print 'Starting {}'.format(key)
                self._pool_res.append(
                    self._pool.apply_async(val, args=(), kwds=kwds))
        
        for res in self._pool_res:
            try:
                res.get()
            except Exception as e:
                print format_debug(e)
        self._pool.close()
        self._pool.join()
    
    def terminate(self):
        if self._pool:
            self._pool.terminate()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Convert ESM model output into CMIP compatible format',
        prog='e3sm_to_cmip',
        usage='%(prog)s [-h]'
    )
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
        '-c', '--caseid',
        metavar='<case_id>',
        required=True,
        help='name of case e.g. 20180129.DECKv1b_piControl.ne30_oEC.edison')
    parser.add_argument(
        '-i', '--input',
        metavar='',
        required=True,
        help='path to directory containing e3sm data with single variables per file')
    parser.add_argument(
        '-o', '--output',
        metavar='',
        required=True,
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
    try:
        args = sys.argv[1:]
    except:
        parser.print_help()
        sys.exit(1)
    else:
        args = parser.parse_args(args)
    
    cmorizer = Cmorizer(
        var_list=args.var_list,
        input_path=args.input,
        user_input_path=args.user_input,
        output_path=args.output,
        caseid=args.caseid,
        nproc=args.num_proc,
        handlers=args.handlers)
    try:
        cmorizer.run()
    except KeyboardInterrupt as e:
        print '--- caught keyboard kill event ---'
        cmorizer.terminate()
    