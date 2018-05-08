# -*- coding: future_fstrings -*-
import os, sys
import argparse
import cdms2
import logging

from imp import reload
from random import uniform
from multiprocessing import cpu_count, Pool
from subprocess import Popen, PIPE
from time import sleep
from datetime import datetime

from lib.util import format_debug, print_message

class Splitter(object):
    """
    Extract regridded monthly means into single variable time series 
    """
    def __init__(self, var_list, caseid, input_path, output_path, start, end, nproc, proc_vars=False, data_type='clm2.h0', **kwargs):
        """
        Setup for extraction
        """
        self._prev_dir = os.getcwd()
        self._var_list = var_list
        self._caseid = caseid
        self._input_path = input_path
        self._output_path = os.path.abspath(output_path)
        self._start = start
        self._end = end
        self._debug = kwargs.get('debug')
        self._pool = None
        self._file_list = None
        os.chdir(os.path.abspath(input_path))

        # reload(logging)
        logging.basicConfig(
            format='%(asctime)s:%(levelname)s: %(message)s',
            datefmt='%m/%d/%Y %I:%M:%S %p',
            filename=os.path.join(self._output_path, 'splitter.log'),
            filemode='w',
            level=logging.DEBUG)

        msg = 'setting up splitter'
        logging.info(msg)
        if self._debug: print msg
        msg = f'''var_list: {self._var_list}
        caseid: {caseid}
        input_path: {input_path}
        output_path: {self._output_path}
        start-year: {start}
        end-year: {end}
        nproc: {nproc}'''
        logging.info(msg)
        if self._debug: print_message(msg, status='debug')

        # First get the list of self._file_list in our year range that match the caseid
        contents = os.listdir(os.getcwd())
        self._file_list = list()
        start_pattern = f'{caseid}.{data_type}.'
        start_pattern_len = len(start_pattern)
        end_pattern = '.nc'
        end_pattern_len = 3
        for item in contents:
            index = item.find(start_pattern)
            if index is None:
                continue
            end_index = item.find(end_pattern)
            if end_index is None:
                continue
            try:
                year = int(item[index + start_pattern_len: end_index - end_pattern_len])
            except:
                continue
            if year >= start and year <= end:
                self._file_list.append(item)
        file_len = len(self._file_list)
        msg = f'found {file_len} input files'
        logging.info(msg)
        if self._debug: print_message(msg, status='debug')

        # Second if the var_list is set to 'all' open up one of the input self._file_list
        # and grab a list of all variables
        if var_list[0] == 'all':
            msg = 'splitting all variables'
            logging.info(msg)
            print_message(msg, status='ok')
            var_list = list()
            f = cdms2.open(self._file_list[0])
            for key, _ in f.variables.items():
                if key[0].isupper():
                    var_list.append(key)

            var_len = len(var_list)
            msg = f'found {var_len} variables to extract'
            logging.info(msg)
            print_message(msg, status='ok')
        else:
            var_list_tmp = list()
            f = cdms2.open(self._file_list[0])
            file_vars = f.variables.keys()
            for var in var_list:
                if var in file_vars:
                    var_list_tmp.append(var)
                else:
                    if self._debug:
                        msg = f'{var} not present'
                        print_message(msg, 'error')
            var_list = var_list_tmp
            msg = f'splitting {" ".join(var_list)}'
            logging.info(msg)
            print_message(msg, status='ok')


        # Setup the number of processes that will exist in the pool
        if proc_vars:
            len_vars = len(var_list)
            ncpu = cpu_count()
            if len_vars >= 100:
                nproc = 100 if ncpu > 100 else ncpu - 1
            else:
                nproc = len_vars

        self._nproc = len(var_list) if nproc > len(var_list) else nproc
        if self._nproc == 0:
            msg = 'No variables found'
            print_message(msg)
            logging.error(msg)
            sys.exit(1)
    
    def split(self):
        """
        Perform the requested variable extraction
        """
        msg = f'starting extraction with nproc = {self._nproc}'
        logging.info(msg)
        print_message(msg, status='ok')
        pool_res = list()
        self._pool = Pool(self._nproc)
        for var in var_list:
            outfile = os.path.join(
                self._output_path,
                f'{var}_{self._start:04d}01_{self._end:04d}12.nc')
            pool_res.append(
                self._pool.apply_async(
                    _split_one, [var, self._file_list, outfile]))

        out = None
        err = None
        for idx, res in enumerate(pool_res):
            out, err = res.get(9999999)
            if err:
                print out, err
            else:
                out += f', {idx + 1}/{len(pool_res)} jobs complete'
                print_message(out, 'ok')
        self._pool.close()
        self._pool.join()
        os.chdir(self._prev_dir)

    
    def terminate(self):
        self._pool.close()
        self._pool.terminate()
        self._pool.join()

def _split_one(var, file_list, outfile):
    """
    Split a single variable from a list of self._file_list into the outfile

    Parameters:
        var (str): the name of the variable to extract
        inself._file_list (list): a list of strings that are the paths to history self._file_list to extract from
        outfile (str): a path to where the output file should be stored
    Returns:
        out (str): the stdout output returned from ncrcat
        err (str): the stderr output from ncrcat
    """
    file_list = sorted(file_list)
    start_time = datetime.now()
    # sleep to avoid printing errors
    # sleep(uniform(0.01, 0.1))
    # print_message(f'Starting {var}', 'ok')
    cmd = ['ncrcat', '-O', '-cv', var] + file_list + [outfile]
    msg = f'starting {var}'
    logging.info(msg)
    while True:
        try:
            proc = Popen(cmd, stderr=PIPE, stdout=PIPE)
            try:
                out, err = proc.communicate()
                if err:
                    return out, err
            except KeyboardInterrupt:
                sleep(uniform(0.01, 0.1))
                msg = f'  - killing {var}'
                proc.terminate()
                print_message(msg)
                return None, None
        except Exception as e:
            msg = format_debug(e)
            logging.error(e)
            msg = 'cant start process, retrying'
            print_message(msg)
            sleep(uniform(0.01, 0.1))
        else:
            break
    end_time = datetime.now()
    tdelta = end_time - start_time
    msg = f'finished {var} in {tdelta.seconds}.{tdelta.microseconds/1000:02d} seconds'
    logging.info(msg)
    return msg, None

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Single variable time series extraction for ESM data',
        prog='singlevar_ts',
        usage='%(prog)s [-h]')
    parser.add_argument(
        '-v', '--var-list',
        nargs='+',
        metavar='',
        help='Space sepperated list of variables, use \'all\' to extract all variables')
    parser.add_argument(
        '-c', '--case-id',
        metavar='<case_id>',
        help='Name of case, e.g. 20180129.DECKv1b_piControl.ne30_oEC.edison',
        required=True)
    parser.add_argument(
        '-i', '--input-path',
        metavar='',
        help='Path to input directory',
        required=True)
    parser.add_argument(
        '-o', '--output-path',
        metavar='',
        help='Path to output directory',
        required=True)
    parser.add_argument(
        '-s', '--start-year',
        metavar='',
        help='First year to extract',
        type=int,
        required=True)
    parser.add_argument(
        '-e', '--end-year',
        metavar='',
        help='Last year to split',
        type=int,
        required=True)
    parser.add_argument(
        '-n', '--num-proc',
        metavar='',
        help='Number of parallel processes, default = 6',
        type=int, default=6)
    parser.add_argument(
        '-d', '--data-type',
        metavar='',
        default='cam.h0',
        help='The type of data to extract from, e.g. clm2.h0 or cam.h0. Defaults to cam.h0')
    parser.add_argument(
        '-N', '--proc-vars',
        # metavar=' ',
        action='store_true',
        help='Set the number of process to the number of variables')
    parser.add_argument(
        '--version',
        help='print the version number and exit',
        action='version',
        version='%(prog)s 0.0.1')
    parser.add_argument(
        '--debug',
        help='Set output level to debug',
        action='store_true')
    try:
        args = sys.argv[1:]
    except:
        parser.print_help()
        sys.exit(1)
    else:
        args = parser.parse_args(args)
    
    if args.var_list:
        var_list = args.var_list
    else:
        if args.data_type == 'clm2.h0':
            var_list = ['SOILWATER_10CM', 'SOILICE', 'SOILLIQ', 'QOVER', 
                        'QRUNOFF', 'QINTR', 'QVEGE', 'QSOIL', 'QVEGT', 'TSOI',
                        'LAISHA', 'LAISUN', 'NBP']
        elif args.data_type == 'cam.h0':
            var_list = ['TREFHT', 'TS', 'TSMN', 'TSMX', 'PSL', 
                        'PS', 'U10', 'RHREFHT', 'QREFHT', 'PRECSC', 
                        'PRECL', 'PRECC', 'QFLX', 'TAUX', 'TAUY',
                        'LHFLX', 'PRECSL', 'FSDS', 'FSNS', 'FLNS',
                        'FLDS', 'SHFLX']
    
    debug = True if args.debug else False
    try:
        splitter = Splitter(
            var_list=var_list,
            input_path=args.input_path,
            caseid=args.case_id,
            output_path=args.output_path,
            start=args.start_year,
            end=args.end_year,
            nproc=args.num_proc,
            proc_vars=args.proc_vars,
            data_type=args.data_type,
            debug=debug)
        splitter.split()
    except KeyboardInterrupt as e:
        msg = 'Caught keyboard interrupt, killing processes'
        print_message(msg)
        splitter.terminate()
