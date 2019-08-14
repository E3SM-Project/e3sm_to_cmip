#!/usr/bin/env python
"""
A python command line tool to turn E3SM model output into CMIP6 compatable data
"""

from __future__ import absolute_import, division, print_function, unicode_literals

import os
import sys
import logging
import tempfile
import shutil
import threading
from pathos.multiprocessing import ProcessPool as Pool

from e3sm_to_cmip import cmor_handlers
from e3sm_to_cmip.util import print_message
from e3sm_to_cmip.util import parse_argsuments
from e3sm_to_cmip.util import load_handlers
from e3sm_to_cmip.util import add_metadata
from e3sm_to_cmip.util import copy_user_metadata
from e3sm_to_cmip.util import print_debug

from e3sm_to_cmip.lib import run_parallel
from e3sm_to_cmip.lib import run_serial


import numpy as np
np.warnings.filterwarnings('ignore')

__version__ = "1.2.1"

def timeout_exit():
    print_message("Hit timeout limit, exiting")
    sys.exit(-1)


def main():

    # parse the command line arguments
    _args = parse_argsuments(version=__version__).__dict__

    if len(_args.get('var_list')) == 1 and " " in _args.get('var_list')[0]:
        var_list = _args.get('var_list')[0].split()
    else:
        var_list = _args.get('var_list')
    var_list = [x.strip(',') for x in var_list]
    input_path = _args.get('input_path')
    output_path = _args.get('output_path')
    tables_path = _args.get('tables_path')
    user_metadata = _args.get('user_metadata')
    no_metadata = _args['no_metadata'] if _args.get('no_metadata') else False
    only_metadata = _args['only_metadata'] if _args.get('only_metadata') else False
    nproc = _args['num_proc'] if _args.get('num_proc') else 6
    serial = _args['serial'] if _args.get('serial') else False
    mode = _args['mode'] if _args.get('mode') else 'atm'
    debug = True if _args.get('debug') else False
    map_path = _args['map'] if _args.get('map') else None
    cmor_log_dir = _args['logdir'] if _args.get('logdir') else None
    timeout = int(_args['timeout']) if _args.get('timeout') else None

    if timeout:
        timer = threading.Timer(timeout, timeout_exit)
        timer.start()

    if _args.get('handlers'):
        handlers_path = os.path.abspath(_args.get('handlers'))
    else:
        handlers_path, _ = os.path.split(
            os.path.abspath(cmor_handlers.__file__))
    
    # add additional optional metadata to the output files
    if only_metadata:
        print_message('Updating file metadata and exiting', 'ok')
        add_metadata(
            file_path=output_path,
            var_list=var_list)
        return 0

    new_metadata_path = os.path.join(
        output_path,
        'user_metadata.json')

    # create the output dir if it doesnt exist
    if not os.path.exists(output_path):
        os.makedirs(output_path)

    # setup temp storage directory
    temp_path = os.environ.get('TMPDIR')
    if temp_path is None:
     
        temp_path = '{}/tmp'.format(output_path)
        if not os.path.exists(temp_path):
            os.makedirs(temp_path)

    tempfile.tempdir = temp_path

    logging_path = os.path.join(output_path, 'converter.log')
    print_message("Writing log output to: {}".format(logging_path), 'debug')

    # setup logging
    logging.basicConfig(
        format='%(asctime)s:%(levelname)s: %(message)s',
        datefmt='%m/%d/%Y %I:%M:%S %p',
        filename=logging_path,
        filemode='w',
        level=logging.INFO)

    # copy the users metadata json file with the updated output directory
    copy_user_metadata(
        user_metadata, output_path)

    # load variable handlers
    handlers = load_handlers(
        handlers_path,
        var_list,
        debug)
    if len(handlers) == 0:
        print_message('No handlers loaded')
        sys.exit(1)

    # run in the user-selected mode
    if serial:
        print_message('Running CMOR handlers in serial', 'ok')
        try:
            status = run_serial(
                handlers=handlers,
                input_path=input_path,
                tables_path=tables_path,
                metadata_path=new_metadata_path,
                map_path=map_path,
                mode=mode,
                logdir=cmor_log_dir)
        except Exception as e:
            print_debug(e)
            return 1
    else:
        print_message('Running CMOR handlers in parallel', 'ok')
        try:
            pool = Pool(nproc)
            status = run_parallel(
                pool=pool,
                handlers=handlers,
                input_path=input_path,
                tables_path=tables_path,
                metadata_path=new_metadata_path,
                map_path=map_path,
                mode=mode,
                logdir=cmor_log_dir)
        except KeyboardInterrupt as error:
            print_message(' -- keyboard interrupt -- ', 'error')
            return 1
        except Exception as error:
            print_debug(error)
            return 1
    if status != 0:
        print_message("Error running handlers")
        return 1

    # add additional optional metadata to the output files
    if no_metadata:
        print_message('Not adding additional metadata', 'ok')
    else:
        add_metadata(
            file_path=output_path,
            var_list=var_list)

    if not _args['no_rm_tmpdir']:
        shutil.rmtree(temp_path)

    return 0
# ------------------------------------------------------------------


if __name__ == "__main__":
    sys.exit(main())
