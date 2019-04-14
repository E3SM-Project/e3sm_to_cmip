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

from multiprocessing import cpu_count, Pool
from time import sleep

from e3sm_to_cmip import cmor_handlers
from e3sm_to_cmip.util import print_message
from e3sm_to_cmip.util import parse_argsuments
from e3sm_to_cmip.util import load_handlers
from e3sm_to_cmip.util import add_metadata
from e3sm_to_cmip.util import copy_user_metadata
from e3sm_to_cmip.util import print_debug
from e3sm_to_cmip.util import terminate

from e3sm_to_cmip.lib import run_parallel
from e3sm_to_cmip.lib import run_serial


import numpy as np
np.warnings.filterwarnings('ignore')


def main():

    # parse the command line arguments
    _args = parse_argsuments().__dict__

    var_list = [x.strip(',') for x in _args.get('var_list')]
    input_path = _args.get('input_path')
    output_path = _args.get('output_path')
    tables_path = _args.get('tables_path')
    user_metadata = _args.get('user_metadata')
    no_metadata = _args['no_metadata'] if _args.get('no_metadata') else False
    nproc = _args['num_proc'] if _args.get('num_proc') else 6
    serial = _args['serial'] if _args.get('serial') else False
    mode = _args['mode'] if _args.get('mode') else 'atm'
    debug = True if _args.get('debug') else False
    map_path = _args['map'] if _args.get('map') else None

    if _args.get('handlers'):
        handlers_path = os.path.abspath(_args.get('handlers'))
    else:
        handlers_path, _ = os.path.split(
            os.path.abspath(cmor_handlers.__file__))

    new_metadata_path = os.path.join(
        output_path,
        'user_metadata.json')

    # create the output dir if it doesnt exist
    if not os.path.exists(output_path):
        os.makedirs(output_path)

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
        level=logging.WARNING)

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
                logging=logging)
        except Exception as e:
            print_debug(e)
            return 1
            # status = 1
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
                logging=logging)
        except KeyboardInterrupt as error:
            print_message(' -- keyboard interrupt -- ', 'error')
            terminate(pool, debug)
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

    shutil.rmtree(temp_path)
    return 0
# ------------------------------------------------------------------


if __name__ == "__main__":
    sys.exit(main())
