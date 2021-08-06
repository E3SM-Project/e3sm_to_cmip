import ipdb


import yaml
import os
import sys
from shutil import rmtree
import argparse

from time import sleep
from subprocess import Popen, PIPE
from pathlib import Path
from typing import List

from e3sm_to_cmip import resources

DESC = '''test the output of the e3sm_to_cmip package from 
two git branches and run a comparison check on the output. Returns 0 if 
all checks run successfully, 1 otherwise. These checks
use the included CWL workflows to post-process the data and prepare it 
to  be ingested by e3sm_to_cmip.

At the moment it only tests atmospheric monthly variables, but more will be added in the future'''


def run_cmd(cmd: str, shell=False, show_output=True):
    print(f"running: '{cmd}'")
    if not shell:
        proc = Popen(cmd.split(), stdout=PIPE, stderr=PIPE)
    else:
        proc = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
    output = []
    while proc.poll() is None:
        out = proc.stdout.read()
        if out:
            o = out.decode('utf-8')
            if show_output:
                print(o)
            output.append(o)
        err = proc.stderr.read()
        if err:
            print(err.decode('utf-8'))
        sleep(0.1)
    return proc.returncode, ''.join(output)

def swap_branch(target_branch):
                
    cmd = 'git status --porcelain'
    retcode, output = run_cmd(cmd)
    if retcode:
        print('Error checking the git status')
        return 1
    if output:
        if '??' in output or 'M' in output or 'nothing to commit, working tree clean' not in output:
            print('git status is not clean, commit or stash your changes and try again')
            return 1
    del output
    
    # check out the branch to compare this one against
    cmd = f'git checkout {target_branch}'
    retcode, output = run_cmd(cmd)
    if retcode:
        print(f'Error checking out {target_branch}')
        return 1
    del output

def install_and_run_test(branchname, vars, frequency, input_path, output_path, tables_path):

    test_output_path = Path(output_path, branchname)
    if test_output_path.exists():
        print("removing previous testing source output")
        rmtree(str(test_output_path))

    print(f"Creating output directory {test_output_path}")
    test_output_path.mkdir(parents=True)

    # swap over to the comparison branch
    # ipdb.set_trace()
    if retcode := swap_branch(branchname):
        print("Unable to swap to branch {branchname}, exiting")
        return retcode

    # install the comparison version of the package so CWL uses the right version
    cmd = "find . -name '*.pyc' -delete; python setup.py install"
    retcode, output = run_cmd(cmd, shell=True, show_output=False)
    if retcode:
        print(f'Error installing from comparison branch {branchname}')
        return 1
    del output

    if retcode := run_test(vars, frequency, input_path, test_output_path, tables_path):
        print(f"Error running {branchname} branch test")
        return retcode

    return 0

def run_test(vars, freq, input_path, out_path, tables_path):
    # ipdb.set_trace()
    resource_path, _ = os.path.split(os.path.abspath(resources.__file__))
    default_metadata_path = Path(resource_path, 'default_metadata.json')
    if  not  default_metadata_path.exists():
        print(f"Error: cannot file default CMOR metadata file: {default_metadata_path}")
        return 1
    
    # actually run the package and make some output
    cmd = f"e3sm_to_cmip --serial -v {', '.join(vars)} -i {input_path} -o {out_path} -t {tables_path} -u {default_metadata_path} -f {freq}"
    retcode, output = run_cmd(cmd)
    if output:
        print(output)
    return retcode

def get_input_vars(output_path, input_path, vars, tables_path, freq):
    info_path = Path(output_path, 'var_info.yaml')
    cmd = f"e3sm_to_cmip --info -v {vars} -t {tables_path} -f {freq} -i {input_path} --info-out {info_path}"
    retcode, output = run_cmd(cmd, show_output=False)
    if retcode:
        print("Error getting available timeseries input files")
        return retcode
    with open(info_path, 'r') as infile:
        info = yaml.load(infile, Loader=yaml.SafeLoader)
    
    vars = []
 
def compare_output(output_path, src_name, cmp_name):
    # i like to have the heavy imports happen after the command line arguments get parsed
    # so that startup is faster when running --help
    import xarray as xr
    import numpy as np
    # compare the output between the two runs
    issues = []
    ipdb.set_trace()
    tables = Path(output_path, cmp_name, 'CMIP6/CMIP/E3SM-Project/E3SM-1-0/piControl/r1i1p1f1/').glob("**")
    for table in tables:
        src_table_path = Path(output_path, src_name, 'CMIP6/CMIP/E3SM-Project/E3SM-1-0/piControl/r1i1p1f1/', table.name)
        for variable in table.glob('**'):
            # check that the variable exists in both sets of output
            source_var_path = src_table_path / variable.name
            if not source_var_path.exists():
                msg = f"{variable} exists in the comparison branch {cmp_name}, but not in the source branch {src_name}"
                issues.append(msg)
                continue

            # get the paths to the highest number dataset for the source and comparison datasets
            try:
                cmp_var_data_path = sorted([x for x in Path(table, variable.name, 'gr').glob('v*')])[-1]
            except IndexError:
                msg = f"Empty variable list in comparison branch {cmp_name}"
                issues.append(msg)
                continue
            try:
                src_var_data_path = sorted([x for x in Path(src_table_path, variable.name, 'gr').glob('v*')])[-1]
            except IndexError:
                msg = f"Empty variable list in source branch {src_name}"
                issues.append(msg)
                continue
                
            print(f"Running comparison for {variable}", end=" ... ")

            # each dataset should only have a single file in it
            with xr.open_dataset(cmp_var_data_path.glob('*.nc').__next__()) as cmp_ds, \
                xr.open_dataset(src_var_data_path.glob('*.nc').__next__()) as src_ds:
                
                if np.allclose(cmp_ds[str(variable)], src_ds[str(variable)]):
                    print(f"{variable} test pass")
                else:
                    msg = f"{variable}: values do not match"
                    issues.append(msg)
                    
    return issues

def test(vars: List, cmp_branch: str, input_path: Path, output_path: Path, tables: str, freq: str, cleanup=False):
    if not input_path.exists():
        raise ValueError(f"Input directory {input_path} does not exist")
    output_path.mkdir(exist_ok=True)
    
    # store what the source branch name is
    cmd = "git branch --show-current"
    retcode, output = run_cmd(cmd)
    if retcode:
        print('Error getting the source git branch')
        return 1
    src_branch = output.strip()

    if ret := install_and_run_test(cmp_branch, vars, freq, input_path=input_path, output_path=output_path, tables_path=tables):
        print(f"Failure to run {cmp_branch}")
        return ret
        
    if ret := install_and_run_test(src_branch, vars, freq, input_path=input_path, output_path=output_path, tables_path=tables):
        print(f"Failure to run {src_branch}")
        return ret

    if issues := compare_output(output_path, src_branch, cmp_branch):
        for issue in issues:
            print(issue)
        return 1
    else:
        if cleanup:
            cleanup()
    return 0



def main():
    parser = argparse.ArgumentParser(
        prog='e3sm_to_cmip',
        description=DESC)
    parser.add_argument(
        'input',
        help='directory of timeseries netcdf files to as input to the e3sm_to_cmip package. these should already be regridded/remapped as needed')
    parser.add_argument(
        'tables',
        help='Path to the Tables directory in the cmip6-cmor-tables repository')

    default_output_path = Path(os.environ['PWD'], 'testing_output')
    parser.add_argument(
        '-o', '--output',
        default=default_output_path,
        required=False,
        help=f'path to where the output files from the test should be stored, default is {default_output_path}')
    parser.add_argument(
        '-v', '--var-list',
        default=['all'],
        nargs="*",
        help='select which variables to include in the comparison, default is all')
    parser.add_argument(
        '-f', '--frequency',
        default='mon',
        help='temporal frequency of the input timeseries files, default is mon')
    parser.add_argument(
        '-c', '--compare',
        default='master',
        help='select which branch to run the comparison against, default is master')
    parser.add_argument(
        '--cleanup',
        action='store_true',
        help='remove the generated data if the test result is a success')
    parsed_args = parser.parse_args()

    try:
        retval = test(
            input_path=Path(parsed_args.input),
            tables=parsed_args.tables,
            vars=parsed_args.var_list,
            cmp_branch=parsed_args.compare,
            output_path=Path(parsed_args.output),
            cleanup=parsed_args.cleanup,
            freq=parsed_args.frequency)
    except Exception as e:
        print(e)
        retval = 1

    # test success actions
    if retval == 0:
        if parsed_args.cleanup:
            os.rmdir(parsed_args.output)
        print('Testing successful')
    else:
        print('Testing error')
    return retval


if __name__ == "__main__":
    sys.exit(main())
