import os
import sys
import yaml
import argparse

from time import sleep
from subprocess import Popen, PIPE
from pathlib import Path
from typing import List

DESC = '''test the output of the e3sm_to_cmip package from 
two git branches and run a comparison check on the output. Returns 0 if 
all checks run successfully, 1 otherwise. These checks
use the included CWL workflows to post-process the data and prepare it 
to  be ingested by e3sm_to_cmip.

At the moment it only tests atmospheric monthly variables, but more will be added in the future'''

def run_cmd(cmd: str):
    print(f"running: '{' '.join(cmd)}'")
    proc = Popen(cmd, stdout=PIPE, stderr=PIPE)
    output = []
    while proc.poll() is None:
        out = proc.stdout.read()
        if out:
            o = out.decode('utf-8')
            print(o)
            output.append(o)
        err = proc.stderr.read()
        if err:
            print(err.decode('utf-8'))
        sleep(0.1)
    return proc.returncode, ''.join(output)


def test(vars: List, comp_branch: str, input: Path, output: Path):
    if not input.exists():
        raise ValueError(f"Input directory {input} does not exist")
    output.mkdir(exist_ok=True)

    cmd = 'git status --porcelain'.split()
    retcode, output = run_cmd(cmd)
    if retcode:
        print('Error checking the git status')
        return 1
    if output and '??' in output or 'M' in output:
        print('git status is not clean, commit or stash your changes and try again')
        return 1
    
    # store what the source branch name is
    cmd = 'git branch --show-current'
    retcode, output = run_cmd(cmd)
    if retcode:
        print('Error getting the source git branch')
        return 1
    source_banch = output

    # check out the branch to compare this one against
    cmd = f'git checkout {comp_branch}'
    retcode, output = run_cmd(cmd)
    if retcode:
        print(f'Error checking out {comp_branch}')
        return 1
    
    # do the testing

    # swap back to the source branch
    cmd = f'git checkout {source_banch}'
    retcode, output = run_cmd(cmd)
    if retcode:
        print(f'Error checking out {source_banch}')
        return 1

    return 0


def main():
    parser = argparse.ArgumentParser(
        prog='e3sm_to_cmip',
        description=DESC)
    parser.add_argument(
        'input', 
        help='directory of raw files to use, these should be native grid raw model output')
    parser.add_argument(
        '-o', '--output', 
        default='testing',
        required=False,
        help=f'path to where the output files from the test should be stored, default is {os.environ.get("PWD")}{os.sep}testing{os.sep}')
    parser.add_argument(
        '--cleanup', 
        action='store_true', 
        help='remove the generated data if the test result is a success')
    parser.add_argument(
        '-v', '--var-list', 
        default='all',
        nargs="*",
        help='select which variables to include in the comparison, default is all')
    parser.add_argument(
        '-c', '--compare', 
        default='master', 
        help='select which branch to run the comparison against, default is master')
    parsed_args = parser.parse_args()

    try:
        retval = test(
            vars=parsed_args.var_list, 
            comp_branch=parsed_args.compare, 
            input=Path(parsed_args.input),
            output=Path(parsed_args.output))
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