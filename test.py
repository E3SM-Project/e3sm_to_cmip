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

def run_cmd(cmd: str, shell=False):
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
            print(o)
            output.append(o)
        err = proc.stderr.read()
        if err:
            print(err.decode('utf-8'))
        sleep(0.1)
    return proc.returncode, ''.join(output)


def test(vars: List, cmp_branch: str, input: Path, output_path: Path, cwl_path: Path, map_path: str, vrt_map: str, metadata: str):
    # i like to have the heavy imports happen after the command line arguments get parsed
    # so that startup is faster when running --help
    import xarray as xr
    import numpy as np
    if not input.exists():
        raise ValueError(f"Input directory {input} does not exist")
    output_path.mkdir(exist_ok=True)

    if os.environ.get('TMPDIR'):
        tmp_dir = f" --tmpdir-prefix={os.environ.get('TMPDIR')}"
    else:
        tmp_dir = ""

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
    
    # store what the source branch name is
    cmd = "git branch --show-current"
    retcode, output = run_cmd(cmd)
    if retcode:
        print('Error getting the source git branch')
        return 1
    src_branch = output
    del output

    # check out the branch to compare this one against
    cmd = f'git checkout {cmp_branch}'
    retcode, output = run_cmd(cmd)
    if retcode:
        print(f'Error checking out {cmp_branch}')
        return 1
    del output
    
    # install the comparison version of the package so CWL uses the right version
    cmd = "find . -name '*.pyc' -delete; python setup.py install"
    retcode, output = run_cmd(cmd, shell=True)
    if retcode:
        print(f'Error installing from comparison branch {cmp_branch}')
        return 1
    del output


    # render out the cwl parameter file with the required info to generate
    # CMIP variables from the raw input
    workflow_path  = Path(cwl_path, 'atm-unified', 'atm-unified.cwl')
    parameter_template_path = Path(cwl_path, 'atm-unified', 'atm-unified-job.yaml')

    with open(parameter_template_path, 'r') as instream:
        parameters = yaml.load(instream, Loader=yaml.SafeLoader)
    
    parameters['data_path'] = str(input)
    parameters['frequency'] = 1
    parameters['timeout'] = '0:30:00'
    parameters['hrz_atm_map_path'] = map_path
    parameters['vrt_map_path'] = vrt_map
    parameters['metadata_path'] = metadata

    cwl_parameter_path = Path(output_path, f'atm-mon-{cmp_branch}-vs-{src_branch}.yaml')
    if cwl_parameter_path.exists():
        cwl_parameter_path.unlink()
    
    print(f"Writing out CWL parameter file {cwl_parameter_path}")
    with open(cwl_parameter_path, 'w') as outstream:
        yaml.dump(parameters, outstream)

    cmp_output_path = Path(output_path, cmp_branch, 'CMIP6')
    if cmp_output_path.exists():
        print("removing previous testing comparison output")
        cmp_output_path.rmdir()

    # execute the workflow and collect output for comparison
    cmd = f"cwltool --outdir {output_path / cmp_branch}{tmp_dir} --preserve-environment UDUNITS2_XML_PATH {workflow_path} {cwl_parameter_path}"
    retcode, output = run_cmd(cmd)
    del output

    if retcode != 0:
        print("Error running comparison branch")
        return retcode
    else:
        print("Completed running the comparison branch")
    
    # swap back to the source branch
    cmd = f'git checkout {src_branch}'
    retcode, output = run_cmd(cmd)
    if retcode:
        print(f'Error checking out {src_branch}')
        return 1
    del output
    
    # install the test version of the package and run the data again
    cmd = "find . -name '*.pyc' -delete; python setup.py install"
    retcode, output = run_cmd(cmd, shell=True)
    if retcode:
        print(f'Error installing from comparison branch {src_branch}')
        return 1
    
    src_output_path = Path(output_path, src_branch, 'CMIP6')
    if src_output_path.exists():
        print("removing previous testing source output")
        cmp_output_path.rmdir()

    cmd = f"cwltool --outdir {output_path / src_branch}{tmp_dir} --preserve-environment UDUNITS2_XML_PATH {workflow_path} {cwl_parameter_path}"
    retcode, output = run_cmd(cmd)
    del output
    if retcode != 0:
        print("Error running source branch")
        return retcode
    else:
        print("Completed running the source branch")

    # compare the output between the two runs
    issues = []
    for table in ['Amon', 'CFmon', 'AERmon']:
        cmp_branch_path = Path(cmp_output_path, 'CMIP6/CMIP/E3SM-Project/E3SM-1-0/piControl/r1i1p1f1/', table)
        src_branch_path = Path(src_output_path, 'CMIP6/CMIP/E3SM-Project/E3SM-1-0/piControl/r1i1p1f1/', table)
        for variable in cmp_branch_path.glob('**'):
            # check that the variable exists in both sets of output
            source_var_path = src_branch_path / variable
            if not source_var_path.exists():
                msg = f"{variable} exists in the comparison branch {cmp_branch}, but not in the source branch {src_branch}"
                issues.append(msg)
                continue

            # get the paths to the highest number dataset for the source and comparison datasets
            src_var_path = sorted([x for x in Path(src_branch_path, variable, 'gr').glob('**')]).pop() 
            cmp_var_path = sorted([x for x in Path(cmp_branch_path, variable, 'gr').glob('v*')]).pop()

            # each dataset should only have a single file in it or this will break
            with xr.open_dataset(cmp_var_path.glob('*').__next__()) as cmp_ds, \
                xr.open_dataset(src_var_path.glob('*').__next__()) as src_ds:
                
                if not np.allclose(cmp_ds[str(variable)], src_ds[str(variable)]):
                    msg = f"{variable}: values do not match"
                    issues.append(msg)
    if not issues:
        return 0
    for issue in issues:
        print(issue)
    return 1


def main():
    parser = argparse.ArgumentParser(
        prog='e3sm_to_cmip',
        description=DESC)
    parser.add_argument(
        'input', 
        help='directory of raw files to use, these should be native grid raw model output')
    parser.add_argument(
        'cwl_path', 
        help='path to the "scripts/cwl_workflows" directory inside the e3sm_to_cmip repository')
    parser.add_argument(
        '-o', '--output', 
        default='testing_output',
        required=False,
        help=f'path to where the output files from the test should be stored, default is {os.environ.get("PWD")}{os.sep}testing_output{os.sep}')
    
    default_map_path = '~zender1/data/maps/map_ne30np4_to_cmip6_180x360_aave.20181001.nc'
    parser.add_argument(
        '--map', 
        default=default_map_path,
        help=f'regrid map path, default is {default_map_path}')

    default_vrt_map_path = '/p/user_pub/e3sm/baldwin32/resources/vrt_remap_plev19.nc'
    parser.add_argument(
        '--vrt-map', 
        default=default_vrt_map_path,
        help=f'vertical remap file path, default is {default_vrt_map_path}')
    
    default_metadata_path = '/p/user_pub/e3sm/baldwin32/resources/CMIP6-Metadata/E3SM-1-0/piControl_r1i1p1f1.json'
    parser.add_argument(
        '--metadata', 
        default=default_metadata_path,
        help=f'metadata file to use in the conversion process {default_metadata_path}')

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
            cwl_path=parsed_args.cwl_path,
            vars=parsed_args.var_list, 
            cmp_branch=parsed_args.compare, 
            input=Path(parsed_args.input),
            output_path=Path(parsed_args.output),
            map_path=parsed_args.map,
            vrt_map=parsed_args.vrt_map,
            metadata=parsed_args.metadata)
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