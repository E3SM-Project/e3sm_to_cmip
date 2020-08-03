import sys
import os
import xarray as xr
import numpy as np
import argparse
from shutil import rmtree
from tqdm import tqdm
from subprocess import Popen, PIPE


def get_cmip_output(path):
    for root, dirs, files in os.walk(path):
        if dirs:
            continue
        if not files:
            continue
        yield os.path.join(root, files.pop())


def find_matching_file(file, path):
    for root, dirs, files in os.walk(path):
        if dirs:
            continue
        if not files:
            continue
        for f in files:
            if f == file:
                return os.path.join(root, file)
    raise ValueError(f"could not find matching file for {file}")


def get_varname(file):
    idx = file.index('_')
    return file[:idx]


def check_zero_diff(sample, control, varname):
    """
    Params:
        sample (str): the path to the test data file
        control (str): the path to the control data file
    Returns:
        True if the difference is zero for all time steps
        False if there is a non-zero difference
    """
    sample_ds = xr.open_dataset(sample)
    control_ds = xr.open_dataset(control)

    exclude_dims = ['time', 'height', 'wavelength']
    dims = tuple([d for d in control_ds.coords if d not in exclude_dims])
    sample_mean = sample_ds.mean(dims).compute()
    control_mean = control_ds.mean(dims).compute()

    for idx, _ in enumerate(control_ds):
        if control_mean[varname][idx] != sample_mean[varname][idx]:
            return False

    return True


def main(control_path):

    if os.path.exists('CMIP6'):
        print(" CMIP6 exists, skipping converter ".center(50, '='))
    else:
        print(' starting CMIP6 converter '.center(50, '='))
        cmd = 'cwltool atm-unified.cwl atm-unified-test-job.yaml'
        proc = Popen(cmd.split(), stdout=PIPE, stderr=PIPE)
        out, err = proc.communicate()
        if proc.returncode != 0:
            print(out.decode('utf-8'))
            print(err.decode('utf-8'))
            return 1

    # iterate over variables produced
    pbar = tqdm()
    for sample_file in get_cmip_output('CMIP6'):
        _, name = os.path.split(sample_file)
        varname = get_varname(name)
        pbar.set_description(f'checking {varname}')
        control_var_path = find_matching_file(name, control_path)
        if not check_zero_diff(sample_file, control_var_path, varname):
            raise ValueError(
                f"{sample_file} has a non-zero diff with its control {control_var_path}")
        pbar.update(1)
    pbar.close()

    print(" all variables pass zero-diff check  ".center(50, '='))
    if not args.keep:
        print(' cleaning up CMIP6 directories '.center(50, '='))
        rmtree('CMIP6')
        rmtree('cmor_logs')
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--control', '-c',
                        help="path to root of CMIP6 control data")
    parser.add_argument('--keep', '-k',
                        help="keep the CMIP6 output produced, otherwise it will be removed")
    args = parser.parse_args()
    sys.exit(main(args.control))
