"""
Some of the land variables were given with bad conversion formulas that didnt account for the unit 
conversions. This module will perform the required conversion on the data.
"""
import os
import sys
import xarray as xr
from dask.diagnostics import ProgressBar
from tqdm import tqdm
import argparse
from datetime import datetime

DEFAULT_VARIABLES = ['fFire', 'fHarvest',
                     'cProduct', 'cSoil',
                     'cVeg', 'nbp', 'gpp', 
                     'ra', 'rh', 'cLitter']


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-i', dest="input",
        help="input path, this should be the Lmon directory for a CMIP6 dataset")
    parser.add_argument(
        '-o', dest="output",
        help="output path, by default this will create a new directory with todays date in the CMIP6 format")
    parser.add_argument(
        '-v', nargs='*',
        dest='variables',
        help="list of variables to check, by default will run for {}".format(
            ''.join(DEFAULT_VARIABLES)),
        default=DEFAULT_VARIABLES)
    return parser.parse_args()


def convert_variable(variable, inpath, outpath):
    land_datasets = os.listdir(inpath)
    if variable not in land_datasets:
        print("{} is not in the input dataset".format(variable))
        return 1

    versions = os.listdir(os.path.join(inpath, variable, 'gr'))
    if not versions:
        raise ValueError(
            "{} does not have a version directory".format(variable))

    # make sure to sort the directories or you never know what your gonna get
    latest_version = sorted(versions)[-1]
    dataset_path = os.path.join(os.path.join(
        inpath, variable, 'gr', latest_version))

    now = datetime.now()
    datestamp = 'v{year:04d}{mon:02d}{day:02d}'.format(
        year=now.year, mon=now.month, day=now.day)
    if outpath:
        output_path = os.path.join(outpath, variable, 'gr', datestamp)
    else:
        output_path = os.path.join(inpath, variable, 'gr', datestamp)
    
    if dataset_path == output_path:
        print("Skipping variable {}".format(variable))
        return 0

    if not os.path.exists(output_path):
        os.makedirs(output_path)
    print("Saving output to {}".format(output_path))

    for f in tqdm(os.listdir(dataset_path), desc="Running " + variable):
        ip = os.path.join(dataset_path, f)
        op = os.path.join(output_path, f)
        with xr.open_dataset(ip) as ds:
            if variable not in ds.data_vars:
                raise ValueError(
                    "{} is not in the target dataset".format(variable))

            ds[variable] = ds[variable]/1000.0
            ds.compute()
            ds.to_netcdf(op)


def main():
    args = parse_args()
    variables = args.variables
    if isinstance(variables, str):
        variables = [variables]

    for variable in variables:
        convert_variable(variable, args.input, args.output)
    return 0


if __name__ == '__main__':
    sys.exit(main())
