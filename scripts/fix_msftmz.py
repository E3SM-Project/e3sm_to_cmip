import sys
import os
import argparse
from datetime import datetime
import xarray as xr
import netCDF4
import numpy as np

def update_history(ds):
    '''Add or append history to attributes of a data set'''
    thiscommand = f'{datetime.now().strftime("%a %b %d %H:%M:%S %Y")}: {" ".join(sys.argv[:])}'
    if 'history' in ds.attrs:
        newhist = '\n'.join([thiscommand, ds.attrs['history']])
    else:
        newhist = thiscommand
    ds.attrs['history'] = newhist

def write_netcdf(ds, fileName, fillValues=netCDF4.default_fillvals, unlimited=None):
    '''Write an xarray Dataset with NetCDF4 fill values where needed'''
    encodingDict = {}
    variableNames = list(ds.data_vars.keys()) + list(ds.coords.keys())
    for variableName in variableNames:
        isNumeric = np.issubdtype(ds[variableName].dtype, np.number)
        if isNumeric:
            dtype = ds[variableName].dtype
            for fillType in fillValues:
                if dtype == np.dtype(fillType):
                    encodingDict[variableName] = \
                        {'_FillValue': fillValues[fillType]}
                    break
        else:
            encodingDict[variableName] = {'_FillValue': None}

    update_history(ds)
    ds.to_netcdf(fileName, encoding=encodingDict, unlimited_dims=unlimited)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('input')
    parser.add_argument('output')
    args = parser.parse_args()

    CONSTANT = 1026.0

    now = datetime.today()
    outdir = os.path.join(args.output, f"v{now.year:04d}{now.month:02d}{now.day:02d}")
    if not os.path.exists(outdir):
        os.makedirs(outdir)
    
    names = sorted(os.listdir(args.input))
    outname = f"{names[0][:-9]}{names[-1][-9:]}"
    outpath = os.path.join(outdir, outname)
    print(f"Writing {outname} to {outdir}")

    attrs = {}
    with xr.open_mfdataset(f"{args.input}/*.nc", decode_times=False, combine='by_coords') as ds:
        for var in list(ds.data_vars.keys()) + list(ds.coords.keys()):
            attrs[var] = ds[var].attrs
        ds['msftmz'] = ds['msftmz'] / CONSTANT
        for var, attr in attrs.items():
            ds[var].attrs = attr
        ds['msftmz'].attrs['missing_value'] = 1e20
        write_netcdf(ds, outpath, fillValues={np.dtype('float32'): 1e20, np.dtype('float64'): 1e20}, unlimited=['time'])
    print("Done")
    return 0

if __name__ == "__main__":
    sys.exit(main())