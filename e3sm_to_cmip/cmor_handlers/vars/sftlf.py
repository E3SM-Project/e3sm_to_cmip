"""
LANDFRAC to sftlf converter
"""

from __future__ import absolute_import, division, print_function, unicode_literals

import json
import logging
import os

import cmor
import numpy as np
import xarray as xr

from e3sm_to_cmip import resources
from e3sm_to_cmip._logger import _setup_child_logger
from e3sm_to_cmip.mpas import write_netcdf
from e3sm_to_cmip.util import print_message, setup_cmor

logger = _setup_child_logger(__name__)

# list of raw variable names needed
RAW_VARIABLES = [str("LANDFRAC")]
VAR_NAME = str("sftlf")
VAR_UNITS = str("%")
TABLE = str("CMIP6_fx.json")


def handle_simple(infiles):
    resource_path, _ = os.path.split(os.path.abspath(resources.__file__))
    table_path = os.path.join(resource_path, TABLE)
    with open(table_path, "r") as ip:
        table_data = json.load(ip)

    ds = xr.Dataset()
    outname = f"{VAR_NAME}_fx_.nc"
    with xr.open_dataset(infiles[RAW_VARIABLES[0]][0]) as inputds:
        ds["lat"] = inputds["lat"]
        ds["lat_bnds"] = inputds["lat_bnds"]
        ds["lon"] = inputds["lon"]
        ds["lon_bnds"] = inputds["lon_bnds"]
        outdata = inputds["LANDFRAC"] * 100.0

        for attr, val in inputds.attrs.items():
            ds.attrs[attr] = val

    ds[VAR_NAME] = outdata
    for attr in ["standard_name", "cell_methods", "long_name", "comment", "units"]:
        ds[VAR_NAME].attrs[attr] = table_data["variable_entry"][VAR_NAME][attr]

    fillVals = {
        np.dtype("float32"): 1e20,
        np.dtype("float64"): 1e20,
    }
    write_netcdf(ds, outname, fillValues=fillVals, unlimited=["time"])


def handle(infiles, tables, user_input_path, cmor_log_dir, table):
    msg = f"{VAR_NAME}: Starting"
    logger.info(msg)

    # check that we have some input files for every variable
    zerofiles = False
    for variable in RAW_VARIABLES:
        if len(infiles[variable]) == 0:
            msg = f"{VAR_NAME}: Unable to find input files for {variable}"
            print_message(msg)
            logging.error(msg)
            zerofiles = True
    if zerofiles:
        return None

    setup_cmor(
        var_name=VAR_NAME,
        table_path=tables,
        table_name=TABLE,
        cmor_log_dir=cmor_log_dir,
        user_input_path=user_input_path,
    )

    msg = "{}: CMOR setup complete".format(VAR_NAME)
    logging.info(msg)

    # extract data from the input file
    msg = "sftlf: loading LANDFRAC"
    logger.info(msg)

    filename = infiles["LANDFRAC"][0]

    if not os.path.exists(filename):
        raise IOError("File not found: {}".format(filename))

    ds = xr.open_dataset(filename, decode_times=False)

    # load the lon and lat info & bounds
    data = {
        "lat": ds["lat"],
        "lon": ds["lon"],
        "lat_bnds": ds["lat_bnds"],
        "lon_bnds": ds["lon_bnds"],
        "LANDFRAC": ds["LANDFRAC"],
    }

    msg = f"{VAR_NAME}: loading axes"
    logger.info(msg)

    axes = [
        {
            str("table_entry"): str("latitude"),
            str("units"): ds["lat"].units,
            str("coord_vals"): data["lat"].values,
            str("cell_bounds"): data["lat_bnds"].values,
        },
        {
            str("table_entry"): str("longitude"),
            str("units"): ds["lon"].units,
            str("coord_vals"): data["lon"].values,
            str("cell_bounds"): data["lon_bnds"].values,
        },
    ]

    msg = "sftlf: running CMOR"
    logging.info(msg)

    axis_ids = list()
    for axis in axes:
        axis_id = cmor.axis(**axis)
        axis_ids.append(axis_id)

    varid = cmor.variable(VAR_NAME, VAR_UNITS, axis_ids)

    outdata = data["LANDFRAC"].values * 100.0
    cmor.write(varid, outdata)

    msg = "{}: write complete, closing".format(VAR_NAME)
    logger.debug(msg)

    cmor.close()

    msg = "{}: file close complete".format(VAR_NAME)
    logger.debug(msg)

    return "sftlf"
