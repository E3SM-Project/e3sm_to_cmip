"""CLMODIS to clmodis converter

TODO: Convert this module into the new handlers API (handler.py, handlers.yaml,
formulas.py).
"""

from __future__ import absolute_import, annotations, division, unicode_literals

import logging
from typing import Dict, List, Union

import cmor
import numpy as np
import xarray as xr

from e3sm_to_cmip._logger import _setup_child_logger
from e3sm_to_cmip.cmor_handlers.vars.utils import fill_nan
from e3sm_to_cmip.util import print_message, setup_cmor

logger = _setup_child_logger(__name__)

# list of raw variable names needed
RAW_VARIABLES = [str("CLMODIS")]
VAR_NAME = str("clmodis")
VAR_UNITS = str("%")
TABLE = str("CMIP6_CFmon.json")


def handle(  # noqa: C901
    vars_to_filepaths: Dict[str, List[str]],
    tables: str,
    metadata_path: str,
    cmor_log_dir: str,
    table: str | None,
) -> str | None:
    """Transform E3SM.CLMODIS into CMIP.clmodis

    clmodis = CLMODIS

    NOTE: `table` is not used and is a placeholder because VarHandler.cmorize
    also includes the `freq` arg.

    Parameters
    ----------
    vars_to_filepaths : Dict[str, List[str]]
        A dictionary mapping E3SM raw variables to a list of filepath(s).
    tables_path : str
        The path to directory containing CMOR Tables directory.
    metadata_path : str
        The path to user json file for CMIP6 metadata
    cmor_log_dir : str
            The directory that stores the CMOR logs.
    table : str | None
        The CMOR table filename, derived from a custom `freq`, by default
        None.

    Returns
    -------
    str | None
        If CMORizing was successful, return the output CMIP variable name
        to indicate success. If failed, return None
    ."""
    logging.info(f"Starting {VAR_NAME}")

    nonzero = False
    for variable in RAW_VARIABLES:
        if len(vars_to_filepaths[variable]) == 0:
            msg = f"{variable}: Unable to find input files for {RAW_VARIABLES}"
            print_message(msg)
            logging.error(msg)
            nonzero = True
    if nonzero:
        return None

    msg = f"{VAR_NAME}: running with input files: {vars_to_filepaths}"
    logger.debug(msg)

    setup_cmor(
        var_name=VAR_NAME,
        table_path=tables,
        table_name=TABLE,
        user_input_path=metadata_path,
        cmor_log_dir=cmor_log_dir,
    )

    msg = f"{VAR_NAME}: CMOR setup complete"
    logger.info(msg)

    data: Dict[str, Union[np.ndarray, xr.DataArray]] = {}

    # assuming all year ranges are the same for every variable
    num_files_per_variable = len(vars_to_filepaths["CLMODIS"])

    # sort the input files for each variable
    vars_to_filepaths["CLMODIS"].sort()

    for index in range(num_files_per_variable):
        ds = xr.open_dataset(vars_to_filepaths["CLMODIS"][index], decode_times=False)

        tau = ds["cosp_tau_modis"].values
        tau[-1] = 100.0
        tau_bnds = ds["cosp_tau_modis_bnds"].values
        tau_bnds[-1] = [60.0, 100000.0]

        # Units of cosp_pr changed from hPa to Pa
        unit_conv_fact = 1
        if ds["cosp_prs"].units == "hPa":
            unit_conv_fact = 100

        # load
        data = {
            "CLMODIS": ds["CLMODIS"].values,
            "lat": ds["lat"],
            "lon": ds["lon"],
            "lat_bnds": ds["lat_bnds"],
            "lon_bnds": ds["lon_bnds"],
            "time": ds["time"].values,
            "time_bnds": ds["time_bnds"].values,
            "plev7c": ds["cosp_prs"].values * unit_conv_fact,
            "plev7c_bnds": ds["cosp_prs_bnds"].values * unit_conv_fact,
            "tau": tau,
            "tau_bnds": tau_bnds,
        }

        # create the cmor variable and axis
        axes = [
            {str("table_entry"): str("time"), str("units"): ds["time"].units},
            {
                str("table_entry"): str("plev7c"),
                str("units"): str("Pa"),
                str("coord_vals"): data["plev7c"],
                str("cell_bounds"): data["plev7c_bnds"],
            },
            {
                str("table_entry"): str("tau"),
                str("units"): str("1"),
                str("coord_vals"): data["tau"],
                str("cell_bounds"): data["tau_bnds"],
            },
            {
                str("table_entry"): str("latitude"),
                str("units"): ds["lat"].units,
                str("coord_vals"): data["lat"].values,  # type: ignore
                str("cell_bounds"): data["lat_bnds"].values,  # type: ignore
            },
            {
                str("table_entry"): str("longitude"),
                str("units"): ds["lon"].units,
                str("coord_vals"): data["lon"].values,  # type: ignore
                str("cell_bounds"): data["lon_bnds"].values,  # type: ignore
            },
        ]

        axis_ids = list()
        for axis in axes:
            axis_id = cmor.axis(**axis)
            axis_ids.append(axis_id)

        varid = cmor.variable(VAR_NAME, VAR_UNITS, axis_ids)

        # Replace NaNs in data with appropriate fill-value for cmor.write,
        # which does not support np.nan as a fill value.
        data["CLMODIS"] = fill_nan(data["CLMODIS"])  # type: ignore

        # write out the data
        msg = f"{VAR_NAME}: time {data['time_bnds'][0][0]:1.1f} - {data['time_bnds'][-1][-1]:1.1f}"
        logger.info(msg)

        cmor.write(
            varid,
            data["CLMODIS"],
            time_vals=data["time"],
            time_bnds=data["time_bnds"],
        )

    msg = f"{VAR_NAME}: write complete, closing"
    logger.info(msg)

    cmor.close()
    msg = f"{VAR_NAME}: file close complete"
    logger.info(msg)

    return VAR_NAME
