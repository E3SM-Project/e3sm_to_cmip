"""FISCCP1_COSP to clisccp converter

TODO: Convert this module into the new handlers API (handler.py, handlers.yaml,
formulas.py).
"""

from __future__ import absolute_import, annotations, division, unicode_literals

import logging
from typing import Dict, List, Union

import cmor
import numpy as np
import xarray as xr

from e3sm_to_cmip._logger import _setup_logger
from e3sm_to_cmip.util import print_message, setup_cmor

logger = _setup_logger(__name__)

# list of raw variable names needed
RAW_VARIABLES = [str("FISCCP1_COSP")]
VAR_NAME = str("clisccp")
VAR_UNITS = str("%")
TABLE = str("CMIP6_CFmon.json")


def handle(  # noqa: C901
    vars_to_filepaths: Dict[str, List[str]],
    tables: str,
    metadata_path: str,
    table: str | None,
    logdir: str | None,
) -> str | None:
    """Transform E3SM.TS into CMIP.ts

    clisccp = FISCCP1_COSP with plev7c, and tau

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
    table : str | None
        The CMOR table filename, derived from a custom `freq`, by default
        None.
    logdir : str | None, optional
        The optional CMOR logging directory, by default None.

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
    )

    msg = f"{VAR_NAME}: CMOR setup complete"
    logger.info(msg)

    data: Dict[str, Union[np.ndarray, xr.DataArray]] = {}

    # assuming all year ranges are the same for every variable
    num_files_per_variable = len(vars_to_filepaths["FISCCP1_COSP"])

    # sort the input files for each variable
    vars_to_filepaths["FISCCP1_COSP"].sort()

    for index in range(num_files_per_variable):
        ds = xr.open_dataset(
            vars_to_filepaths["FISCCP1_COSP"][index], decode_times=False
        )

        tau = ds["cosp_tau"].values
        tau[-1] = 100.0
        tau_bnds = ds["cosp_tau_bnds"].values
        tau_bnds[-1] = [60.0, 100000.0]

        # Units of cosp_pr changed from hPa to Pa
        unit_conv_fact = 1
        if ds["cosp_prs"].units == "hPa":
            unit_conv_fact = 100

        # load
        data = {
            "FISCCP1_COSP": ds["FISCCP1_COSP"].values,
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

        # write out the data
        msg = f"{VAR_NAME}: time {data['time_bnds'][0][0]:1.1f} - {data['time_bnds'][-1][-1]:1.1f}"
        logger.info(msg)

        cmor.write(
            varid,
            data["FISCCP1_COSP"],
            time_vals=data["time"],
            time_bnds=data["time_bnds"],
        )

    msg = f"{VAR_NAME}: write complete, closing"
    logger.info(msg)

    cmor.close()
    msg = f"{VAR_NAME}: file close complete"
    logger.info(msg)

    return VAR_NAME
