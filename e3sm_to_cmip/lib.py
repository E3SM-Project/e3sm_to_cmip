# Import the needed `cmor` modules individually, otherwise the error
# `Can't pickle <class '_cmor.CMORError'>: import of module '_cmor' failed`
# is raised when running `e3sm_to_cmip`` in parallel. Further investigation is
# needed to figure out why this happens (maybe a circular or non-existent import
# of CMORError`).
from __future__ import annotations

import json
import logging
import os
from typing import Callable, Dict, List, TypedDict, no_type_check

import cmor
import numpy as np
import xarray as xr
from tqdm import tqdm

from e3sm_to_cmip import resources
from e3sm_to_cmip._logger import _setup_logger
from e3sm_to_cmip.mpas import write_netcdf
from e3sm_to_cmip.util import (
    find_atm_files,
    find_mpas_files,
    get_levgrnd_bnds,
    print_debug,
    print_message,
    terminate,
)

logger = _setup_logger(__name__)


LEVEL_NAMES = [
    "atmosphere_sigma_coordinate",
    "standard_hybrid_sigma",
    "standard_hybrid_sigma_half",
]

# A type annotation for a dictionary with each component of a variable.
# For example, a key can be the name of the axis such as "lat" and the
# value is the data represented as either a `np.ndarray` or `xr.DataArray`.
VarDataDict = Dict[str, np.ndarray | xr.DataArray]


class Levels(TypedDict):
    name: str
    units: str
    e3sm_axis_name: str
    e3sm_axis_bnds: str | None
    time_name: str | None


def run_parallel(
    pool,
    handlers,
    input_path,
    tables_path,
    metadata_path,
    map_path=None,
    realm="atm",
    nproc=6,
    **kwargs,
):
    """
    Run all the handlers in parallel
    Params:
    -------
        pool (multiprocessing.Pool): a processing pool to run the handlers in
        handlers: a dict(str: (function_pointer, list(str) ) )
        input_path (str): path to the input files directory
        tables_path (str): path to the tables directory
        metadata_path (str): path to the cmor input metadata
        realm (str): the realm of the data, [atm, lnd, mpaso, mpassi]
    Returns:
    --------
        returns 1 if an error occurs, else 0
    """
    pool_res = list()
    will_run = []

    for idx, handler in enumerate(handlers):
        handler_method = handler["method"]
        handler_variables = handler["raw_variables"]
        # find the input files this handler needs
        if realm in ["atm", "lnd"]:
            var_to_filepaths = {
                var: [
                    os.path.join(input_path, x) for x in find_atm_files(var, input_path)
                ]
                for var in handler_variables
            }
        elif realm == "fx":
            var_to_filepaths = {
                var: [
                    os.path.join(input_path, x)
                    for x in os.listdir(input_path)
                    if x[-3:] == ".nc"
                ]
                for var in handler_variables
            }
        else:
            var_to_filepaths = {
                var: find_mpas_files(var, input_path, map_path)
                for var in handler_variables
            }

        # Setup the input args for the handler.
        _kwargs = {
            "table": handler.get("table"),
            "raw_variables": handler.get("raw_variables"),
            "units": handler.get("units"),
            "positive": handler.get("positive"),
            "name": handler.get("name"),
            "logdir": kwargs.get("logdir"),
            "unit_conversion": handler.get("unit_conversion"),
            "simple": kwargs.get("simple"),
            "outpath": kwargs.get("outpath"),
        }
        will_run.append(handler.get("name"))

        pool_res.append(
            pool.submit(
                handler_method, var_to_filepaths, tables_path, metadata_path, **_kwargs
            )
        )

    # wait for each result to complete
    pbar = tqdm(total=len(pool_res))
    num_success = 0
    num_handlers = len(handlers)
    finished_success = []
    for idx, res in enumerate(pool_res):
        try:
            out = res.result()
            finished_success.append(out)
            if out:
                num_success += 1
                msg = f"Finished {out}, {idx + 1}/{num_handlers} jobs complete"
            else:
                msg = f'Error running handler {handlers[idx]["name"]}'
                logger.error(msg)

            logger.info(msg)
        except Exception as e:
            print_debug(e)
        pbar.update(1)

    pbar.close()
    terminate(pool)

    msg = f"{num_success} of {num_handlers} handlers complete"
    logger.info(msg)

    failed = set(will_run) - set(finished_success)
    if failed:
        logger.error(f"{', '.join(list(failed))} failed to complete")
        logger.error(msg)

    return 0


def run_serial(  # noqa: C901
    handlers,
    input_path,
    tables_path,
    metadata_path,
    map_path=None,
    realm="atm",
    logdir=None,
    simple=False,
    outpath=None,
    freq="mon",
):
    """
    Run each of the handlers one at a time on the main process

    Params:
    -------
        handlers: a dict(str: (function_pointer, list(str) ) )
        input_path (str): path to the input files directory
        tables_path (str): path to the tables directory
        metadata_path (str): path to the cmor input metadata
        realm (str): what type of files to work with
    Returns:
    --------
        returns 1 if an error occurs, else 0
    """
    try:
        num_handlers = len(handlers)
        num_success = 0
        name = None

        if realm != "atm":
            pbar = tqdm(total=len(handlers))

        for _, handler in enumerate(handlers):
            handler_method = handler["method"]
            handler_variables = handler["raw_variables"]
            unit_conversion = handler.get("unit_conversion")

            # find the input files this handler needs
            if realm in ["atm", "lnd"]:
                var_to_filepaths = {
                    var: [
                        os.path.join(input_path, x)
                        for x in find_atm_files(var, input_path)
                    ]
                    for var in handler_variables
                }
            elif realm == "fx":
                var_to_filepaths = {
                    var: [
                        os.path.join(input_path, x)
                        for x in os.listdir(input_path)
                        if x[-3:] == ".nc"
                    ]
                    for var in handler_variables
                }
            else:
                var_to_filepaths = {
                    var: find_mpas_files(var, input_path, map_path)
                    for var in handler_variables
                }

            msg = f"Trying to CMORize with handler: {handler}"
            logger.info(msg)

            try:
                name = handler_method(
                    var_to_filepaths,
                    tables_path,
                    metadata_path,
                    raw_variables=handler.get("raw_variables"),
                    units=handler.get("units"),
                    name=handler.get("name"),
                    table=handler.get("table"),
                    positive=handler.get("positive"),
                    serial=True,
                    logdir=logdir,
                    simple=simple,
                    outpath=outpath,
                    unit_conversion=unit_conversion,
                    freq=freq,
                )
            except Exception as e:
                print_debug(e)

            if name is not None:
                num_success += 1
                msg = f"Finished {name}, {num_success}/{num_handlers} jobs complete"
                logger.info(msg)
            else:
                msg = f"Error running handler {handler['name']}"
                logger.info(msg)

            if realm != "atm":
                pbar.update(1)
        if realm != "atm":
            pbar.close()

    except Exception as error:
        print_debug(error)
        return 1
    else:
        msg = f"{num_success} of {num_handlers} handlers complete"
        logger.info(msg)

        return 0


# ------------------------------------------------------------------


@no_type_check
def handle_simple(  # noqa: C901 # type: ignore
    infiles,
    raw_variables,
    write_data,
    outvar_name,
    outvar_units,
    serial=None,
    positive=None,
    levels=None,
    axis=None,
    logdir=None,
    outpath=None,
    table="Amon",
    time_name=None,
):
    # TODO: This function needs to be refactored. There is a lot of redundant
    # code that is copied and pasted from `handle_variables()`. This
    # function is also broken somewhere.
    logger.info(f"{outvar_name}: Starting")

    # check that we have some input files for every variable
    zerofiles = False
    for variable in raw_variables:
        if len(infiles[variable]) == 0:
            msg = f"{outvar_name}: Unable to find input files for {variable}"
            logging.error(msg)
            zerofiles = True
    if zerofiles:
        return None

    # Create the logging directory and setup cmor
    if logdir:
        logpath = logdir
    else:
        outpath, _ = os.path.split(logger.__dict__["handlers"][0].baseFilename)
        logpath = os.path.join(outpath, "cmor_logs")
    os.makedirs(logpath, exist_ok=True)

    _, inputfile = os.path.split(sorted(infiles[raw_variables[0]])[0])
    # counting from the end, since the variable names might have a _ in them
    start_year = inputfile[len(raw_variables[0]) + 1 :].split("_")[0]
    end_year = inputfile[len(raw_variables[0]) + 1 :].split("_")[1]

    data = {}

    # assuming all year ranges are the same for every variable
    num_files_per_variable = len(infiles[raw_variables[0]])

    # sort the input files for each variable
    for var_name in raw_variables:
        infiles[var_name].sort()

    for file_index in range(num_files_per_variable):
        loaded = False

        # reload the dimensions for each time slice
        get_dims = True

        # load data for each variables
        for var_name in raw_variables:
            # extract data from the input file
            logger.info(f"{outvar_name}: loading {var_name}")

            new_data = _get_var_data_dict(
                filename=infiles[var_name][file_index],
                variable=var_name,
                levels=levels,
                get_dims=get_dims,
            )
            data.update(new_data)
            get_dims = False
            if not loaded:
                loaded = True

                # new data set
                ds = xr.Dataset()
                if time_name is not None:
                    dims = ["time", "lat", "lon"]
                else:
                    dims = ["lat", "lon"]

                for depth_dim in ["lev", "plev", "levgrnd"]:
                    if depth_dim in new_data.keys():
                        dims.insert(1, depth_dim)

                ds[outvar_name] = (tuple(dims), new_data[var_name])
                for d in dims:
                    ds.coords[d] = new_data[d][:]

        # write out the data
        msg = f"{outvar_name}: time {data['time_bnds'].values[0][0]:1.1f} - {data['time_bnds'].values[-1][-1]:1.1f}"

        logger.info(msg)

        if serial:
            pbar = tqdm(total=len(data["time"]))
            pbar.set_description(msg)

        for time_index, val in enumerate(data["time"]):
            outdata = write_data(
                varid=0,
                data=data,
                timeval=val,
                timebnds=[data["time_bnds"].values[time_index, :]],
                index=time_index,
                raw_variables=raw_variables,
                simple=True,
            )
            ds[outvar_name][time_index] = outdata
            if serial:
                pbar.update(1)

        if serial:
            pbar.close()

    with xr.open_dataset(
        infiles[raw_variables[0]][0], decode_cf=False, decode_times=False
    ) as inputds:
        for attr, val in inputds.attrs.items():
            ds.attrs[attr] = val

        ds["lat_bnds"] = inputds["lat_bnds"]
        ds["lon_bnds"] = inputds["lon_bnds"]

        # check for and change the bounds name for lnd files since "time_bounds" is different
        # from every other bounds name in the entire E3SM project
        time_bounds_name = (
            "time_bnds" if "time_bnds" in inputds.data_vars else "time_bounds"
        )
        ds["time_bnds"] = inputds[time_bounds_name]
        ds["time"] = inputds["time"]
        ds["time"].attrs["bounds"] = "time_bnds"

    resource_path, _ = os.path.split(os.path.abspath(resources.__file__))
    table_path = os.path.join(resource_path, table)
    with open(table_path, "r") as ip:
        table_data = json.load(ip)

    variable_attrs = [
        "standard_name",
        "long_name",
        "comment",
        "cell_methods",
        "cell_measures",
        "units",
    ]
    for attr in variable_attrs:
        ds[outvar_name].attrs[attr] = table_data["variable_entry"][outvar_name][attr]

    output_file_path = os.path.join(
        outpath, f"{outvar_name}_{table[:-5]}_{start_year}-{end_year}"
    )
    msg = f"writing out variable to file {output_file_path}"
    print_message(msg, "ok")
    fillVals = {
        np.dtype("float32"): 1e20,
        np.dtype("float64"): 1e20,
    }
    write_netcdf(ds, output_file_path, fillValues=fillVals, unlimited=["time"])

    msg = f"{outvar_name}: file close complete"
    logger.debug(msg)

    return outvar_name


def handle_variables(  # noqa: C901
    var_to_filepaths: Dict[str, List[str]],
    raw_variables: List[str],
    write_data: Callable,
    outvar_name: str,
    outvar_units: str,
    table: str,
    tables: str,
    metadata_path: str,
    serial=None,
    positive=None,
    levels=None,
    axis=None,
    logdir=None,
    simple=False,
    outpath=None,
):
    # TODO: Move this function to `handler.VarHandler` once all legacy handlers
    # are refactored.
    table_abs_path = os.path.join(tables, table)
    time_name: str | None = _get_var_time_name(table_abs_path, outvar_name)

    if simple:
        return handle_simple(
            var_to_filepaths,
            raw_variables,
            write_data,
            outvar_name,
            outvar_units,
            serial=serial,
            table=table,
            positive=positive,
            levels=levels,
            axis=axis,
            logdir=logdir,
            outpath=outpath,
            time_name=time_name,
        )

    logger.info(f"{outvar_name}: Starting")

    # Check that we have some input files for every variable
    # --------------------------------------------------------------------------
    zerofiles = False

    for variable in raw_variables:
        if len(var_to_filepaths[variable]) == 0:
            logging.error(f"{outvar_name}: Unable to find input files for {variable}")
            zerofiles = True

    if zerofiles:
        return None

    # Create the logging directory and setup CMOR
    # --------------------------------------------------------------------------
    _setup_cmor_module(outvar_name, table, tables, metadata_path, logdir)

    # Loop over variable files and CMORize
    # --------------------------------------------------------------------------
    # A dictionary, with the key representing the axis and the value being
    # an array for the axis data.
    var_data_dict: VarDataDict = {}

    # Assuming all year ranges are the same for every variable
    num_files_per_variable = len(var_to_filepaths[raw_variables[0]])

    # Sort the input files for each variable to ensure axis data is in correct
    # order.
    for var_name in raw_variables:
        var_to_filepaths[var_name].sort()

    for index in range(num_files_per_variable):
        # Reload the dimensions for each time slice
        get_dims = True

        # Get the axis data for each variable
        for var_name in raw_variables:
            logger.info(f"{outvar_name}: loading {var_name}")

            new_data_dict = _get_var_data_dict(
                filename=var_to_filepaths[var_name][index],
                variable=var_name,
                levels=levels,
                get_dims=get_dims,
            )
            var_data_dict.update(new_data_dict)
            get_dims = False

        logger.info(f"{outvar_name}: loading axes")

        # Create the base CMOR Variable
        # ----------------------------------------------------------------------
        cmor_axes, ips = _get_cmor_axes_and_ips(
            data=var_data_dict, levels=levels, time_name=time_name
        )
        cmor_var = cmor.variable(
            outvar_name, outvar_units, cmor_axes, positive=positive
        )

        if ips is not None:
            var_data_dict["ips"] = ips

        # CMORize the variable.
        # ----------------------------------------------------------------------
        time_bnds_data = var_data_dict["time_bnds"].values  # type: ignore
        logger_msg = (
            f"{outvar_name}: time {time_bnds_data[0][0]:1.1f} - "
            f"{time_bnds_data[-1][-1]:1.1f}"
        )
        logger.info(logger_msg)

        if time_name is not None:
            if serial:
                pbar = tqdm(total=var_data_dict["time"].shape[0])
                pbar.set_description(logger_msg)

            try:
                time_data = var_data_dict["time"].values  # type: ignore
                for index, val in enumerate(time_data):
                    write_data(
                        varid=cmor_var,
                        data=var_data_dict,
                        timeval=val,
                        timebnds=[time_bnds_data[index, :]],
                        index=index,
                        raw_variables=raw_variables,
                    )

                    if serial:
                        pbar.update(1)
            except Exception as e:
                logger.error(e)

            if serial:
                pbar.close()
        else:
            write_data(varid=cmor_var, data=var_data_dict, raw_variables=raw_variables)

    logger.debug(f"{outvar_name}: write complete, closing")
    cmor.close()
    logger.debug(f"{outvar_name}: file close complete")

    return outvar_name


def _get_var_time_name(table_path: str, variable: str) -> str | None:
    with open(table_path, "r") as inputstream:
        table_info = json.load(inputstream)

    axis_info = table_info["variable_entry"][variable]["dimensions"].split(" ")

    if "time" in axis_info:
        return "time"
    elif "time1" in axis_info:
        return "time1"

    return None


def _setup_cmor_module(
    var_name: str,
    table: str,
    tables_path: str,
    metadata_path: str,
    log_dir: str | None,
):
    """Set up the CMOR module before CMORzing variables.

    Parameters
    ----------
    var_name : str
        The name of the variable.
    table : str
        The table filename.
    tables_path : str
        The tables path.
    metadata_path : str
        The metadata path.
    logdir : str | None
        The optional log directory.
    """
    if log_dir:
        logpath = log_dir
    else:
        outpath, _ = os.path.split(logger.__dict__["handlers"][0].baseFilename)
        logpath = os.path.join(outpath, "cmor_logs")

    os.makedirs(logpath, exist_ok=True)
    logfile = os.path.join(logpath, var_name + ".log")

    cmor.setup(
        inpath=tables_path, netcdf_file_action=cmor.CMOR_REPLACE, logfile=logfile
    )
    cmor.dataset_json(metadata_path)
    cmor.load_table(table)

    logging.info(f"{var_name}: CMOR setup complete")


def _get_var_data_dict(  # noqa: C901
    filename: str, variable: str, levels: Levels | None, get_dims: bool = False
) -> VarDataDict:
    """Get the variable's data dictionary.

    This function creates the variable's data dictionary by opening up a netCDF
    file as an `xr.Dataset` and extracting information such as the axes data,
    levels, etc.

    # TODO: Fill out below example.
    Example output:
        {
            data: xarray Dataset from the file
            lat: numpy array of lat midpoints,
            lat_bnds: numpy array of lat edge points,
            lon: numpy array of lon midpoints,
            lon_bnds: numpy array of lon edge points,
            time: array of time points,
            time_bdns: array of time bounds
            <OPTIONAL, for 3D variables>
            lev:
            ilev:
            ps:
            p0:
            hyam:
            hyai:
            hybm:
            hybi:
        }

    Parameters
    ----------
    filename : str
        The name of the netCDF dataset containing the variable.
    variable : str
        The name of the variable.
    levels : Levels | None
        The optional levels dictionary.
    get_dims : bool, optional
        Whether to get the dimensions or not, by default False

    Returns
    -------
    VarDataDict
        A dictionary with the key being a component of the variable (e.g., axis)
        and the value being data represented as `nd.array` or `xr.DataArray`.

    Raises
    ------
    IOError
        _description_
    KeyError
        _description_
    KeyError
        _description_
    """
    if not os.path.exists(filename):
        raise IOError(f"File not found: {filename}")

    ds: xr.Dataset = xr.open_dataset(filename, decode_times=False)
    da_var: xr.DataArray = ds[variable]

    var_data_dict: VarDataDict = {}

    # The `np.ndarray` of plev and lev are extracted from the xr.DataArray.
    if "plev" in ds.dims or "lev" in ds.dims:
        var_data_dict[variable] = da_var.values
    else:
        var_data_dict[variable] = da_var

    # load the lon and lat and time info & bounds
    if get_dims:
        var_data_dict.update(
            {
                "lat": ds["lat"],
                "lon": ds["lon"],
                "lat_bnds": ds["lat_bnds"],
                "lon_bnds": ds["lon_bnds"],
                "time": ds["time"],
            }
        )

        try:
            time2 = ds["time2"]
        except KeyError:
            pass
        else:
            var_data_dict["time2"] = time2

        # NOTE: atm uses "time_bnds" but the lnd component uses "time_bounds"
        time_bounds_name = (
            "time_bnds" if "time_bnds" in ds.data_vars.keys() else "time_bounds"
        )

        if time_bounds_name in ds.data_vars:
            time_bnds = ds[time_bounds_name]

            if len(time_bnds.shape) == 1:
                time_bnds = time_bnds.reshape(1, 2)

            var_data_dict["time_bnds"] = time_bnds

        # load level and level bounds
        if levels is not None:
            name = levels.get("name")

            if name in LEVEL_NAMES:
                var_data_dict.update(
                    {
                        "lev": ds["lev"].values / 1000,
                        "ilev": ds["ilev"].values / 1000,
                        "ps": ds["PS"].values,
                        "p0": ds["P0"].values.item(),
                        "hyam": ds["hyam"],
                        "hyai": ds["hyai"],
                        "hybm": ds["hybm"],
                        "hybi": ds["hybm"],
                    }
                )
            elif name == "sdepth":
                var_data_dict.update(
                    {
                        "levgrnd": ds["levgrnd"].values,
                        "levgrnd_bnds": get_levgrnd_bnds(),
                    }
                )
            else:
                e3sm_axis_name = levels.get("e3sm_axis_name", None)
                if e3sm_axis_name is None:
                    raise KeyError(
                        "No 'e3sm_axis_name' key is defined in the handler for "
                        f"'{variable}'."
                    )

                if e3sm_axis_name in ds.data_vars or e3sm_axis_name in ds.coords:
                    var_data_dict[e3sm_axis_name] = ds[e3sm_axis_name]
                else:
                    raise KeyError(
                        f"Unable to find they 'e3sm_axis_name' key ({e3sm_axis_name}) "
                        "in the dataset for '{variable}'."
                    )

                e3sm_axis_bnds = levels.get("e3sm_axis_bnds")

                if e3sm_axis_bnds is not None:
                    if e3sm_axis_bnds in ds.dims or e3sm_axis_bnds in ds.data_vars:
                        var_data_dict[e3sm_axis_bnds] = ds[e3sm_axis_bnds]
                    else:
                        raise KeyError(
                            "Unable to find 'e3sm_axis_bnds' key in the dataset for "
                            f"'{variable}'."
                        )
    return var_data_dict


def _get_cmor_axes_and_ips(
    data: Dict[str, np.ndarray | xr.DataArray],
    levels: Levels | None = None,
    time_name: str | None = None,
):
    # Create the CMOR lat and lon axis objects, which always exist.
    lat = cmor.axis(
        "latitude",
        units=data["lat"].units,  # type: ignore
        coord_vals=data["lat"].values,  # type: ignore
        cell_bounds=data["lat_bnds"].values,  # type: ignore
    )

    lon = cmor.axis(
        "longitude",
        units=data["lon"].units,  # type: ignore
        coord_vals=data["lon"].values,  # type: ignore
        cell_bounds=data["lon_bnds"].values,  # type: ignore
    )

    # Create the list of CMOR axes to return.
    cmor_axes = [lat, lon]

    # Create the lev CMOR axis.
    lev = None
    if levels is not None:
        lev = _get_lev_axis(levels, data)
        cmor_axes.insert(0, lev)

    # # Create the time CMOR axis.
    # Use either the special name for time if it exists or the normal time.
    time = None
    if levels is not None:
        levels_time_name = levels.get("time_name", None)

        if levels_time_name is not None:
            units = data[levels_time_name].units  # type: ignore
            time = cmor.axis(levels_time_name, units=units)
            cmor_axes.insert(0, time)

    elif time_name is not None:
        time = cmor.axis(time_name, units=data["time"].attrs["units"])  # type: ignore

        cmor_axes.insert(0, time)

    if levels is not None and levels.get("name") in LEVEL_NAMES:
        ips = _get_hybrid_level_formula_terms(data, time, lev, lat, lon)
    else:
        ips = None

    return cmor_axes, ips


def _get_lev_axis(levels, data):
    name = levels.get("name")
    units = levels.get("units")
    e3sm_axis_name = levels["e3sm_axis_name"]

    coord_vals = data[e3sm_axis_name]

    if isinstance(coord_vals, xr.DataArray):
        coord_vals = coord_vals.values

    axis_bnds = levels.get("e3sm_axis_bnds")

    if axis_bnds is not None:
        cell_bounds = data[axis_bnds]

        # i.g. handler cl, cli, clw returns xarray dataarray
        if isinstance(cell_bounds, xr.DataArray):
            cell_bounds = cell_bounds.values

        lev = cmor.axis(
            name, units=units, coord_vals=coord_vals, cell_bounds=cell_bounds
        )
    else:
        lev = cmor.axis(name, units=units, coord_vals=coord_vals)

    return lev


def _get_hybrid_level_formula_terms(data, time, lev, lat, lon) -> cmor.zfactor:
    cmor.zfactor(
        zaxis_id=lev,
        zfactor_name="a",
        axis_ids=[
            lev,
        ],
        zfactor_values=data["hyam"].values,
        zfactor_bounds=data["hyai"].values,
    )

    cmor.zfactor(
        zaxis_id=lev,
        zfactor_name="b",
        axis_ids=[
            lev,
        ],
        zfactor_values=data["hybm"].values,
        zfactor_bounds=data["hybi"].values,
    )

    cmor.zfactor(zaxis_id=lev, zfactor_name="p0", units="Pa", zfactor_values=data["p0"])

    ips = cmor.zfactor(
        zaxis_id=lev, zfactor_name="ps", axis_ids=[time, lat, lon], units="Pa"
    )

    return ips
