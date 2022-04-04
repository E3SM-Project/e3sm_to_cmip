from __future__ import absolute_import, division, print_function, unicode_literals

import argparse
import imp
import json
import os
import re
import sys
import traceback
from pathlib import Path
from pprint import pprint
from typing import Any, Dict, List

import cmor
import xarray as xr
import yaml
from tqdm import tqdm

from e3sm_to_cmip import resources
from e3sm_to_cmip.version import __version__

ATMOS_TABLES = [
    "CMIP6_Amon.json",
    "CMIP6_day.json",
    "CMIP6_3hr.json",
    "CMIP6_6hrLev.json",
    "CMIP6_6hrPlev.json",
    "CMIP6_6hrPlevPt.json",
    "CMIP6_AERmon.json",
    "CMIP6_AERday.json",
    "CMIP6_AERhr.json",
    "CMIP6_CFmon.json",
    "CMIP6_CF3hr.json",
    "CMIP6_CFday.json",
    "CMIP6_fx.json",
]

LAND_TABLES = ["CMIP6_Lmon.json", "CMIP6_LImon.json"]
OCEAN_TABLES = ["CMIP6_Omon.json", "CMIP6_Ofx.json"]
SEAICE_TABLES = ["CMIP6_SImon.json"]


FREQUENCIES = {
    "regular": ("mon",),
    "high": ("day", "6hrLev", "6hrPlev", "6hrPlevPt", "3hr", "1hr"),
}

# Variables that have both a self-named and "_highfreq" handler.
HIGHFREQ_VARS = ["pr", "rlut"]


def print_debug(e):
    _, _, tb = sys.exc_info()
    traceback.print_tb(tb)
    print(e)


# ------------------------------------------------------------------


class colors:
    HEADER = "\033[95m"
    OKBLUE = "\033[94m"
    OKGREEN = "\033[92m"
    WARNING = "\033[93m"
    FAIL = "\033[91m"
    ENDC = "\033[0m"
    BOLD = "\033[1m"
    UNDERLINE = "\033[4m"


# ------------------------------------------------------------------


def print_message(message, status="error"):
    """
    Prints a message with either a green + or a red -

    Parameters:
        message (str): the message to print
        status (str): th"""
    if status == "error":
        print(
            colors.FAIL
            + "[-] "
            + colors.ENDC
            + colors.BOLD
            + str(message)
            + colors.ENDC
        )
    elif status == "ok":
        print(colors.OKGREEN + "[+] " + colors.ENDC + str(message))
    elif status == "debug":
        print(
            colors.OKBLUE
            + "[*] "
            + colors.ENDC
            + str(message)
            + colors.OKBLUE
            + colors.ENDC
        )


# ------------------------------------------------------------------


def setup_cmor(var_name, table_path, table_name, user_input_path):
    """
    Sets up cmor and logging for a single handler
    """
    var_name = str(var_name)
    table_path = str(table_path)
    table_name = str(table_name)
    user_input_path = str(user_input_path)

    logfile = os.path.join(os.getcwd(), "logs")
    if not os.path.exists(logfile):
        os.makedirs(logfile)

    logfile = os.path.join(logfile, var_name + ".log")
    cmor.setup(inpath=table_path, netcdf_file_action=cmor.CMOR_REPLACE, logfile=logfile)

    cmor.dataset_json(user_input_path)
    cmor.load_table(table_name)


# ------------------------------------------------------------------


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Convert ESM model output into CMIP compatible format",
        prog="e3sm_to_cmip",
        usage="%(prog)s [-h]",
    )
    # TODO: Add a check for valid variables
    parser.add_argument(
        "-v",
        "--var-list",
        nargs="+",
        required=True,
        metavar="",
        help="Space separated list of variables to convert from e3sm to cmip. Use 'all' to convert all variables or the name of a CMIP6 table to run all handlers from that table",
    )
    parser.add_argument(
        "-i",
        "--input-path",
        metavar="",
        help="Path to directory containing e3sm time series data files. Additionally namelist, restart, and mappings files if handling MPAS data.",
    )
    parser.add_argument(
        "-o", "--output-path", metavar="", help="Where to store cmorized output"
    )
    parser.add_argument(
        "--simple",
        help="Perform a simple translation of the E3SM output to CMIP format, but without the CMIP6 metadata checks",
        action="store_true",
    )
    parser.add_argument(
        "-f",
        "--freq",
        help="The frequency of that data, default is monthly. Accepted values are mon, day, 6hrLev, 6hrPlev, 6hrPlevPt, 3hr, 1hr",
        default="mon",
    )
    parser.add_argument(
        "-u",
        "--user-metadata",
        metavar="<user_input_json_path>",
        help="Path to user json file for CMIP6 metadata, required unless the --simple flag is used",
    )
    parser.add_argument(
        "-t",
        "--tables-path",
        metavar="<tables-path>",
        help="Path to directory containing CMOR Tables directory, required unless the --simple flag is used",
    )
    parser.add_argument(
        "--map",
        metavar="<map_mpas_to_std_grid>",
        help="The path to an mpas remapping file. Required if realm is mpaso or mpassi",
    )
    parser.add_argument(
        "-n",
        "--num-proc",
        metavar="<nproc>",
        default=6,
        type=int,
        help="optional: number of processes, default = 6",
    )
    parser.add_argument(
        "-H",
        "--handlers",
        metavar="<handler_path>",
        default=None,
        help="Path to cmor handlers directory, default = e3sm_to_cmip/cmor_handlers",
    )
    parser.add_argument(
        "--custom-metadata",
        help="the path to a json file with additional custom metadata to add to the output files",
    )
    parser.add_argument(
        "-s",
        "--serial",
        help="Run in serial mode, usefull for debugging purposes",
        action="store_true",
    )
    parser.add_argument(
        "--debug", help="Set output level to debug", action="store_true"
    )
    parser.add_argument(
        "--realm",
        metavar="<realm>",
        default="atm",
        help="The realm to process, atm, lnd, mpaso or mpassi. Default is atm",
    )
    parser.add_argument(
        "--logdir",
        default="./cmor_logs",
        help="Where to put the logging output from CMOR",
    )
    parser.add_argument(
        "--timeout",
        help="Exit with code -1 if execution time exceeds given time in seconds",
    )
    parser.add_argument(
        "--precheck",
        type=str,
        help="Check for each variable if its already in the output CMIP6 directory, only run variables that dont have CMIP6 output",
    )
    parser.add_argument(
        "--info",
        action="store_true",
        help="""Print information about the variables passed in the --var-list argument and exit without doing any processing.
There are three modes for getting the info, if you just pass the --info flag with the --var-list then it will print out the information for the requested variable.
If the --freq <frequency> is passed along with the --tables-path, then the CMIP6 tables will get checked to see if the requested variables are present in the CMIP6 table matching the freq.
If the --freq <freq> is passed with the --tables-path, and the --input-path, and the input-path points to raw unprocessed E3SM files, then an additional check will me made for if the required raw
variables are present in the E3SM output.""",
    )
    parser.add_argument(
        "--info-out",
        type=str,
        help="If passed with the --info flag, will cause the variable info to be written out to the specified file path as yaml",
    )
    parser.add_argument(
        "--version",
        help="print the version number and exit",
        action="version",
        version="%(prog)s {}".format(__version__),
    )
    try:
        _args = sys.argv[1:]
    except (Exception, BaseException):
        parser.print_help()
        sys.exit(1)
    else:
        _args = parser.parse_args(_args)
        if _args.realm == "mpaso" and not _args.map:
            raise ValueError("MPAS ocean handling requires a map file")
        if _args.realm == "mpassi" and not _args.map:
            raise ValueError("MPAS sea-ice handling requires a map file")
        if not _args.simple and not _args.tables_path and not _args.info:
            raise ValueError(
                "Running without the --simple flag requires CMIP6 tables path"
            )
        if (not _args.input_path or not _args.output_path) and not _args.info:
            raise ValueError("Input and output paths required")
        if not _args.simple and not _args.user_metadata and not _args.info:
            raise ValueError(
                "Running without the --simple flag requires CMIP6 metadata json file"
            )

        valid_freqs = [freq for freq_type in FREQUENCIES.values() for freq in freq_type]
        if _args.freq and _args.freq not in valid_freqs:
            raise ValueError(
                f"Frequency set to {_args.freq} which is not in the set of allowed "
                "frequencies: {', '.join(valid_freqs)}"
            )

    return _args


# ------------------------------------------------------------------


def print_var_info(handlers, freq=None, inpath=None, tables=None, outpath=None):

    messages = []

    # if the user just asked for the handler info
    if freq == "mon" and not inpath and not tables:
        for handler in handlers:
            msg = {
                "CMIP6 Name": handler["name"],
                "CMIP6 Table": handler["table"],
                "CMIP6 Units": handler["units"],
                "E3SM Variables": ", ".join(handler["raw_variables"]),
            }
            if handler.get("unit_conversion"):
                msg["Unit conversion"] = handler["unit_conversion"]
            if handler.get("levels"):
                msg["Levels"] = handler["levels"]
            messages.append(msg)

    # if the user asked if the variable is included in the table
    # but didnt ask about the files in the inpath
    elif freq and tables and not inpath:
        for handler in handlers:
            table_info = _get_table_info(tables, handler["table"])
            if handler["name"] not in table_info["variable_entry"]:
                msg = f"Variable {handler['name']} is not included in the table {handler['table']}"
                print_message(msg, status="error")
                continue
            else:
                msg = {
                    "CMIP6 Name": handler["name"],
                    "CMIP6 Table": handler["table"],
                    "CMIP6 Units": handler["units"],
                    "E3SM Variables": ", ".join(handler["raw_variables"]),
                }
                if handler.get("unit_conversion"):
                    msg["Unit conversion"] = handler["unit_conversion"]
                if handler.get("levels"):
                    msg["Levels"] = handler["levels"]
                messages.append(msg)

    elif freq and tables and inpath:
        file_path = next(Path(inpath).glob("*.nc"))

        resource_path, _ = os.path.split(os.path.abspath(resources.__file__))
        defaults_path = os.path.join(resource_path, "default_handler_info.yaml")

        with open(defaults_path, "r") as infile:
            default_info = yaml.load(infile, Loader=yaml.SafeLoader)

        with xr.open_dataset(file_path) as ds:
            for handler in handlers:

                table_info = _get_table_info(tables, handler["table"])
                if handler["name"] not in table_info["variable_entry"]:
                    continue

                msg = None
                raw_vars = []
                for default in default_info:
                    # NOTE: This returns the legal CMIP6 info by using the
                    # default handler instead of the e2c "_highfreq" convention
                    if (
                        handler["name"] + "_highfreq" == default["cmip_name"]
                        and freq in default["table"]
                    ):
                        msg = {
                            "CMIP6 Name": default["cmip_name"],
                            "CMIP6 Table": default["table"],
                            "CMIP6 Units": default["units"],
                            "E3SM Variables": [default["e3sm_name"]],
                        }
                        raw_vars.append(default["e3sm_name"])
                        break
                if msg is None:
                    msg = {
                        "CMIP6 Name": handler["name"],
                        "CMIP6 Table": handler["table"],
                        "CMIP6 Units": handler["units"],
                        "E3SM Variables": ", ".join(handler["raw_variables"]),
                    }
                    raw_vars.extend(handler["raw_variables"])
                if handler.get("unit_conversion"):
                    msg["Unit conversion"] = handler["unit_conversion"]
                if handler.get("levels"):
                    msg["Levels"] = handler["levels"]

                has_vars = True
                for raw_var in raw_vars:

                    if raw_var not in ds.data_vars:
                        has_vars = False
                        msg = f"Variable {handler['name']} is not present in the input dataset"
                        print_message(msg, status="error")
                        break
                if not has_vars:
                    continue
                messages.append(msg)

    if outpath is not None:
        with open(outpath, "w") as outstream:
            yaml.dump(messages, outstream)
    else:
        pprint(messages)


# ------------------------------------------------------------------


def load_handlers(
    handlers_path: str,
    tables_path: str,
    var_list: List[str],
    freq: str = "mon",
    realm: str = "atm",
    simple: bool = False,
) -> List[Dict[str, Any]]:
    """Loads default and/or complex handlers based on a list of variable names.

    If this function is called with `simple=True`, then the `freq` argument
    is not considered. This means that the base table is used (usually monthly).

    Parameters
    ----------
    handlers_path : str
        The path to the complex handlers, which are Python modules.
    tables_path : str
        The path to the CMIP6 tables.
    var_list : List[str]
        The list of variables to CMOR-ize.
    freq : str, optional
        The frequency, by default "mon".
    realm : str, optional
        The realm, by default "atm".
    simple : bool, optional
        If True, run in simple mode, by default False

    Returns
    -------
    List[Dict[str, str]]:
        A list of dictionaries, with each dictionary storing information related
        to the CMOR handler of each variable.
    """
    available_handlers = _get_available_handlers(handlers_path)
    chosen_handlers: List[Dict[str, Any]] = []

    if "all" in var_list:
        for key, handler in available_handlers.items():
            # Update the handler's "table" if the frequency is not monthly.
            if not simple and freq != "mon":
                available_handlers[key]["table"] = _get_table_for_non_monthly_freq(
                    handler["cmip_name"], handler["table"], freq, realm, tables_path
                )

        print_message(
            f"Loaded all available handlers with a frequency of {freq}.",
            "debug",
        )
        chosen_handlers = [handler for handler in available_handlers.values()]

        return chosen_handlers

    for var in var_list:
        # Use the high frequency version of a variable handler based on the
        # specified frequency. For example, for "pr" with a frequency of "day"
        # or "3hr, use "pr_highfreq.py" instead of "pr.py".
        # TODO: Once the handler modules are refactored using classes, we can
        # update this check to something more sophisticated.
        if _use_highfreq_handler(var, freq):
            var += "_highfreq"

        handler = available_handlers.get(var)
        if handler is None:
            raise KeyError(
                f"Variable '{var}' failed to load. Either an entry is not defined in "
                "the default handlers file, or a Python module of the same name does "
                "not exist."
            )

        # Update the handler's "table" if the frequency is not monthly.
        if not simple and freq != "mon":
            handler["table"] = _get_table_for_non_monthly_freq(
                handler["cmip_name"], handler["table"], freq, realm, tables_path
            )

        chosen_handlers.append(handler)
        print_message(
            f"Loaded `{var}` handler for variable `{handler['cmip_name']}`.", "debug"
        )

    return chosen_handlers


def _get_available_handlers(handlers_path: str) -> Dict[str, Dict[str, str]]:
    """Loads default and/or complex handlers based on a list of variable names.

    Parameters
    ----------
    handlers_path : str
        The path to the complex handlers, which are Python modules.

    Returns
    -------
    Dict[str, Dict[str, str]]
        A dictionary of dictionaries, with each dictionary storing the
        information for a complex or default handler.
    """
    default_handlers = _get_default_handlers()
    complex_handlers = _get_complex_handlers(handlers_path)
    all_handlers = {**default_handlers, **complex_handlers}

    return all_handlers


def _get_default_handlers() -> Dict[str, Dict[str, str]]:
    """Gets the information for all default handlers in a nested dictionary.

    The parent key is the CMIP name of the variable, and the value is a nested
    dictionary storing key/value pairs for "cmip_name", "units", "table",
    "method", "raw_variables", and "unit_conversion" (optional).

    Example output:
    ---------------
    {
        "abs550aer": {
            "cmip_name": "abs550aer",
            "units": "1",
            "table": "CMIP6_AERmon.json",
            "method": <function e3sm_to_cmip.default.default_handler(infiles, tables, user_input_path, **kwargs)>
            "raw_variables": ["AODABS"],
        },
    }

    Returns
    -------
    Dict[str, Dict[str, str]]
        A dictionary of dictionaries, with each dictionary storing the
        information for a default handler.
    """
    # FIXME: Imports should be at the module level. However, moving this to the
    # module level results in a circular import.
    from e3sm_to_cmip.default import default_handler

    resources_path = os.path.split(os.path.abspath(resources.__file__))[0]
    defaults_path = os.path.join(resources_path, "default_handler_info.yaml")

    with open(defaults_path, "r") as infile:
        default_handlers = yaml.load(infile, Loader=yaml.SafeLoader)

    # Convert the list of dicts to a nested dict, with the key being the
    # "cmip_name" (variable name).
    default_handlers = {handler["cmip_name"]: handler for handler in default_handlers}

    # Post-process the loaded default handlers to align with the example
    # dictionary structure.
    for key in default_handlers.keys():
        # Add a "method" key with a value of default_handler to each nested dict.
        default_handlers[key]["method"] = default_handler
        # The "raw_variables" entry should be a list of strings.
        default_handlers[key]["raw_variables"] = [
            default_handlers[key].pop("e3sm_name")
        ]

    return default_handlers


def _get_complex_handlers(handlers_path: str) -> Dict[str, Dict[str, str]]:
    """Gets the information for all complex handlers in a nested dictionary.

    The parent key is the CMIP name of the variable, and the value is a nested
    dictionary storing key/value pairs for "cmip_name", "units", "table",
    "method", "raw_variables", "positive" (optional), and "levels" (optional).

    Example output:
    ---------------
    {
        "cesm_mmrbc": {
            "cmip_name": "mmrbc",
            "units": "kg kg-1",
            "table": "CMIP6_AERmon.json",
            "method": "<function cesm_mmrbc.handle(infiles, tables, user_input_path, **kwargs)>",
            "raw_variables": ["bc_a1", "bc_a4", "bc_c1", "bc_c4"],
            "positive": None,
            "levels": {
                "name": "standard_hybrid_sigma",
                "units": "1",
                "e3sm_axis_name": "lev",
                "e3sm_axis_bnds": "ilev",
            },
        }
    }

    Parameters
    ----------
    handlers_path : str
        The path to the complex handlers, which are Python modules.

    Returns
    -------
    Dict[str, Dict[str, str]]
        A dictionary of dictionaries, with each dictionary storing the
        information for a complex handler.
    """
    complex_handlers = {}

    for root, _, files in os.walk(handlers_path):
        for file in files:
            if file.endswith(".py") and file != "__init__.py":
                var = file.split(".")[0]
                filepath = os.path.join(root, file)
                module = imp.load_source(var, filepath)

                complex_handlers[var] = {
                    "cmip_name": module.VAR_NAME,
                    "units": module.VAR_UNITS,
                    "table": module.TABLE,
                    "method": module.handle,
                    "raw_variables": module.RAW_VARIABLES,
                    "positive": module.POSITIVE
                    if hasattr(module, "POSITIVE")
                    else None,
                    "levels": module.LEVELS if hasattr(module, "LEVELS") else None,
                }

    return complex_handlers


def _get_table_for_non_monthly_freq(
    var: str, base_table: str, freq: str, realm: str, tables_path: str
) -> str:
    """Get the table for the handler based on the frequency.

    This function finds the correct table based on the frequency, starting from
    the base table (for monthly data). It checks that the tables directory
    includes the table, and that the the variable is included in the table.

    Parameters
    ----------
    var : str
        The variable.
    base_table : str
        The name of the base table, usually monthly (e.g., "CMIP6_Amon").
    freq : str
        The frequency.
    realm : str
        The realm.
    tables_path : str
        The path to the CMIP6 tables.

    Returns
    -------
    str
        The handler's table for the frequency (e.g., "CMIP6_3hr.json).

    Raises
    ------
    ValueError
        If no table exists for the variable at the specified frequency.
    ValueError
        If the path for the table could not be found for the variable at the
        specified frequency.
    KeyError
        If the variable is not included in the table.
    ValueError
        If the table is not supported by the realm.
    """
    # Get the table for the frequency
    # Example: "CMIP6_3hr.json" for freq="3hr"
    table_for_freq = _get_table_for_freq(base_table, freq)
    if table_for_freq is None:
        raise ValueError(
            f"No table exists for frequency '{freq}' with base table of {base_table}."
        )

    # Get the absolute path to the table for the frequency.
    # Example: "/home/user/PCMDI/cmip6-cmor-tables/Tables/CMIP6_6hr.json"
    table_path = Path(tables_path, table_for_freq)
    if not table_path.exists():
        raise ValueError(
            f"Table `{table_for_freq}` does not exist in `{tables_path}`."
        )

    # Set table to the name of the table file
    # Example: "CMIP6_3hr.json"
    table = table_path.name

    # Check that the variable is included in the table.
    table_data = _get_table_info(tables_path, table_for_freq)
    if var not in table_data["variable_entry"].keys():
        raise KeyError(f"Variable {var} is not included in table `{table}`.")

    # Check if the table is supported by the realm.
    if not _is_table_supported_by_realm(table, realm):
        raise ValueError(f"Table (`{table}`) is not supported by realm {realm}.")

    return table


def _get_table_for_freq(base_table: str, freq: str) -> str:
    """Get the table for the frequency.

    Parameters
    ----------
    base_table : str
        The name of the base table, usually monthly (e.g., "CMIP_Amon").
    freq : str
        The frequency.

    Returns
    -------
    str
        The table for the frequency.
    """
    # Replace the base table (monthly) with the frequency.
    if base_table == "CMIP6_Amon.json":
        return f"CMIP6_{freq}.json"
    # Tables not supported for frequencies other than month.
    elif base_table in ["CMIP6_Lmon.json", "CMIP6_LImon.json"] and freq != "mon":
        return None
    # "fx" tables not supported for frequencies other than month.
    elif "fx" in base_table and freq != "mon":
        return None

    return f"{base_table[:-8]}{freq}.json"


def _get_table_info(tables, table):
    table = Path(tables, table)
    if not table.exists():
        raise ValueError(f"CMIP6 table doesnt exist: {table}")
    with open(table, "r") as instream:
        return json.load(instream)


def _use_highfreq_handler(var: str, freq: str) -> bool:
    """
    Use the high frequency version of a variable handler based on the
    specified frequency.

    For example, for "pr" with a frequency of "day", use the handler
    "pr_highfreq.py" instead of "pr.py".

    Parameters
    ----------
    var : str
        The variable.
    freq : str
        The frequency.

    Returns
    -------
    bool
        True if high frequency applies to variable, otherwise False.
    """
    return var in HIGHFREQ_VARS and freq in FREQUENCIES["high"]


def _is_table_supported_by_realm(table: str, realm: str) -> bool:
    """Check if the table is supported by the realm.

    Parameters
    ----------
    table : str
        The name of the table (e.g., "CMIP_3hr.json").
    realm : str
        The realm.

    Returns
    -------
    bool
        If the table is in the realm's table or not.
    """
    in_tables = (
        (realm == "atm" and table in ATMOS_TABLES)
        or (realm == "lnd" and table in LAND_TABLES)
        or (realm == "ocn" and table in OCEAN_TABLES)
        or (realm == "ice" and table in SEAICE_TABLES)
    )
    return in_tables


def copy_user_metadata(input_path, output_path):
    """
    write out the users input file for cmor into the output directory
    and replace the output line with the path to the output directory

    Params:
    -------
        input_path (str): A path to the users original cmor metadata json file
        output_path (str): The new path for the updated metadata file with the output path changed
    """
    try:
        fin = open(input_path, "r")
    except IOError as error:
        print("Unable to write out metadata")
        raise error
    try:
        fout = open(os.path.join(output_path, "user_metadata.json"), "w")
    except IOError as error:
        print("Unable to open output location for custom user metadata")
        raise error
    try:
        for line in fin:
            if "outpath" in line:
                fout.write(f'\t"outpath": "{output_path}",\n')
            else:
                fout.write(line)

    except IOError as error:
        print("Write failure for user metadata")
        raise error
    finally:
        fin.close()
        fout.close()


# ------------------------------------------------------------------


def add_metadata(file_path, var_list, metadata_path):
    """
    Recurses down a file tree, adding metadata to any netcdf files in the tree
    that are on the variable list.

    Parameters
    ----------
        file_path (str): the root directory to search for files under
        var_list (list(str)): a list of cmip6 variable names
    """

    def filter_variables(file_path, var_list):
        for root, _, files in os.walk(file_path, topdown=False):
            for name in files:
                if name[-3:] != ".nc":
                    continue
                index = name.find("_")
                if index != -1 and name[:index] in var_list or "all" in var_list:
                    yield os.path.join(root, name)

    with open(metadata_path, "r") as instream:
        if metadata_path.endswith("json"):
            metadata = json.load(instream)
        elif metadata_path.endswith("yaml"):
            metadata = yaml.load(instream, Loader=yaml.SafeLoader)
        else:
            raise ValueError(
                f"custom metadata file {metadata_path} is not a json or yaml document"
            )

    for filepath in tqdm(
        filter_variables(file_path, var_list),
        desc="Adding additional metadata to output files",
    ):
        ds = xr.open_dataset(filepath, decode_times=False)
        for key, value in metadata.items():
            ds.attrs[key] = value
        ds.to_netcdf(filepath)


# ------------------------------------------------------------------


def find_atm_files(var, path):
    """
    Looks in the given path for all files that match that match VAR_\d{6}_\d{6}.nc

    Params:
    -------
        var (str): the name of the variable to look for
        path (str): the path of the directory to look in
    Returns:
    --------
        files (list(str)): A list of paths to the matching files
    """
    pattern = var + r"\_\d{6}\_\d{6}\.nc"
    for item in os.listdir(path):
        if re.match(pattern, item):
            yield item


# ------------------------------------------------------------------


def find_mpas_files(component, path, map_path=None):
    """
    Looks in the path given for MPAS monthly-averaged files

    Params:
    -------
        component (str): Either the mpaso or mpassi component name or variable name
        path (str): The path of the directory to search for files in
    """
    # save original in case it's an atm var
    var = str(component)
    component = component.lower()
    contents = os.listdir(path)

    if component == "mpaso":

        pattern = r"mpaso.hist.am.timeSeriesStatsMonthly.\d{4}-\d{2}-\d{2}.nc"
        results = [
            os.path.join(path, x)
            for x in contents
            if re.match(pattern=pattern, string=x)
        ]
        return sorted(results)

    if component == "mpassi":
        patterns = [
            r"mpassi.hist.am.timeSeriesStatsMonthly.\d{4}-\d{2}-\d{2}.nc",
            r"mpascice.hist.am.timeSeriesStatsMonthly.\d{4}-\d{2}-\d{2}.nc",
        ]
        for pattern in patterns:
            results = [
                os.path.join(path, x)
                for x in contents
                if re.match(pattern=pattern, string=x)
            ]
            if results:
                return sorted(results)
        raise IOError("Unable to find mpassi in the input directory")

    elif component == "mpaso_namelist":

        patterns = ["mpaso_in", "mpas-o_in"]
        for pattern in patterns:
            for infile in contents:
                if re.match(pattern, infile):
                    return os.path.abspath(os.path.join(path, infile))
        raise IOError("Unable to find MPASO_namelist in the input directory")

    elif component == "mpassi_namelist":

        patterns = ["mpassi_in", "mpas-cice_in"]
        for pattern in patterns:
            for infile in contents:
                if re.match(pattern, infile):
                    return os.path.abspath(os.path.join(path, infile))
        raise IOError("Unable to find MPASSI_namelist in the input directory")

    elif component == "mpas_mesh":

        pattern = r"mpaso.rst.\d{4}-\d{2}-\d{2}_\d{5}.nc"
        for infile in contents:
            if re.match(pattern, infile):
                return os.path.abspath(os.path.join(path, infile))
        raise IOError("Unable to find mpas_mesh in the input directory")

    elif component == "mpas_map":
        if not map_path:
            raise ValueError("No map path given")
        map_path = os.path.abspath(map_path)
        if os.path.exists(map_path):
            return map_path
        else:
            raise IOError("Unable to find mpas_map in the input directory")

    elif component == "mpaso_moc_regions":

        pattern = "_region_"
        for infile in contents:
            if pattern in infile:
                return os.path.abspath(os.path.join(path, infile))
        raise IOError("Unable to find mpaso_moc_regions in the input directory")

    else:
        files = [x for x in find_atm_files(var, path)]
        if len(files) > 0:
            files = [os.path.join(path, name) for name in files]
            return files
        else:
            raise ValueError(
                f"Unrecognized component {component}, unable to find input files"
            )


# ------------------------------------------------------------------


def terminate(pool, debug=False):
    """
    Terminates the process pool

    Params:
    -------
        pool (multiprocessing.Pool): the pool to shutdown
        debug (bool): if we're running in debug mode
    Returns:
    --------
        None
    """
    if debug:
        print_message("Shutting down process pool", "debug")
    pool.shutdown()


# ------------------------------------------------------------------


def get_levgrnd_bnds():
    return [
        0,
        0.01751106046140194,
        0.045087261125445366,
        0.09055273048579693,
        0.16551261954009533,
        0.28910057805478573,
        0.4928626772016287,
        0.8288095649331808,
        1.3826923426240683,
        2.2958906944841146,
        3.801500206813216,
        6.28383076749742,
        10.376501685008407,
        17.124175196513534,
        28.249208575114608,
        42.098968505859375,
    ]


# ------------------------------------------------------------------


def get_years_from_raw(path, realm, var):
    """
    given a file path, return the start and end years for the data
    Parameters:
    -----------
        path (str): the directory to look in for data
        realm (str): the type of data to look for, i.e atm, lnd, mpaso, mpassi
    """
    start = 0
    end = 0
    if realm in ["atm", "lnd"]:
        contents = sorted(
            [f for f in os.listdir(path) if f.endswith("nc") and var in f]
        )
        p = var + r"\d{6}_\d{6}.nc"
        s = re.match(pattern=p, string=contents[0])
        start = int(contents[0][s.start() : s.start() + 4])
        s = re.search(pattern=p, string=contents[-1])
        end = int(contents[-1][s.start() : s.start() + 4])
    elif realm in ["mpassi", "mpaso"]:

        files = sorted(find_mpas_files(realm, path))
        p = r"\d{4}-\d{2}-\d{2}.nc"
        s = re.search(pattern=p, string=files[0])
        start = int(files[0][s.start() : s.start() + 4])
        s = re.search(pattern=p, string=files[1])
        end = int(files[-1][s.start() : s.start() + 4])

    else:
        raise ValueError("Invalid realm")
    return start, end


def get_year_from_cmip(filename):
    """
    Given a file name, assuming its a cmip file, return the start and end year
    """
    p = r"\d{6}-\d{6}\.nc"
    s = re.search(pattern=p, string=filename)
    if not s:
        raise ValueError("unable to match file years for {}".format(filename))

    start, end = [
        int(x[:-2]) if not x.endswith(".nc") else int(x[:-5])
        for x in filename[s.span()[0] : s.span()[1]].split("-")
    ]
    return start, end


def precheck(inpath, precheck_path, variables, realm):
    """
    Check if the data has already been produced and skip

    returns a list of variable names that were not found in the output directory with matching years
    """

    # First check the inpath for the start and end years
    start, end = get_years_from_raw(inpath, realm, variables[0])
    var_map = [{"found": False, "name": var} for var in variables]

    # then check the output tree for files with the correct variables for those years
    for val in var_map:
        for _, _, files in os.walk(precheck_path, topdown=False):
            if files:
                prefix = val["name"] + "_"
                if files[0][: len(prefix)] != prefix:
                    # this directory doesnt have the variable we're looking for
                    continue

                files = [x for x in sorted(files) if x.endswith(".nc")]
                for f in files:
                    cmip_start, cmip_end = get_year_from_cmip(f)
                    if cmip_start == start and cmip_end == end:
                        val["found"] = True
                        break
                if val["found"] == True:
                    break

    return [x["name"] for x in var_map if not x["found"]]
