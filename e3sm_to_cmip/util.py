from __future__ import absolute_import, division, print_function, unicode_literals

import json
import os
import re
import sys
import traceback
from pathlib import Path
from pprint import pprint
from typing import Optional

import cmor
import xarray as xr
import yaml
from tqdm import tqdm

from e3sm_to_cmip._logger import _setup_child_logger

logger = _setup_child_logger(__name__)

FREQUENCIES = ["mon", "day", "6hrLev", "6hrPlev", "6hrPlevPt", "3hr", "1hr"]
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

# Mapping from CMIP6 table names to their associated frequency.
# This dictionary helps determine the frequency category (e.g., "mon", "day", etc.)
# for a given CMIP6 table name when deriving or validating variable handlers.
# Keys are CMIP6 table filenames, values are their frequency strings.
FREQUENCY_TO_CMIP_TABLES = {
    "mon": [
        "CMIP6_Amon.json",
        "CMIP6_Lmon.json",
        "CMIP6_LImon.json",
        "CMIP6_fx.json",
        "CMIP6_Ofx.json",
        "CMIP6_CFmon.json",
        "CMIP6_AERmon.json",
        "CMIP6_Omon.json",
        "CMIP6_SImon.json",
    ],
    "day": [
        "CMIP6_day.json",
        "CMIP6_CFday.json",
        "CMIP6_AERday.json",
    ],
    "3hr": [
        "CMIP6_3hr.json",
        "CMIP6_CF3hr.json",
    ],
    "6hrLev": [
        "CMIP6_6hrLev.json",
    ],
    "6hrPlev": [
        "CMIP6_6hrPlev.json",
    ],
    "6hrPlevPt": [
        "CMIP6_6hrPlevPt.json",
    ],
    "1hr": [
        "CMIP6_AERhr.json",
    ],
}


def print_debug(e):
    # TODO: Deprecate this function. We use Python logger now.
    _, _, tb = sys.exc_info()
    traceback.print_tb(tb)
    print(e)


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

    # TODO: Deprecate this function. We use Python logger now. Colors can't
    # be captured in log files.

    Parameters:
        message (str): the message to print
        status (str): the status message.
    """

    if status == "error":
        logger.error(
            colors.FAIL
            + "[-] "
            + colors.ENDC
            + colors.BOLD
            + str(message)
            + colors.ENDC
        )
    elif status == "ok":
        logger.info(colors.OKGREEN + "[+] " + colors.ENDC + str(message))
    elif status == "info":
        logger.info(str(message))
    elif status == "debug":
        logger.info(
            colors.OKBLUE
            + "[*] "
            + colors.ENDC
            + str(message)
            + colors.OKBLUE
            + colors.ENDC
        )


# ------------------------------------------------------------------


def setup_cmor(var_name, table_path, table_name, user_input_path, cmor_log_dir):
    """
    Sets up cmor and logging for a single handler.

    NOTE: This function is only used by the MPAS variable handlers defined
    under ``e3sm_to_cmip/cmor_handlers/mpas_vars`` and legacy handlers
    defined under ``e3sm_to_cmip/cmor_handlers/vars``.
    """
    logfile = os.path.join(cmor_log_dir, var_name + ".log")
    # NOTE: Could add "set_verbosity=cmor.CMOR_DEBUG" as an optional parameter here
    cmor.setup(inpath=table_path, netcdf_file_action=cmor.CMOR_REPLACE, logfile=logfile)

    cmor.dataset_json(user_input_path)
    cmor.load_table(table_name)


# ------------------------------------------------------------------


def print_var_info(  # noqa: C901
    handlers, freq=None, inpath=None, tables=None, outpath=None
):
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
                msg = f"Variable {handler['name']} is not included in the table {handler['table']}"  # type: ignore
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

        with xr.open_dataset(file_path) as ds:
            for handler in handlers:
                table_info = _get_table_info(tables, handler["table"])
                if handler["name"] not in table_info["variable_entry"]:
                    continue

                msg = None
                raw_vars = []

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
                        msg = f"Variable {handler['name']} is not present in the input dataset"  # type: ignore
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
        raise ValueError(f"Table `{table_for_freq}` does not exist in `{tables_path}`.")

    # Set table to the name of the table file
    # Example: "CMIP6_3hr.json"
    table = table_path.name

    # Check that the variable is included in the table.
    table_data = _get_table_info(tables_path, table_for_freq)
    if var not in table_data["variable_entry"].keys():
        raise KeyError(f"Variable '{var}' is not included in table `{table}`.")

    # Check if the table is supported by the realm.
    if not _is_table_supported_by_realm(table, realm):
        raise ValueError(f"Table (`{table}`) is not supported by realm {realm}.")

    return table


def _get_table_for_freq(base_table: str, freq: str) -> Optional[str]:
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


def get_handler_info_msg(handler):
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
    return msg


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
    r"""
    Looks in the given path for all files that match that match VAR_\d{6}_\d{6}.nc  # noqa: W605

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


def find_mpas_files(component, path, map_path=None):  # noqa: C901
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

    logger.info(f"find_mpas_files: component = {component}, path = {path}")

    if component == "mpaso":
        pattern = r".*mpaso.hist.am.timeSeriesStatsMonthly.\d{4}-\d{2}-\d{2}.nc"
        results = [
            os.path.join(path, x)
            for x in contents
            if re.match(pattern=pattern, string=x)
        ]
        if results:
            logger.info(f"results found: {len(results)} items")
            return sorted(results)
        raise IOError("Unable to find mpaso in the input directory")

    if component == "mpassi":
        patterns = [
            r".*mpassi.hist.am.timeSeriesStatsMonthly.\d{4}-\d{2}-\d{2}.nc",
            r".*mpascice.hist.am.timeSeriesStatsMonthly.\d{4}-\d{2}-\d{2}.nc",
        ]
        for pattern in patterns:
            results = [
                os.path.join(path, x)
                for x in contents
                if re.match(pattern=pattern, string=x)
            ]
            if results:
                logger.info(f"results found: {len(results)} items")
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
        pattern = r".*mpaso.rst.\d{4}-\d{2}-\d{2}_\d{5}.nc"
        for infile in contents:
            if re.match(pattern, infile):
                logger.info(f"component mpas_mesh found: {infile}")
                return os.path.abspath(os.path.join(path, infile))
        raise IOError("Unable to find mpas_mesh in the input directory")

    elif component == "mpas_map":
        if not map_path:
            raise ValueError("No map path given")
        map_path = os.path.abspath(map_path)
        if os.path.exists(map_path):
            logger.info(f"component mpas_map found: {map_path}")
            return map_path
        else:
            raise IOError("Unable to find mpas_map in the input directory")

    elif component == "mpaso_moc_regions":
        pattern_v1 = "_region_"
        pattern_v2 = "mocBasinsAndTransects"
        for infile in contents:
            if pattern_v1 in infile or pattern_v2 in infile:
                logger.info(f"component mpas0_moc_regions found: {infile}")
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
        start = int(contents[0][s.start() : s.start() + 4])  # type: ignore
        s = re.search(pattern=p, string=contents[-1])
        end = int(contents[-1][s.start() : s.start() + 4])  # type: ignore
    elif realm in ["mpassi", "mpaso"]:
        files = sorted(find_mpas_files(realm, path))
        p = r"\d{4}-\d{2}-\d{2}.nc"
        s = re.search(pattern=p, string=files[0])
        start = int(files[0][s.start() : s.start() + 4])  # type: ignore
        s = re.search(pattern=p, string=files[1])
        end = int(files[-1][s.start() : s.start() + 4])  # type: ignore

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

    logger.info(f"precheck: working on year-range {start} to {end}")

    # then check the output tree for files with the correct variables for those years
    for val in var_map:
        logger.info(f"precheck: testing for var {val} in path {precheck_path}")
        for _, _, files in os.walk(precheck_path, topdown=False):
            if files:
                # Seek files named <var>_<anything>
                prefix = val["name"] + "_"
                if files[0][: len(prefix)] != prefix:
                    # this directory doesnt have the variable we're looking for
                    continue

                files = [x for x in sorted(files) if x.endswith(".nc")]
                for f in files:
                    cmip_start, cmip_end = get_year_from_cmip(f)
                    if cmip_start == start and cmip_end == end:
                        logger.info(f"found file: {f}")
                        val["found"] = True
                        break
                if val["found"] is True:
                    break

    return [x["name"] for x in var_map if not x["found"]]
