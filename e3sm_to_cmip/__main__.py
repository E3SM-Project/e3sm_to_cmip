"""
A python command line tool to turn E3SM model output into CMIP6 compatable data.
"""

import argparse
import os
import signal
import sys
import tempfile
import threading
from concurrent.futures import ProcessPoolExecutor as Pool
from dataclasses import dataclass
from pathlib import Path
from pprint import pprint
from typing import List, Optional, Union

import xarray as xr
import yaml
from tqdm import tqdm

from e3sm_to_cmip import ROOT_HANDLERS_DIR, __version__, resources
from e3sm_to_cmip._logger import (
    LOG_FILENAME,
    _setup_logger,
    _update_root_logger_filepath,
)
from e3sm_to_cmip.cmor_handlers.utils import (
    MPAS_REALMS,
    REALMS,
    Frequency,
    MPASRealm,
    Realm,
    _get_mpas_handlers,
    derive_handlers,
    load_all_handlers,
)
from e3sm_to_cmip.util import (
    FREQUENCIES,
    _get_table_info,
    add_metadata,
    copy_user_metadata,
    find_atm_files,
    find_mpas_files,
    get_handler_info_msg,
    precheck,
    print_debug,
    print_message,
)

# Set up a module level logger object. This logger object is a child of the
# root logger.
logger = _setup_logger(__name__)


@dataclass
class CLIArguments:
    """A data class storing the command line arguments for e3sm_to_cmip.

    The argparse arguments are converted to this data class. It is is useful for
    type annotations and type checking, which argparse does not support.

    Refer to the `help` section of each argument in `E3SMtoCMIP._parse_args()`
    for documentation.
    """

    # Run mode settings.
    simple: bool
    serial: bool
    info: bool

    # Run settings.
    num_proc: int
    debug: bool
    timeout: int

    # CMOR settings
    var_list: List[str]
    realm: Union[Realm, MPASRealm]
    freq: Frequency

    # Path references.
    input_path: Optional[str]
    output_path: Optional[str]
    tables_path: Optional[str]
    handlers: Optional[str]
    map: Optional[str]
    info_out: Optional[str]

    precheck: Optional[str]
    logdir: Optional[str]
    user_metadata: Optional[str]
    custom_metadata: Optional[str]


class E3SMtoCMIP:
    def __init__(self, args: Optional[List[str]] = None):
        # A dictionary of command line arguments.
        parsed_args = self._parse_args(args)

        # NOTE: The order of these attributes align with class CLIArguments.
        # ======================================================================
        # Run Mode settings.
        # ======================================================================
        self.simple_mode: bool = parsed_args.simple
        self.serial_mode: bool = parsed_args.serial
        self.info_mode: bool = parsed_args.info

        # ======================================================================
        # Run settings.
        # ======================================================================
        self.num_proc: int = parsed_args.num_proc
        self.debug: bool = parsed_args.debug
        self.timeout: int = parsed_args.timeout

        # ======================================================================
        # CMOR settings.
        # ======================================================================
        self.var_list: List[str] = self._get_var_list(parsed_args.var_list)
        self.realm: Union[Realm, MPASRealm] = parsed_args.realm
        self.freq: Frequency = parsed_args.freq

        # ======================================================================
        # Paths references.
        # ======================================================================
        self.input_path: Optional[str] = parsed_args.input_path
        self.output_path: Optional[str] = parsed_args.output_path
        self.tables_path: str = self._get_tables_path(parsed_args.tables_path)
        self.handlers_path: str = self._get_handlers_path(parsed_args.handlers)
        self.map_path: Optional[str] = parsed_args.map
        self.info_out_path: Optional[str] = parsed_args.info_out
        self.precheck_path: Optional[str] = parsed_args.precheck
        self.cmor_log_dir: Optional[str] = parsed_args.logdir
        self.user_metadata: Optional[str] = parsed_args.user_metadata
        self.custom_metadata: Optional[str] = parsed_args.custom_metadata

        if self.output_path is not None:
            self.output_path = os.path.abspath(self.output_path)
            os.makedirs(self.output_path, exist_ok=True)

        # Setup directories using the CLI argument paths (e.g., output dir).
        # ======================================================================
        self._setup_dirs_with_paths()

        # Run the pre-check to determine if any of the variables have already
        # been CMORized.
        # ======================================================================
        if self.precheck_path is not None:
            self._run_precheck()

        # Setup logger information and print out e3sm_to_cmip CLI arguments.
        # ======================================================================
        logger.info("--------------------------------------")
        logger.info("| E3SM to CMIP Configuration")
        logger.info("--------------------------------------")

        config_details = {
            "Mode": (
                "Info"
                if self.info_mode
                else "Serial"
                if self.serial_mode
                else "Parallel"
            ),
            "Variable List": self.var_list,
            "Input Path": self.input_path,
            "Output Path": self.output_path,
            "Precheck Path": self.precheck_path,
            "Log Path": self.log_path,
            "CMOR Log Path": self.cmor_log_dir,
            "Frequency": self.freq,
            "Realm": self.realm,
        }

        for key, value in config_details.items():
            logger.info(f"    * {key}: {value}")

        # Load the CMOR handlers based on the realm and variable list.
        self.handlers = self._get_handlers()

    def run(self):
        # Run e3sm_to_cmip with info mode.
        # ======================================================================
        if self.info_mode:
            self._run_info_mode()
            sys.exit(0)

        # Run e3sm_to_cmip to CMORize serially or in parallel.
        # ======================================================================
        timer = None
        if self.timeout:
            timer = threading.Timer(self.timeout, self._timeout_exit)
            timer.start()

        status = self._run()

        if status != 0:
            print_message(
                f"Error running handlers: { ' '.join([x['name'] for x in self.handlers]) }"
            )
            return 1

        if self.custom_metadata:
            add_metadata(
                file_path=self.output_path,
                var_list=self.var_list,
                metadata_path=self.custom_metadata,
            )

        if timer is not None:
            timer.cancel()

        return 0

    def _get_handlers(self):
        if self.info_mode:
            handlers = load_all_handlers(self.realm, self.var_list)
        elif not self.info_mode and self.input_path is not None:
            e3sm_vars = self._get_e3sm_vars(self.input_path)
            logger.debug(f"Input dataset variables: {e3sm_vars}")

            if self.realm in REALMS:
                handlers = derive_handlers(
                    cmip_tables_path=self.tables_path,
                    cmip_vars=self.var_list,
                    e3sm_vars=e3sm_vars,
                    freq=self.freq,
                    realm=self.realm,
                )

                cmip_to_e3sm_vars = {
                    handler["name"]: handler["raw_variables"] for handler in handlers
                }

                logger.info("--------------------------------------")
                logger.info("| Derived CMIP6 Variable Handlers")
                logger.info("--------------------------------------")
                for k, v in cmip_to_e3sm_vars.items():
                    logger.info(f"    * '{k}' -> {v}")

            elif self.realm in MPAS_REALMS:
                handlers = _get_mpas_handlers(self.var_list)

            if len(handlers) == 0:
                logger.error(
                    "No CMIP6 variable handlers were derived from the variables found "
                    "in using the E3SM input datasets."
                )
                sys.exit(1)

        return handlers

    def _get_e3sm_vars(self, input_path: str) -> List[str]:
        """Gets all E3SM variables from the input files to derive CMIP variables.

        This method walks through the input file path and reads each `.nc` file
        into a xr.Dataset to retrieve the `data_vars` keys. These `data_vars` keys
        are appended to a list, which is returned.

        NOTE: This method is not used to derive CMIP variables from MPAS input
        files.

        Parameters
        ----------
        input_path: str
            The path to the input `.nc` files.

        Returns
        -------
        List[str]
            List of data variables in the input files.

        Raises
        ------
        IndexError
            If no data variables were found in the input files.
        """
        paths: List[str] = []
        e3sm_vars: List[str] = []

        for root, _, files in os.walk(input_path):
            for filename in files:
                if ".nc" in filename:
                    paths.append(str(Path(root, filename).absolute()))

        for path in paths:
            ds = xr.open_dataset(path)
            data_vars = list(ds.data_vars.keys())

            e3sm_vars = e3sm_vars + data_vars

        if len(e3sm_vars) == 0:
            raise IndexError(
                f"No variables were found in the input file(s) at '{input_path}'."
            )

        return e3sm_vars

    def _parse_args(self, args: Optional[List[str]]) -> CLIArguments:
        """Parses command line arguments.

        Parameters
        ----------
        args : Optional[List[str]]
            A list of arguments, useful for debugging purposes to simulate a
            passing arguments via the CLI.

        Returns
        -------
        CLIArguments
            A data class of parsed arguments.
        """
        argparser = self._setup_argparser()

        try:
            args_to_parse = sys.argv[1:] if args is None else args
        except (Exception, BaseException):
            argparser.print_help()
            sys.exit(1)

        # Parse the arguments and perform validation.
        parsed_args = argparser.parse_args(args_to_parse)
        self._validate_parsed_args(parsed_args)

        # Convert to this data class for type checking to work.
        final_args = CLIArguments(**vars(parsed_args))
        return final_args

    def _setup_argparser(self) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser(
            description="Convert ESM model output into CMIP compatible format.",
            prog="e3sm_to_cmip",
            usage="%(prog)s [-h]",
            add_help=False,
        )

        # Argument groups to organize the numerous arguments printed by --help.
        required = parser.add_argument_group("required arguments (general)")
        required_no_info = parser.add_argument_group(
            "required arguments (without --info)"
        )
        required_no_simple = parser.add_argument_group(
            "required arguments (without --simple)"
        )
        required_mpas = parser.add_argument_group(
            "required arguments (with --realm [mpasso|mpassi])"
        )

        optional = parser.add_argument_group("optional arguments (general)")
        optional_mode = parser.add_argument_group("optional arguments (run mode)")
        optional_run = parser.add_argument_group("optional arguments (run settings)")
        optional_info = parser.add_argument_group("optional arguments (with --info)")

        helper = parser.add_argument_group("helper arguments")

        # NOTE: The order of these arguments align with class CLIArguments.
        # ======================================================================
        # Run Mode settings.
        # ======================================================================
        optional_mode.add_argument(
            "--info",
            action="store_true",
            help=(
                "Produce information about the CMIP6 variables passed in the --var-list "
                "argument and exit without doing any processing. There are three modes "
                "for getting the info. (Mode 1) If you just pass the --info flag with the "
                "--var-list then it will print out the handler information as yaml data for "
                "the requested variable to your default output path (or to a file designated "
                "by the --info-out path). (Mode 2) If the --freq <frequency> is passed "
                "along with the --tables-path, then the variable handler information will "
                "only be output if the requested variables are present in the CMIP6 table matching the freq. "
                "NOTE: For MPAS data, one must also include --realm mpaso (or mpassi) and --map no_map. "
                "(Mode 3) For non-MPAS data, if the --freq <freq> is passed with the --tables-path, and the "
                "--input-path, and the input-path points to raw unprocessed E3SM files, "
                "then an additional check will me made for if the required raw "
                "variables are present in the E3SM native output. "
            ),
        )
        optional_mode.add_argument(
            "--simple",
            help=(
                "Perform a simple translation of the E3SM output to CMIP format, but "
                "without the CMIP6 metadata checks. (WARNING: NOT WORKING AS OF 1.8.2)"
            ),
            action="store_true",
        )
        optional_mode.add_argument(
            "-s",
            "--serial",
            help="Run in serial mode (by default parallel). Useful for debugging purposes.",
            action="store_true",
        )

        # ======================================================================
        # Run settings.
        # ======================================================================
        optional.add_argument(
            "-n",
            "--num-proc",
            type=int,
            metavar="<nproc>",
            default=6,
            help=(
                "Optional: number of processes, default = 6. Not used when -s, "
                "--serial specified."
            ),
        )
        optional.add_argument(
            "--debug", help="Set output level to debug.", action="store_true"
        )
        optional.add_argument(
            "--timeout",
            type=int,
            help="Exit with code -1 if execution time exceeds given time in seconds.",
        )

        # ======================================================================
        # CMOR settings.
        # ======================================================================
        required.add_argument(
            "-v",
            "--var-list",
            nargs="+",
            required=True,
            metavar="",
            help=(
                "Space separated list of CMIP variables to convert from E3SM to CMIP."
            ),
        )
        optional_run.add_argument(
            "--realm",
            metavar="<realm>",
            type=str,
            default="atm",
            help="The realm to process. Must be atm, lnd, mpaso or mpassi. Default is atm.",
        )
        # TODO: Use list of choices for freq.
        optional_run.add_argument(
            "-f",
            "--freq",
            type=str,
            help=(
                "The frequency of the data (default is 'mon' for monthly). Accepted "
                "values are 'mon', 'day', '6hrLev', '6hrPlev', '6hrPlevPt', '3hr', '1hr."
            ),
            default="mon",
        )

        # ======================================================================
        # Paths references.
        # ======================================================================
        required_no_info.add_argument(
            "-i",
            "--input-path",
            type=str,
            metavar="",
            help=(
                "Path to directory containing e3sm time series data files. "
                "Additionally namelist, restart, and 'region' files if handling MPAS "
                "data. Region files are available from "
                "https://web.lcrc.anl.gov/public/e3sm/inputdata/ocn/mpas-o/<mpas_mesh_name>."
            ),
        )
        required_no_simple.add_argument(
            "-o",
            "--output-path",
            type=str,
            metavar="",
            help="Where to store cmorized output.",
        )
        required_no_simple.add_argument(
            "-t",
            "--tables-path",
            metavar="<tables-path>",
            type=str,
            help=(
                "Path to directory containing CMOR Tables directory, required unless "
                "the `--simple` flag is used."
            ),
        )
        optional.add_argument(
            "-H",
            "--handlers",
            type=str,
            metavar="<handler_path>",
            help=(
                "Path to cmor handlers directory, default is the (built-in) "
                "'e3sm_to_cmip/cmor_handlers'."
            ),
        )
        required_mpas.add_argument(
            "--map",
            type=str,
            metavar="<map_mpas_to_std_grid>",
            help=(
                "The path to an mpas remapping file. Required if realm is 'mpaso' or "
                "'mpassi'.  Available from https://web.lcrc.anl.gov/public/e3sm/mapping/maps/."
            ),
        )
        optional.add_argument(
            "--precheck",
            type=str,
            help=(
                "Check for each variable if it's already in the output CMIP6 "
                "directory, only run variables that don't have pre-existing CMIP6 "
                "output."
            ),
        )
        optional.add_argument(
            "--logdir",
            type=str,
            default="cmor_logs",
            help=(
                "The sub-directory that stores the CMOR logs. This sub-directory will "
                "be stored under --output-path."
            ),
        )
        required_no_simple.add_argument(
            "-u",
            "--user-metadata",
            type=str,
            metavar="<user_input_json_path>",
            help=(
                "Path to user json file for CMIP6 metadata, required unless the "
                "`--simple` flag is used."
            ),
        )
        optional.add_argument(
            "--custom-metadata",
            type=str,
            help=(
                "The path to a json file with additional custom metadata to add to "
                "the output files."
            ),
        )
        optional_info.add_argument(
            "--info-out",
            type=str,
            help=(
                "If passed with the --info flag, will cause the variable info to be "
                "written out to the specified file path as yaml."
            ),
        )

        # ======================================================================
        # Helper arguments.
        # ======================================================================
        helper.add_argument(
            "-h",
            "--help",
            action="help",
            default=argparse.SUPPRESS,
            help="show this help message and exit",
        )

        helper.add_argument(
            "--version",
            help="Print the version number and exit.",
            action="version",
            version="%(prog)s {}".format(__version__),
        )

        return parser

    def _validate_parsed_args(self, parsed_args: argparse.Namespace):
        if parsed_args.realm == "mpaso" and not parsed_args.map:
            raise ValueError("MPAS ocean handling requires a map file")

        if parsed_args.realm == "mpassi" and not parsed_args.map:
            raise ValueError("MPAS sea-ice handling requires a map file")

        if (
            not parsed_args.simple
            and not parsed_args.tables_path
            and not parsed_args.info
        ):
            raise ValueError(
                "Running without the --simple flag requires CMIP6 tables path"
            )

        if (
            not parsed_args.input_path or not parsed_args.output_path
        ) and not parsed_args.info:
            raise ValueError("Input and output paths required")

        if (
            not parsed_args.simple
            and not parsed_args.user_metadata
            and not parsed_args.info
        ):
            raise ValueError(
                "Running without the --simple flag requires CMIP6 metadata json file"
            )

        valid_freqs = [freq for freq_type in FREQUENCIES.values() for freq in freq_type]
        if parsed_args.freq and parsed_args.freq not in valid_freqs:
            raise ValueError(
                f"Frequency set to {parsed_args.freq} which is not in the set of allowed "
                "frequencies: {', '.join(valid_freqs)}"
            )

    def _get_var_list(self, input_var_list: List[str]) -> List[str]:
        if len(input_var_list) == 1 and " " in input_var_list[0]:
            var_list = input_var_list[0].split()
        else:
            var_list = input_var_list

        var_list = [x.strip(",") for x in var_list]

        return var_list

    def _get_handlers_path(self, handlers_path: Optional[str]) -> str:
        if handlers_path is None:
            return ROOT_HANDLERS_DIR

        return os.path.abspath(handlers_path)

    def _get_tables_path(self, tables_path: Optional[str]):
        if self.simple_mode and tables_path is None:
            resource_path, _ = os.path.split(os.path.abspath(resources.__file__))

            return resource_path

        return tables_path

    def _run_precheck(self):
        """
        Runs a pre-check on the list of input CMIP variables to see which ones
        have already been CMORized.

        If all input variables have already been CMORized, return 0. Otherwise,
        return a list of variables that have not been CMORized yet.
        """
        new_var_list = precheck(
            self.input_path, self.precheck_path, self.var_list, self.realm
        )
        if not new_var_list:
            print("All variables previously computed")
            if self.output_path is not None:
                os.mkdir(os.path.join(self.output_path, "CMIP6"))
            return 0
        else:
            print_message(f"Setting up conversion for {' '.join(new_var_list)}", "ok")
            self.var_list = new_var_list

    def _setup_dirs_with_paths(self):
        """Sets up various directories and paths required for e3sm_to_cmip.

        This method initializes paths for metadata, logs, and temporary storage.
        It also updates the root logger's file path and copies user metadata
        to the output directory if not in simple mode.

        Notes
        -----
        If the environment variable `TMPDIR` is not set, a temporary directory
        is created under the output path.
        """
        self.new_metadata_path = os.path.join(self.output_path, "user_metadata.json")  # type: ignore
        self.cmor_log_dir = os.path.join(self.output_path, self.cmor_log_dir)  # type: ignore

        self.log_path = os.path.join(self.output_path, LOG_FILENAME)  # type: ignore
        _update_root_logger_filepath(self.log_path)

        # Copy the user's metadata json file with the updated output directory
        if not self.simple_mode:
            copy_user_metadata(self.user_metadata, self.output_path)

        # Setup temp storage directory
        temp_path = os.environ.get("TMPDIR")
        if temp_path is None:
            temp_path = f"{self.output_path}/tmp"
            if not os.path.exists(temp_path):
                os.makedirs(temp_path)

        tempfile.tempdir = temp_path

    def _run_info_mode(self):  # noqa: C901
        messages = []

        # if the user just asked for the handler info
        if self.freq == "mon" and not self.input_path and not self.tables_path:
            for handler in self.handlers:
                hand_msg = get_handler_info_msg(handler)
                messages.append(hand_msg)

        # if the user asked if the variable is included in the table
        # but didnt ask about the files in the inpath
        elif self.freq and self.tables_path and not self.input_path:  # info mode 2
            for handler in self.handlers:
                table_info = _get_table_info(self.tables_path, handler["table"])
                if handler["name"] not in table_info["variable_entry"]:
                    msg = f"Variable {handler['name']} is not included in the table {handler['table']}"
                    print_message(msg, status="error")
                    continue
                else:
                    if self.freq == "mon" and handler["table"] == "CMIP6_day.json":
                        continue
                    if (self.freq == "day" or self.freq == "3hr") and handler[
                        "table"
                    ] == "CMIP6_Amon.json":
                        continue
                    hand_msg = get_handler_info_msg(handler)
                    messages.append(hand_msg)

        elif self.freq and self.tables_path and self.input_path:  # info mode 3
            file_path = next(Path(self.input_path).glob("*.nc"))

            with xr.open_dataset(file_path) as ds:
                for handler in self.handlers:
                    table_info = _get_table_info(self.tables_path, handler["table"])
                    if handler["name"] not in table_info["variable_entry"]:
                        continue

                    raw_vars = handler["raw_variables"]
                    has_vars = True

                    for raw_var in raw_vars:
                        if raw_var not in ds.data_vars:
                            has_vars = False

                            msg = f"Variable {handler['name']} is not present in the input dataset"
                            print_message(msg, status="error")

                            break

                    if not has_vars:
                        continue

                    # We test here against the input "freq", because atmos mon
                    # data satisfies BOTH CMIP6_day.json AND CMIP6_mon.json, but
                    # we only want the latter in the "hand_msg" output. The vars
                    # "hass" and "rlut" have multiple freqs.
                    if self.freq == "mon" and handler["table"] == "CMIP6_day.json":
                        continue
                    if (self.freq == "day" or self.freq == "3hr") and handler[
                        "table"
                    ] == "CMIP6_Amon.json":
                        continue

                    hand_msg = None
                    stat_msg = None

                    raw_vars = []
                    raw_vars.extend(handler["raw_variables"])

                    allpass = True
                    for raw_var in raw_vars:
                        if raw_var in ds.data_vars:
                            continue
                        allpass = False

                    if allpass:
                        stat_msg = f"Table={handler['table']}:Variable={handler['name']}:DataSupport=TRUE"
                        hand_msg = get_handler_info_msg(handler)
                        messages.append(hand_msg)
                    else:
                        stat_msg = f"Table={handler['table']}:Variable={handler['name']}:DataSupport=FALSE"
                    logger.info(stat_msg)

        if self.info_out_path is not None:
            with open(self.info_out_path, "w") as outstream:
                yaml.dump(messages, outstream)
        elif self.output_path is not None:
            yaml_filepath = os.path.join(self.output_path, "info.yaml")

            with open(yaml_filepath, "w") as outstream:
                yaml.dump(messages, outstream)
        else:
            pprint(messages)

    def _run(self):
        if self.serial_mode:
            run_func = self._run_serial
        else:
            run_func = self._run_parallel

        try:
            status = run_func()
        except KeyboardInterrupt:
            print_message(" -- keyboard interrupt -- ", "error")
            return 1
        except Exception as e:
            print_debug(e)
            return 1

        return status

    def _run_serial(self) -> int:  # noqa: C901
        """Run each of the handlers one at a time on the main process

        Returns
        -------
        int
            1 if an error occurs, else 0
        """
        try:
            num_handlers = len(self.handlers)
            num_success = 0
            name = None

            if self.realm != "atm":
                pbar = tqdm(total=len(self.handlers))

            for _, handler in enumerate(self.handlers):
                handler_method = handler["method"]
                handler_variables = handler["raw_variables"]
                table = handler["table"]

                # find the input files this handler needs
                if self.realm in ["atm", "lnd"]:
                    vars_to_filepaths = {
                        var: [
                            os.path.join(self.input_path, x)  # type: ignore
                            for x in find_atm_files(var, self.input_path)
                        ]
                        for var in handler_variables
                    }
                elif self.realm == "fx":
                    vars_to_filepaths = {
                        var: [
                            os.path.join(self.input_path, x)  # type: ignore
                            for x in os.listdir(self.input_path)
                            if x[-3:] == ".nc"
                        ]
                        for var in handler_variables
                    }
                else:
                    vars_to_filepaths = {
                        var: find_mpas_files(var, self.input_path, self.map_path)
                        for var in handler_variables
                    }

                msg = f"Trying to CMORize with handler: {handler}"
                logger.info(msg)

                # NOTE: We need a try and except statement here for TypeError because
                # the VarHandler.cmorize method does not use **kwargs, while the handle
                # method for MPAS still does.
                try:
                    name = handler_method(
                        vars_to_filepaths,
                        self.tables_path,
                        self.new_metadata_path,
                        table,
                        self.cmor_log_dir,
                    )
                except TypeError:
                    name = handler_method(
                        vars_to_filepaths,
                        self.tables_path,
                        self.new_metadata_path,
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

                if self.realm != "atm":
                    pbar.update(1)
            if self.realm != "atm":
                pbar.close()

        except Exception as error:
            print_debug(error)
            return 1
        else:
            msg = f"{num_success} of {num_handlers} handlers complete"
            logger.info(msg)

            return 0

    def _run_parallel(self) -> int:  # noqa: C901
        """Run all the handlers in parallel using multiprocessing.Pool.

        Returns
        --------
        int
            1 if an error occurs, else 0
        """
        pool = Pool(max_workers=self.num_proc)
        pool_res = list()
        will_run = []

        for _, handler in enumerate(self.handlers):
            handler_method = handler["method"]
            handler_variables = handler["raw_variables"]
            table = handler["table"]

            # find the input files this handler needs
            if self.realm in ["atm", "lnd"]:
                vars_to_filepaths = {
                    var: [
                        os.path.join(self.input_path, x)  # type: ignore
                        for x in find_atm_files(var, self.input_path)
                    ]
                    for var in handler_variables
                }
            elif self.realm == "fx":
                vars_to_filepaths = {
                    var: [
                        os.path.join(self.input_path, x)  # type: ignore
                        for x in os.listdir(self.input_path)
                        if x[-3:] == ".nc"
                    ]
                    for var in handler_variables
                }
            else:
                vars_to_filepaths = {
                    var: find_mpas_files(var, self.input_path, self.map_path)
                    for var in handler_variables
                }

            will_run.append(handler.get("name"))

            # NOTE: We need a try and except statement here for TypeError because
            # the VarHandler.cmorize method does not use **kwargs, while the handle
            # method for MPAS still does.
            try:
                res = pool.submit(
                    handler_method,
                    vars_to_filepaths,
                    self.tables_path,
                    self.new_metadata_path,
                    table,
                    self.cmor_log_dir,
                )
            except TypeError:
                res = pool.submit(
                    handler_method,
                    vars_to_filepaths,
                    self.tables_path,
                    self.new_metadata_path,
                )

            pool_res.append(res)

        # wait for each result to complete
        pbar = tqdm(total=len(pool_res))
        num_success = 0
        num_handlers = len(self.handlers)
        finished_success = []
        for idx, res in enumerate(pool_res):
            try:
                out = res.result()
                finished_success.append(out)
                if out:
                    num_success += 1
                    msg = f"Finished {out}, {idx + 1}/{num_handlers} jobs complete"
                else:
                    msg = f'Error running handler {self.handlers[idx]["name"]}'
                    logger.error(msg)

                logger.info(msg)
            except Exception as e:
                print_debug(e)
            pbar.update(1)

        pbar.close()
        pool.shutdown()

        msg = f"{num_success} of {num_handlers} handlers complete"
        logger.info(msg)

        failed = set(will_run) - set(finished_success)
        if failed:
            logger.error(f"{', '.join(list(failed))} failed to complete")
            logger.error(msg)

        return 0

    def _timeout_exit(self):
        print_message("Hit timeout limit, exiting")
        os.kill(os.getpid(), signal.SIGINT)


def main(args: Optional[List[str]] = None):
    app = E3SMtoCMIP(args)
    app.run()


if __name__ == "__main__":
    main()
