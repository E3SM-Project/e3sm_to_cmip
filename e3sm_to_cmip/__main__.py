"""
A python command line tool to turn E3SM model output into CMIP6 compatable data.
"""
from __future__ import absolute_import, division, print_function, unicode_literals

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

import numpy as np
import xarray as xr
import yaml

from e3sm_to_cmip import HANDLERS_PATH, __version__, resources
from e3sm_to_cmip._logger import _setup_custom_logger
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
from e3sm_to_cmip.lib import run_parallel, run_serial
from e3sm_to_cmip.util import (
    FREQUENCIES,
    _get_table_info,
    add_metadata,
    copy_user_metadata,
    precheck,
    print_debug,
    print_message,
)

os.environ["CDAT_ANONYMOUS_LOG"] = "false"

# FIXME: Module has no attribute "warnings" mypy(error)
np.warnings.filterwarnings("ignore")  # type: ignore


logger = _setup_custom_logger(__name__)


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
        # Run the pre-check to determine if any of the variables have already
        # been CMORized.
        if self.precheck_path is not None:
            self._run_precheck()

        # ======================================================================
        # Load handlers.
        # ======================================================================
        logger.info(f"CMIP6 variables to CMORize: \n{self.var_list}\n")

        if self.info_mode:
            self.handlers = load_all_handlers(self.realm, self.var_list)
        elif not self.info_mode and self.input_path is not None:
            e3sm_vars = self._get_e3sm_vars(self.input_path)
            logger.debug(f"Input dataset variables: {e3sm_vars}")

            if self.realm in REALMS:
                self.handlers = derive_handlers(
                    cmip_tables_path=self.tables_path,
                    cmip_vars=self.var_list,
                    e3sm_vars=e3sm_vars,
                    freq=self.freq,
                    realm=self.realm,
                )

                # FIXME: Improve logging here
                cmip_to_e3sm_vars = {
                    handler["name"]: handler["raw_variables"]
                    for handler in self.handlers
                }

                logger.info(
                    "CMIP6 variable handlers derived using these input E3SM variables: "
                    f"\n{cmip_to_e3sm_vars}\n"
                )
            elif self.realm in MPAS_REALMS:
                self.handlers = _get_mpas_handlers(self.var_list)

            if len(self.handlers) == 0:
                print_message(
                    "No CMIP6 variable handlers were successfully derived using the "
                    "input E3SM variables."
                )
                sys.exit(1)

    def run(self):
        # Setup logger information and print out e3sm_to_cmip CLI arguments.
        # ======================================================================
        logger = _setup_custom_logger(f"{self.cmor_log_dir}/e3sm_to_cmip.log", True)

        logger.info("-------------------------------------")
        logger.info("Run Settings")
        logger.info("-------------------------------------")
        logger.info(
            f"\ninput_path='{self.input_path}'\n output_path='{self.output_path}'\n "
            f"precheck_path='{self.precheck_path}'\n freq='{self.freq}'\n "
            f"realm='{self.realm}' "
        )

        if self.output_path is not None:
            logging_path = os.path.join(self.output_path, "converter.log")
            logger.debug(f"Writing log output to: {logging_path}")

            self.new_metadata_path = os.path.join(
                self.output_path, "user_metadata.json"
            )

        # Setup directories using the CLI argument paths (e.g., output dir).
        # ======================================================================
        self._setup_dirs_with_paths()

        # Run e3sm_to_cmip with info mode.
        # ======================================================================
        if self.info_mode:
            logger.info("\n-------------------")
            logger.info("Running Info Mode")
            logger.info("-------------------")
            self._run_info_mode()
            sys.exit(0)

        # Run e3sm_to_cmip to CMORize serially or in parallel.
        # ======================================================================
        timer = None
        if self.timeout:
            timer = threading.Timer(self.timeout, self._timeout_exit)
            timer.start()

        if self.serial_mode:
            logger.info("\n-------------------------------------")
            logger.info("Running E3SM to CMIP in Serial")
            logger.info("-------------------------------------")
            status = self._run_serial()
        else:
            logger.info("\n-------------------------------------")
            logger.info("Running E3SM to CMIP in Parallel")
            logger.info("-------------------------------------")
            status = self._run_parallel()

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
                "Print information about the variables passed in the --var-list "
                "argument and exit without doing any processing. There are three modes "
                "for getting the info, if you just pass the --info flag with the "
                "--var-list then it will print out the information for the requested "
                "variable. \nIf the --freq <frequency> is passed along with the "
                "--tables-path, then the CMIP6 tables will get checked to see if the "
                "requested variables are present in the CMIP6 table matching the freq. "
                "\nIf the --freq <freq> is passed with the --tables-path, and the "
                "--input-path, and the input-path points to raw unprocessed E3SM files, "
                "then an additional check will me made for if the required raw "
                "variables are present in the E3SM output. "
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
            help=("Space separated list of variables to convert from E3SM to CMIP."),
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
            default="./cmor_logs",
            help="Where to put the logging output from CMOR.",
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
            return HANDLERS_PATH

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
        # Create the output directory if it doesn't exist.
        if not os.path.exists(self.output_path):  # type: ignore
            os.makedirs(self.output_path)  # type: ignore

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
        elif self.freq and self.tables_path and not self.input_path:
            for handler in self.handlers:
                table_info = _get_table_info(self.tables_path, handler["table"])
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

        elif self.freq and self.tables_path and self.input_path:
            file_path = next(Path(self.input_path).glob("*.nc"))

            with xr.open_dataset(file_path) as ds:
                for handler in self.handlers:

                    table_info = _get_table_info(self.tables_path, handler["table"])
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

        if self.output_path is not None:
            with open(self.output_path, "w") as outstream:
                yaml.dump(messages, outstream)
        else:
            pprint(messages)

    def _run_serial(self):
        try:
            status = run_serial(
                handlers=self.handlers,
                input_path=self.input_path,
                tables_path=self.tables_path,
                metadata_path=self.new_metadata_path,
                map_path=self.map_path,
                realm=self.realm,
                logdir=self.cmor_log_dir,
                simple=self.simple_mode,
                outpath=self.output_path,
                freq=self.freq,
            )
        except KeyboardInterrupt:
            print_message(" -- keyboard interrupt -- ", "error")
            return 1
        except Exception as e:
            print_debug(e)
            return 1

        return status

    def _run_parallel(self):
        try:
            pool = Pool(max_workers=self.num_proc)
            status = run_parallel(
                pool=pool,
                handlers=self.handlers,
                input_path=self.input_path,
                tables_path=self.tables_path,
                metadata_path=self.new_metadata_path,
                map_path=self.map_path,
                realm=self.realm,
                logdir=self.cmor_log_dir,
                simple=self.simple_mode,
                outpath=self.output_path,
                freq=self.freq,
            )
        except KeyboardInterrupt:
            print_message(" -- keyboard interrupt -- ", "error")
            return 1
        except Exception as error:
            print_debug(error)
            return 1

        return status

    def _timeout_exit(self):
        print_message("Hit timeout limit, exiting")
        os.kill(os.getpid(), signal.SIGINT)


def main(args: Optional[List[str]] = None):
    app = E3SMtoCMIP(args)
    app.run()


if __name__ == "__main__":
    main()
