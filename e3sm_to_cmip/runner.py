"""
This module provides the main functionality for converting E3SM model output
to CMIP-compliant datasets. It includes the `E3SMtoCMIP` class, which handles
the configuration, logging, and execution of the conversion process. The module
supports both serial and parallel processing modes and provides options for
pre-checking variables, generating metadata, and handling various realms and
frequencies.
"""

import argparse
import concurrent
import os
import signal
import subprocess
import sys
import tempfile
import threading
from concurrent.futures import ProcessPoolExecutor as Pool
from concurrent.futures import as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from pprint import pprint
from typing import Literal

import xarray as xr
import yaml
from tqdm import tqdm

from e3sm_to_cmip import ROOT_HANDLERS_DIR, __version__, resources
from e3sm_to_cmip._logger import _add_filehandler, _setup_child_logger
from e3sm_to_cmip.argparser import parse_args
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
    _get_table_info,
    add_metadata,
    find_atm_files,
    find_mpas_files,
    get_handler_info_msg,
    precheck,
)

logger = _setup_child_logger(__name__)


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
    on_var_failure: Literal["ignore", "fail", "stop"]

    # Run settings.
    num_proc: int
    debug: bool
    timeout: int

    # CMOR settings
    var_list: list[str]
    realm: Realm | MPASRealm
    freq: Frequency

    # Path references.
    input_path: str | None
    output_path: str | None
    tables_path: str | None
    handlers: str | None
    map: str | None
    info_out: str | None

    precheck: str | None
    logdir: str
    user_metadata: str | None
    custom_metadata: str | None


class E3SMtoCMIP:
    def __init__(self, args: argparse.Namespace | list[str] | None = None):
        self.timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
        self.log_filename = f"{self.timestamp}.log"

        # Parse the command line arguments if they are not already parsed.
        if not isinstance(args, argparse.Namespace):
            args = parse_args(args)

        parsed_args = self.convert_parsed_args_to_data_class(args)

        # NOTE: The order of these attributes align with class CLIArguments.
        # ======================================================================
        # Run Mode settings.
        # ======================================================================
        self.simple_mode: bool = parsed_args.simple
        self.serial_mode: bool = parsed_args.serial
        self.info_mode: bool = parsed_args.info
        self.on_var_failure: Literal["ignore", "fail", "stop"] = (
            parsed_args.on_var_failure
        )

        # ======================================================================
        # Run settings.
        # ======================================================================
        self.num_proc: int = parsed_args.num_proc
        self.debug: bool = parsed_args.debug
        self.timeout: int = parsed_args.timeout

        # ======================================================================
        # CMOR settings.
        # ======================================================================
        self.var_list: list[str] = self._get_var_list(parsed_args.var_list)
        self.realm: Realm | MPASRealm = parsed_args.realm
        self.freq: Frequency = parsed_args.freq

        # ======================================================================
        # Paths references.
        # ======================================================================
        self.input_path: str | None = parsed_args.input_path
        self.output_path: str | None = self._setup_output_path(parsed_args.output_path)
        self.tables_path: str = self._get_tables_path(parsed_args.tables_path)
        self.handlers_path: str = self._get_handlers_path(parsed_args.handlers)
        self.map_path: str | None = parsed_args.map
        self.info_out_path: str | None = parsed_args.info_out
        self.precheck_path: str | None = parsed_args.precheck
        self.cmor_log_dir: str = parsed_args.logdir
        self.user_metadata: str | None = parsed_args.user_metadata
        self.custom_metadata: str | None = parsed_args.custom_metadata

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
            "Timestamp": self.timestamp,
            "Version Info": self._get_version_info(),
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
            "CMIP Metadata Path": self.new_metadata_path,
            "Temp Path for Processing MPAS Files": self.temp_path,
            "Frequency": self.freq,
            "Realm": self.realm,
        }

        for key, value in config_details.items():
            logger.info(f"    * {key}: {value}")

        # Load the CMOR handlers based on the realm and variable list.
        self.handlers = self._get_handlers()

    def _get_version_info(self) -> str:
        """Retrieve version information for the current codebase.

        This method attempts to determine the current Git branch name and commit
        hash of the repository containing this file. If the Git information
        cannot be retrieved, it falls back to using the `__version__` variable.

        Returns
        -------
        str
            A string containing the Git branch name and commit hash in the
            format "branch <branch_name> with commit <commit_hash>", or the
            fallback version string in the format "version <__version__>" if Git
            information is unavailable.
        """
        try:
            branch_name = (
                subprocess.check_output(
                    ["git", "rev-parse", "--abbrev-ref", "HEAD"],
                    cwd=os.path.dirname(__file__),
                    stderr=subprocess.DEVNULL,
                )
                .strip()
                .decode("utf-8")
            )
            commit_hash = (
                subprocess.check_output(
                    ["git", "rev-parse", "HEAD"],
                    cwd=os.path.dirname(__file__),
                    stderr=subprocess.DEVNULL,
                )
                .strip()
                .decode("utf-8")
            )
            version_info = f"branch {branch_name} with commit {commit_hash}"
        except subprocess.CalledProcessError:
            version_info = f"version {__version__}"

        return version_info

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

        is_run_successful = self._run_by_mode()

        if is_run_successful:
            if self.custom_metadata:
                add_metadata(
                    file_path=self.output_path,
                    var_list=self.var_list,
                    metadata_path=self.custom_metadata,
                )

            if timer is not None:
                timer.cancel()

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

    def _get_e3sm_vars(self, input_path: str) -> list[str]:
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
        list[str]
            List of data variables in the input files.

        Raises
        ------
        IndexError
            If no data variables were found in the input files.
        """
        paths: list[str] = []
        e3sm_vars: list[str] = []

        for root, _, files in os.walk(input_path):
            for filename in files:
                if ".nc" in filename:
                    paths.append(str(Path(root, filename).absolute()))

        for path in paths:
            ds = xr.open_dataset(path, decode_timedelta=True)
            data_vars = list(ds.data_vars.keys())

            e3sm_vars = e3sm_vars + data_vars

        if len(e3sm_vars) == 0:
            raise IndexError(
                f"No variables were found in the input file(s) at '{input_path}'."
            )

        return e3sm_vars

    def _get_var_list(self, input_var_list: list[str]) -> list[str]:
        if len(input_var_list) == 1 and " " in input_var_list[0]:
            var_list = input_var_list[0].split()
        else:
            var_list = input_var_list

        var_list = [x.strip(",") for x in var_list]

        return var_list

    def _get_handlers_path(self, handlers_path: str | None) -> str:
        if handlers_path is None:
            return ROOT_HANDLERS_DIR

        return os.path.abspath(handlers_path)

    def _get_tables_path(self, tables_path: str | None):
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
            logger.info("All variables previously computed")
            if self.output_path is not None:
                os.mkdir(os.path.join(self.output_path, "CMIP6"))
            return 0
        else:
            logger.info(f"Setting up conversion for {' '.join(new_var_list)}", "ok")
            self.var_list = new_var_list

    def _setup_output_path(self, output_path: str | None) -> str:
        """
        Set up and return the absolute output directory path.

        If an output path is provided, its absolute path is used. Otherwise, a
        new directory is created in the current working directory with a name
        based on the current timestamp. The directory is created if it does not
        exist, and permissions are set accordingly.

        Parameters
        ----------
        output_path : str | None
            The desired output directory path. If None, a default directory is
            created.

        Returns
        -------
        str
            The absolute path to the output directory.
        """
        if output_path is not None:
            output_abs_path = os.path.abspath(output_path)
        elif output_path is None:
            output_abs_path = os.path.join(
                os.getcwd(), f"e3sm_to_cmip_run_{self.timestamp}"
            )

        os.makedirs(output_abs_path, exist_ok=True)

        return output_abs_path

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
        self.cmor_log_dir = os.path.join(self.output_path, self.cmor_log_dir)  # type: ignore
        os.makedirs(self.cmor_log_dir, exist_ok=True)

        # NOTE: Any warnings that appear before the log filehandler is
        # instantiated will not be captured (e.g,. esmpy VersionWarning).
        # However, they will still be captured by the console via a
        # StreamHandler.
        self.log_path = os.path.join(self.output_path, self.log_filename)  # type: ignore
        _add_filehandler(self.log_path)

        # Make the metadata filename unique by appending the process ID (PID)
        # to prevent resource conflicts when invoking multiple instances of
        # e3sm_to_cmip simultaneously.
        self.new_metadata_path = os.path.join(
            self.output_path,  # type: ignore
            f"user_metadata_{os.getpid()}.json",
        )
        # Copy the user's metadata json file with the updated output directory.
        if not self.simple_mode and not self.info_mode:
            self._copy_user_metadata()

        # Temporary directory for MPAS processing (e.g., regridding).
        self.temp_path = None
        if not self.info_mode:
            # Make temp_path unique by appending the process ID
            self.temp_path = f"{self.output_path}/tmp_mpas/tmp_{os.getpid()}"

            os.makedirs(self.temp_path, exist_ok=True)

            tempfile.tempdir = self.temp_path

    def _copy_user_metadata(self):
        """
        Copies user metadata from an input file to an output file, updating the
        "outpath" field.

        TODO: Refactor this method to reduce its complexity.

        Raises
        ------
        IOError
            If there is an error reading the input file, opening the output file,
            or writing to the output file.

        Notes
        -----
        If a line in the input metadata contains the string "outpath", it will
        be replaced with a line containing the new output path. All other lines
        are copied as-is.
        """
        try:
            fin = open(self.user_metadata, "r")  # type: ignore
        except IOError as error:
            print("Unable to write out metadata")
            raise error

        try:
            fout = open(self.new_metadata_path, "w")
        except IOError as error:
            print("Unable to open output location for custom user metadata")
            raise error

        try:
            for line in fin:
                if "outpath" in line:
                    fout.write(f'\t"outpath": "{self.output_path}",\n')
                else:
                    fout.write(line)

        except IOError as error:
            print("Write failure for user metadata")
            raise error
        finally:
            fin.close()
            fout.close()

    def _run_info_mode(self):  # noqa: C901
        """
        Executes the "info mode" logic for the runner, providing information
        about variable handlers, their inclusion in CMIP tables, and dataset
        consistency.

        The method operates in three modes based on the provided inputs:

        1. **Handler Information Mode**: Lists handler info when frequency is
           "mon" and no input or table paths are provided.
        2. **Variable Inclusion Mode**: Checks if variables are in CMIP tables
           when frequency and table paths are provided, but no input path.
        3. **Table-Dataset Consistency Mode**: Validates dataset and CMIP table
           consistency when frequency, table paths, and input paths are given.

        Logs errors for unsupported variables, missing raw variables, or invalid
        frequency-table combinations. Outputs results to a YAML file or prints
        them to the console.

        Raises
        ------
        Exception
            Logs and handles unexpected errors during execution.

        Notes
        -----
        - Uses `self.handlers` to iterate over variable handlers.
        - Outputs are written to `self.info_out_path` or `self.output_path` if
          specified.
        - Finalizes failure behavior using `_finalize_failure_exit`.
        """
        messages = []
        failed_handlers: list[str] = []

        try:
            # Info mode 1: only show handler info
            # Use case: when a user just asks for the handler information.
            if self.freq == "mon" and not self.input_path and not self.tables_path:
                for handler in self.handlers:
                    hand_msg = get_handler_info_msg(handler)
                    messages.append(hand_msg)

            # Info mode 2: check variable inclusion in tables.
            # Use case: when a user asked if the variable is included in the
            # table but did not ask about the files in the inpath.
            elif self.freq and self.tables_path and not self.input_path:
                for handler in self.handlers:
                    table_info = _get_table_info(self.tables_path, handler["table"])
                    if handler["name"] not in table_info["variable_entry"]:
                        msg = (
                            f"Variable {handler['name']} is not included in the table "
                            f"{handler['table']}"
                        )
                        logger.error(msg)

                        failed_handlers = self._handle_failed_handler(
                            handler["name"], failed_handlers, msg
                        )
                        continue

                    # Skip irrelevant table-frequency combos.
                    if self.freq == "mon" and handler["table"] == "CMIP6_day.json":
                        continue
                    if (self.freq in ["day", "3hr"]) and handler[
                        "table"
                    ] == "CMIP6_Amon.json":
                        continue

                    hand_msg = get_handler_info_msg(handler)
                    messages.append(hand_msg)

            # Info mode 3: check table + dataset consistency
            elif self.freq and self.tables_path and self.input_path:
                file_path = next(Path(self.input_path).glob("*.nc"))

                with xr.open_dataset(file_path) as ds:
                    for handler in self.handlers:
                        table_info = _get_table_info(self.tables_path, handler["table"])

                        # If the variable is not in the table, it is not supported
                        # and therefore logged as a failure.
                        if handler["name"] not in table_info["variable_entry"]:
                            msg = (
                                f"Variable {handler['name']} is not included in the table "
                                f"{handler['table']}"
                            )
                            logger.error(msg)

                            failed_handlers = self._handle_failed_handler(
                                handler["name"], failed_handlers, msg
                            )
                            continue

                        # Skip invalid frequency-table pairs.
                        # We test here against the input "freq", because atmos mon
                        # data satisfies BOTH CMIP6_day.json AND CMIP6_mon.json, but
                        # we only want the latter in the "hand_msg" output. The vars
                        # "hass" and "rlut" have multiple freqs.
                        if self.freq == "mon" and handler["table"] == "CMIP6_day.json":
                            continue
                        if (self.freq in ["day", "3hr"]) and handler[
                            "table"
                        ] == "CMIP6_Amon.json":
                            continue

                        raw_vars = list(handler["raw_variables"])
                        missing_vars = [v for v in raw_vars if v not in ds.data_vars]

                        if missing_vars:
                            msg = (
                                f"Variable {handler['name']} is missing raw vars "
                                f"{missing_vars} in the input dataset"
                            )
                            logger.error(msg)
                            failed_handlers = self._handle_failed_handler(
                                handler["name"], failed_handlers, msg
                            )
                            continue

                        if not missing_vars:
                            # Passed all checks → supported
                            stat_msg = f"Table={handler['table']}:Variable={handler['name']}:DataSupport=TRUE"
                            hand_msg = get_handler_info_msg(handler)
                            messages.append(hand_msg)
                        else:
                            # Missing raw vars → not supported
                            stat_msg = f"Table={handler['table']}:Variable={handler['name']}:DataSupport=FALSE"

                        logger.info(stat_msg)

            # Output log messages.
            if self.info_out_path is not None:
                with open(self.info_out_path, "w") as outstream:
                    yaml.dump(messages, outstream)
            elif self.output_path is not None:
                yaml_filepath = os.path.join(self.output_path, "info.yaml")
                with open(yaml_filepath, "w") as outstream:
                    yaml.dump(messages, outstream)
            else:
                pprint(messages)

        except Exception as e:
            logger.error(f"Unexpected error in info mode: {e}")
            failed_handlers = self._handle_failed_handler(
                "info_mode", failed_handlers, e
            )

        self._finalize_failure_exit(failed_handlers)

    def _run_by_mode(self) -> bool:
        """
        Executes the CMORization process in either serial or parallel mode.

        Returns
        -------
        bool
            True if the run was successful, False otherwise.
        """
        try:
            if self.serial_mode:
                result = self._run_serial()
            else:
                result = self._run_parallel()
        except KeyboardInterrupt:
            logger.error(" -- keyboard interrupt -- ")

            return False
        except Exception as e:
            logger.error(e)

            return False

        return result

    def _run_serial(self) -> Literal[True]:
        """Run each of the handlers one at a time on the main process.

        This method processes each handler sequentially and logs the status of
        each handler.

        The behavior depends on the `self.on_var_failure` setting:

           - "ignore": Continues processing even if some handlers fail.
             Always returns True.
           - "fail": Exits with a status code of 1 if any handler fails.
           - "stop": Terminates immediately upon the first failure and exits with a
              status code of 1.

        Returns
        -------
        Literal[True]
            Always True, even if some handlers fail, unless `self.on_var_failure`
            is set to "fail" or "stop", in which case the process may terminate early.
        """
        num_handlers = len(self.handlers)
        num_success = 0
        failed_handlers: list[str] = []

        try:
            if self.realm != "atm":
                pbar = tqdm(total=num_handlers)

            logger.info("========== STARTING CMORIZING PROCESS ==========")
            for index, handler in enumerate(self.handlers):
                is_cmor_successful = False
                handler_method = handler["method"]
                handler_variables = handler["raw_variables"]
                handler_table = handler["table"]
                vars_to_filepaths = self._get_handler_input_files(handler_variables)

                logger.info(
                    f"CMOR attempt {index + 1}/{num_handlers} -- '{handler['name']}' handler: {handler}"
                )
                try:
                    # MPAS handlers require a different set of arguments than other
                    # handlers.
                    if self.realm in MPAS_REALMS:
                        is_cmor_successful = handler_method(
                            vars_to_filepaths,
                            self.tables_path,
                            self.new_metadata_path,
                            self.cmor_log_dir,
                        )
                    else:
                        is_cmor_successful = handler_method(
                            vars_to_filepaths,
                            self.tables_path,
                            self.new_metadata_path,
                            self.cmor_log_dir,
                            handler_table,
                        )
                except Exception as e:
                    logger.error(f"Exception in handler '{handler['name']}': {e}")
                    is_cmor_successful = False

                num_success, failed_handlers = self._log_handler_status(
                    is_cmor_successful,
                    handler["name"],
                    num_handlers,
                    num_success,
                    failed_handlers,
                )

                if not is_cmor_successful:
                    failed_handlers = self._handle_failed_handler(
                        handler["name"], failed_handlers
                    )

                if self.realm != "atm":
                    pbar.update(1)

            if self.realm != "atm":
                pbar.close()

        except Exception as error:
            logger.error(error)

        self._log_final_result(num_handlers, num_success, failed_handlers)
        self._finalize_failure_exit(failed_handlers)

        return True

    def _run_parallel(self) -> Literal[True]:
        """Run all handlers in parallel using ProcessPoolExecutor.

        This method processes handlers concurrently, tracks their success or failure,
        and logs the results.

        The behavior depends on the `self.on_var_failure` setting:

           - "ignore": Continues processing even if some handlers fail.
             Always returns True.
           - "fail": Exits with a status code of 1 if any handler fails.
           - "stop": Terminates immediately upon the first failure and exits with a
              status code of 1.

        Returns
        -------
        Literal[True]
            True if the process completes, unless terminated early due to "fail" or "stop".
        """
        pool = Pool(max_workers=self.num_proc)
        futures: list[concurrent.futures.Future] = []
        # Map each future to its handler name
        future_to_name = {}
        pbar = tqdm(total=len(self.handlers))

        num_handlers = len(self.handlers)
        num_success = 0
        failed_handlers: list[str] = []

        logger.info("========== STARTING CMORIZING PROCESS ==========")
        for handler in self.handlers:
            handler_method = handler["method"]
            handler_variables = handler["raw_variables"]
            handler_table = handler["table"]
            vars_to_filepaths = self._get_handler_input_files(handler_variables)

            try:
                if self.realm in MPAS_REALMS:
                    future = pool.submit(
                        handler_method,
                        vars_to_filepaths,
                        self.tables_path,
                        self.new_metadata_path,
                        self.cmor_log_dir,
                    )
                else:
                    future = pool.submit(
                        handler_method,
                        vars_to_filepaths,
                        self.tables_path,
                        self.new_metadata_path,
                        self.cmor_log_dir,
                        handler_table,
                    )
            except Exception as exc:
                logger.error(
                    f"Failed to submit handler '{handler.get('name', 'unknown')}' to pool: {exc}"
                )
                continue

            futures.append(future)
            # Map future job to handler name for progress tracking as they
            # complete
            future_to_name[future] = handler.get("name", "unknown")

        # Execute the jobs in the pool and log their status as they complete.
        for future in as_completed(futures):
            handler_name = future_to_name[future]
            future_result = None

            try:
                future_result = future.result()
            except Exception as e:
                logger.error(f"Handler '{handler_name}' raised an exception: {e}")
                future_result = False

            num_success, failed_handlers = self._log_handler_status(
                future_result, handler_name, num_handlers, num_success, failed_handlers
            )

            if not future_result:
                failed_handlers = self._handle_failed_handler(
                    handler_name, failed_handlers
                )

            pbar.update(1)

        pbar.close()
        pool.shutdown()
        self._log_final_result(num_handlers, num_success, failed_handlers)
        self._finalize_failure_exit(failed_handlers)

        return True

    def _get_handler_input_files(
        self, handler_variables: dict[str, str]
    ) -> dict[str, list[str]]:
        """Get the input files for a given handler.

        Parameters
        ----------
        handler_variables : dict[str, str]
            The handler dictionary containing the variable names and their
            corresponding file paths.

        Returns
        -------
        dict[str, list[str]]
            A dictionary mapping variable names to their corresponding file paths.
        """
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

        return vars_to_filepaths

    def _log_handler_status(
        self,
        is_cmor_successful: bool | str | None,
        name: str,
        num_handlers: int,
        num_success: int,
        failed_handlers: list[str],
    ) -> tuple[int, list[str]]:
        """
        Logs the status of a handler after attempting to CMORize a variable.

        Parameters
        ----------
        is_cmor_successful : bool | str | None
            Indicates if the handler was successful. False, None, or "" means
            failure. VarHandler objects (`handlers/*.py`) return False for
            failures. MPAS handlers (`mpas_vars/*.py`) return "" for failures.
        name : str
            The name of the handler (variable) being processed.
        num_handlers : int
            The total number of handlers.
        num_success : int
            The current count of successful handlers.
        failed_handlers : list[str]
            A list to append failed handler names to.

        Returns
        -------
        tuple[int, list[str]]
            The updated number of successful handlers and the list of failed
            handlers.
        """
        if is_cmor_successful:
            num_success += 1
            logger.info(f"Successfully processed '{name}' handler.")
        else:
            failed_handlers.append(name)
            logger.error(f"Error processing '{name}' handler.")

        logger.info("=" * 60)
        logger.info("STATUS UPDATE:")
        logger.info(f"  * Successful handlers: {num_success} of {num_handlers}")
        logger.info(f"  * Failed handlers: {len(failed_handlers)}")

        if failed_handlers:
            logger.info(f"  - Failed handler names: {', '.join(failed_handlers)}")
        else:
            logger.info("  - No failed handlers so far.")
        logger.info("=" * 60)

        return num_success, failed_handlers

    def _log_final_result(
        self, num_handlers: int, num_successes: int, failed_handlers: list[str]
    ):
        """
        Logs the final result of the CMORization process.

        Parameters
        ----------
        num_handlers : int
            The total number of handlers that were processed.
        num_successes : int
            The number of handlers that completed successfully.
        failed_handlers : list[str]
            A list of handler names that failed during processing.
        """
        logger.info("========== FINAL RUN RESULTS ==========")
        logger.info(f"* {num_successes} of {num_handlers} handlers succeeded.")

        if failed_handlers:
            logger.error(
                "* The following handlers failed: "
                + ", ".join(str(h) for h in failed_handlers)
            )
        else:
            logger.info("* All handlers completed successfully.")

        logger.info("=======================================")

    def _timeout_exit(self):
        logger.info("Hit timeout limit, exiting")
        os.kill(os.getpid(), signal.SIGINT)

    def _handle_failed_handler(
        self,
        handler_name: str,
        failed_handlers: list[str],
        reason: str | Exception | None = None,
    ) -> list[str]:
        """Record and log a failed handler.

        If ``self.on_var_failure`` is set to "stop", the program will log an
        error message and terminate execution immediately (exit code 1).

        Parameters
        ----------
        handler_name : str
            The name of the handler that failed.
        failed_handlers : list of str
            A list of handler names that failed during processing.
        reason : str or Exception or None, optional
            The reason for the handler failure. Can be a string, an exception, or None.

        Returns
        -------
        list[str]
            The updated list of failed handler names.
        """
        failed_handlers.append(handler_name)

        msg = f"Handler '{handler_name}' failed."
        if reason:
            msg += f" Reason: {reason}"

        logger.error(msg)

        if self.on_var_failure == "stop":
            logger.error(
                f"Stopping immediately due to --on-var-failure=stop "
                f"(failed handler: '{handler_name}')"
            )
            sys.exit(1)

        return failed_handlers

    def _finalize_failure_exit(self, failed_handlers: list[str]) -> None:
        """Handle final exit behavior based on --on-var-failure mode.

        This method finalizes the process by checking for failed handlers and
        exiting if necessary if ``self.on_var_failure`` is set to "fail"
        (exit code 1).

        Parameters
        ----------
        failed_handlers : list[str]
            A list of handler names that failed during processing.
        """
        if failed_handlers and self.on_var_failure == "fail":
            logger.error(
                f"{len(failed_handlers)} handler(s) failed. "
                f"Exiting with code 1 (--on-var-failure=fail)."
            )

            sys.exit(1)

    def convert_parsed_args_to_data_class(
        self, parsed_args: argparse.Namespace
    ) -> CLIArguments:
        """Convert parsed arguments to a data class.

        Parameters
        ----------
        parsed_args : argparse.Namespace
            The parsed arguments from the command line.

        Returns
        -------
        CLIArguments
            A data class of parsed arguments.
        """
        return CLIArguments(**vars(parsed_args))
