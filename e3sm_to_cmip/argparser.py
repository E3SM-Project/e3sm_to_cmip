import argparse
import sys
from typing import List

from e3sm_to_cmip import __version__
from e3sm_to_cmip.util import FREQUENCIES


def setup_argparser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Convert ESM model output into CMIP compatible format.",
        prog="e3sm_to_cmip",
        usage="%(prog)s [-h]",
        add_help=False,
    )

    # Argument groups to organize the numerous arguments printed by --help.
    required = parser.add_argument_group("required arguments (general)")
    required_no_info = parser.add_argument_group("required arguments (without --info)")
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
        help=("Space separated list of CMIP variables to convert from E3SM to CMIP."),
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
        version=f"%(prog)s {__version__}",
    )

    return parser


def parse_args(args: List[str] | None) -> argparse.Namespace:
    """Parses command line arguments.

    Parameters
    ----------
    args : List[str] | None
        A list of arguments, useful for debugging purposes to simulate a
        passing arguments via the CLI.

    Returns
    -------
    CLIArguments
        A data class of parsed arguments.
    """
    argparser = setup_argparser()

    try:
        args_to_parse = sys.argv[1:] if args is None else args
    except (Exception, BaseException):
        argparser.print_help()
        sys.exit(1)

    # Parse the arguments and perform validation.
    parsed_args = argparser.parse_args(args_to_parse)
    _validate_parsed_args(parsed_args)

    return parsed_args


def _validate_parsed_args(parsed_args: argparse.Namespace):
    if parsed_args.realm == "mpaso" and not parsed_args.map:
        raise ValueError("MPAS ocean handling requires a map file")

    if parsed_args.realm == "mpassi" and not parsed_args.map:
        raise ValueError("MPAS sea-ice handling requires a map file")

    if not parsed_args.simple and not parsed_args.tables_path and not parsed_args.info:
        raise ValueError("Running without the --simple flag requires CMIP6 tables path")

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

    valid_freqs = [freq for freq_type in FREQUENCIES for freq in freq_type]
    if parsed_args.freq and parsed_args.freq not in valid_freqs:
        raise ValueError(
            f"Frequency set to {parsed_args.freq} which is not in the set of allowed "
            "frequencies: {', '.join(valid_freqs)}"
        )
