"""
Entry point for the E3SM to CMIP conversion tool.

Usage:
    Run this script with --help or --version for information.
"""

import sys

from e3sm_to_cmip.argparser import parse_args


def main(args: list[str] | None = None):
    parsed_args = parse_args(args)

    try:
        # Defer expensive imports and initialization.
        from e3sm_to_cmip.runner import E3SMtoCMIP
        from e3sm_to_cmip._logger import _setup_child_logger, _setup_root_logger

        _setup_root_logger()
        logger = _setup_child_logger(__name__)

        # Initialize the application.
        app = E3SMtoCMIP(parsed_args)

        # Run the application.
        app.run()

    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}", exc_info=True)

        sys.exit(1)


if __name__ == "__main__":
    main()
