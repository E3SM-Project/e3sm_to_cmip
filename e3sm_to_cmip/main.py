"""
Entry point for the E3SM to CMIP conversion tool.

Usage:
    Run this script with --help or --version for information.
"""

import logging
import sys

from e3sm_to_cmip.argparser import parse_args


def main(args: list[str] | None = None):
    parsed_args = parse_args(args)

    try:
        # Defer expensive imports and initialization.
        from e3sm_to_cmip._logger import _setup_child_logger, _setup_root_logger
        from e3sm_to_cmip.runner import E3SMtoCMIP

        _setup_root_logger()
        logger = _setup_child_logger(__name__)

        # Initialize the application.
        app = E3SMtoCMIP(parsed_args)

        # Run the application.
        app.run()

        _remove_root_handlers()
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}", exc_info=True)

        _remove_root_handlers()

        sys.exit(1)


def _remove_root_handlers():
    """Remove all handlers associated with the root logger.

    This prevents duplicate log messages when the `e3sm_to_cmip` is run
    multiple times successively.
    """
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)


if __name__ == "__main__":
    main()
