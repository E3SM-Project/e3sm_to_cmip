"""Top-level package for e3sm_to_cmip."""

__version__ = "1.10.0"  # pragma: no cover

import os

from e3sm_to_cmip import cmor_handlers

# Path to the root directory for all handler related files.
ROOT_HANDLERS_DIR = os.path.split(os.path.abspath(cmor_handlers.__file__))[0]

# Path to the yaml file where handlers are defined.
HANDLER_DEFINITIONS_PATH = os.path.join(ROOT_HANDLERS_DIR, "handlers.yaml")

# Path to the directory where legacy handlers are defined.
# TODO: The handlers defined here need to be refactored and moved to `handlers.yaml`.
LEGACY_HANDLER_DIR_PATH = f"{ROOT_HANDLERS_DIR}/vars"

# Path to the directory where MPAS handlers are defined.
# TODO: MPAS handlers should eventually be refactored to `handlers.yaml` too.
MPAS_HANDLER_DIR_PATH = f"{ROOT_HANDLERS_DIR}/mpas_vars"
