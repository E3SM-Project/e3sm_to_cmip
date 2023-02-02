"""Top-level package for e3sm_to_cmip."""

__version__ = "1.9.1"  # pragma: no cover

import os

from e3sm_to_cmip import cmor_handlers, resources

resources_path = os.path.split(os.path.abspath(resources.__file__))[0]

# Handlers defined as yaml file entries.
HANDLER_YAML_PATH = os.path.join(resources_path, "handlers.yaml")

# Handlers defined as Python modules (e.g., MPAS var handlers).
HANDLERS_PATH = os.path.split(os.path.abspath(cmor_handlers.__file__))[0]

VAR_HANDLER_PATHS = f"{HANDLERS_PATH}/vars"
MPAS_VAR_HANDLER_PATHS = f"{HANDLERS_PATH}/mpas_vars"
