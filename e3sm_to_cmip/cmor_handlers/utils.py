import imp
import os
from collections import defaultdict
from typing import Any, Dict, List, Literal, Union, get_args

import pandas as pd
import yaml

from e3sm_to_cmip import (
    HANDLER_DEFINITIONS_PATH,
    LEGACY_HANDLER_DIR_PATH,
    MPAS_HANDLER_DIR_PATH,
)
from e3sm_to_cmip._logger import _setup_custom_logger
from e3sm_to_cmip.cmor_handlers.handler import VarHandler
from e3sm_to_cmip.util import _get_table_for_non_monthly_freq

logger = _setup_custom_logger(__name__)

# Type aliases
Frequency = Literal["mon", "day", "6hrLev", "6hrPlev", "6hrPlevPt", "3hr", "1hr"]

Realm = Literal["atm", "lnd", "fx"]
REALMS = get_args(Realm)

MPASRealm = Literal["mpaso", "mpassi", "SImon", "Omon"]
MPAS_REALMS = get_args(MPASRealm)

# Used by areacella.py
RADIUS = 6.37122e6


def load_all_handlers(
    realm: Union[Realm, MPASRealm], cmip_vars: List[str]
) -> List[Dict[str, Any]]:
    """Loads variable handlers based on a list of variable names.

    This function is used specifically for printing out the handler information
    (info_mode).

    Parameters
    ----------
    realm: Union[Realm, MPASRealm]
        The realm.
    cmip_vars : List[str]
        The list of CMIP6 variables to CMORize.



    Returns
    -------
    List[Dict[str, Any]]:
        A list of the dictionary representation of VarHandler objects.

    Raises
    ------
    KeyError
        If no handlers are defined for a CMIP6 variable in `handlers.yaml`.
    """
    handlers_by_var: Dict[str, List[Dict[str, Any]]] = _get_handlers_by_var()

    if realm in REALMS:
        handlers: List[Dict[str, Any]] = []

        for var in cmip_vars:
            try:
                var_handlers = handlers_by_var[var]
            except KeyError:
                raise KeyError(
                    f"No variable handlers are defined for '{var}'. Make sure a "
                    "variable handler(s) in `handlers.yaml`."
                )

            handlers = handlers + var_handlers
    else:
        handlers = _get_mpas_handlers(cmip_vars)

    return handlers


def _get_mpas_handlers(cmip_vars: List[str]):
    """Get MPAS variable handlers using the list of CMIP variables.

    All current MPAS variable handlers are defined as modules and there is only
    1 handler module defined per variable.

    Parameters
    ----------
    cmip_vars : List[str]
        The list of CMIP6 variables to CMORize.

    Returns
    -------
    KeyError
        If no handlers are defined for the MPAS CMIP6 variable.
    """
    handlers = _get_handlers_from_modules(MPAS_HANDLER_DIR_PATH)

    derived_handlers: List[Dict[str, Any]] = []
    for var in cmip_vars:
        try:
            var_handler = handlers[var]
        except KeyError:
            raise KeyError(
                f"No variable handlers are defined for '{var}'. Make sure a "
                f"variable handler is defined for '{var}' in {MPAS_HANDLER_DIR_PATH}."
            )

        derived_handlers.append(var_handler[0])

    return derived_handlers


def derive_handlers(
    cmip_tables_path: str,
    cmip_vars: List[str],
    e3sm_vars: List[str],
    freq: Frequency = "mon",
    realm: Union[Realm, MPASRealm] = "atm",
) -> List[Dict[str, Any]]:
    """Derives the appropriate handler for each CMIP variable.

    For each CMIP variable the user wants to CMORize (`cmip_vars`), a variable
    handler is derived based on the existing E3SM variables in the input
    dataset. For example, the user wants to CMORize the "pr" variable. In some
    cases, the input dataset might have the E3SM variable "PRECT", while in other
    cases "PRECL" and "PRECC". "pr" can be CMORized in either of these cases,
    so this function is flexible enough to derive the appropriate handler based
    on the existing E3SM variables.

    The CMIP table used for the handler is based on the frequency that is
    passed. If `freq="mon"`, then the base table is used (usually monthly).
    For other frequencies such as "day", the table is derived using the
    base table as the starting point.

    Parameters
    ----------
    cmip_tables_path : str
        The path to the CMIP6 tables.
    cmip_vars : List[str]
        The list of CMIP6 variables to CMORize.
    e3sm_vars : Optional[List[str]]
        The list of E3SM variables from the input files to use for CMORizing.
    freq : Optional[Frequency], optional
        The frequency used to derive the appropriate CMIP6 table for each
        variable handler, by default "mon".
    realm : Optional[str], optional
        The realm, by default "atm".

    Returns
    -------
    List[Dict[str, Any]]:
        A list of the dictionary representation of VarHandler objects.

    Raises
    ------
    KeyError
        If no handlers are defined for a CMIP6 variable in `handlers.yaml`.
    KeyError
        If a handler could not be derived for a CMIP6 variable using the existing
        E3SM variables.
    """
    # TODO: Refactor the function parameters.
    handlers_by_var: Dict[str, List[Dict[str, Any]]] = _get_handlers_by_var()
    derived_handlers: List[Dict[str, Any]] = []

    for var in cmip_vars:
        # Get all handlers defined in `handlers.yaml` for the variable.
        try:
            var_handlers = handlers_by_var[var]
        except KeyError:
            raise KeyError(
                f"No variable handlers are defined for '{var}'. Make sure a "
                "variable handler(s) in `handlers.yaml`."
            )

        # Derive the appropriate handler to use for the variable based on
        # the E3SM variables on the dataset.
        derived_handler = _derive_handler(var, e3sm_vars, var_handlers)

        # Update which CMIP table to use for lookup if a custom frequency is
        # passed.
        if freq != "mon":
            derived_handler["table"] = _get_table_for_non_monthly_freq(
                derived_handler["name"],
                derived_handler["table"],
                freq,
                realm,
                cmip_tables_path,
            )

        derived_handlers.append(derived_handler)

    return derived_handlers


def _derive_handler(
    cmip_var: str, e3sm_vars: List[str], handlers: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Derives a VarHandler object for a CMIP variable based on the existing E3SM
    variables.

    This function loops through a list of VarHandler objects defined for a
    CMIP6 variable. It checks if any of the handler's raw E3SM variables exist
    in the list of input varibles. If there is a match, break out of the for
    loop and return the matching handler. Otherwise, return None.

    Parameters
    ----------
    cmip_var: str
        The CMIP variable to CMORize.
    e3sm_vars : List[str]
        The list of E3SM variables from the input files to use for CMORizing.
    handlers : List[Dict[str, Any]]
        The list of VarHandler objects as dictionaries, defined for a CMIP6
        variable.

    Returns
    -------
    Dict[str, Any]
        The matching handler.

    Raises
    ------
    KeyError
        If a matching handler could not be derived using the input E3SM
        variables.
    """
    matching_handler = None

    for handler in handlers:
        if all(var in e3sm_vars for var in handler["raw_variables"]):
            matching_handler = handler
            break

    if matching_handler is None:
        raise KeyError(
            f"A '{cmip_var}' handler could not be derived using the variables from the "
            "input dataset(s). Check the E3SM variables from the input dataset(s) "
            f"align with the '{cmip_var}` handler(s) defined in `handlers.yaml`."
        )

    return matching_handler


def _get_handlers_by_var() -> Dict[str, List[Dict[str, Any]]]:
    handlers_from_yaml = _get_handlers_from_yaml()
    handlers_from_modules = _get_handlers_from_modules(LEGACY_HANDLER_DIR_PATH)
    all_handlers = {**handlers_from_yaml, **handlers_from_modules}

    return all_handlers


def _get_handlers_from_yaml() -> Dict[str, List[Dict[str, Any]]]:
    """Get VarHandler objects using the `handlers.yaml` file.

    Returns
    -------
    Dict[str, List[Dict[str, Any]]]
        A dictionary, with the key being the CMIP6 variable ID and the value
        being a list of VarHandler objects.
    """
    with open(HANDLER_DEFINITIONS_PATH, "r") as infile:
        handlers_file = yaml.load(infile, yaml.SafeLoader)

    df_in = pd.DataFrame.from_dict(handlers_file)

    handlers = defaultdict(list)
    for row in df_in.itertuples():
        var_handler = VarHandler(
            name=row.name,
            units=row.units,
            raw_variables=row.raw_variables,
            table=row.table,
            formula=row.formula,
            unit_conversion=row.unit_conversion,
            positive=row.positive,
            levels=row.levels,
        ).to_dict()
        handlers[row.name].append(var_handler)

    return dict(handlers)


def _get_handlers_from_modules(path: str) -> Dict[str, List[Dict[str, Any]]]:
    """Gets variable handlers defined in Python modules.

    A Python module variable handler defines information about a variable,
    including `RAW_VARIABLES`, `VAR_NAME`, `VAR_UNITS`, `TABLE`, the `handler()`
    function, `positive` (bool, optional), and `levels` (dictionary, optional).
    It also has a `write_data()` function, where additional processing may or
    may not be performed.

    The output of this function is a dictionary. The parent key is the CMIP name
    of the variable, and the value is a nested dictionary storing key/value
    pairs for "name", "units", "table", "method", "raw_variables", "positive"
    (optional), and "levels" (optional).

    Example output:
    ---------------
    {
        "cesm_mmrbc": {
            "name": "mmrbc",
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
    path: str
        Path to the handler modules.

    Returns
    -------
    Dict[str, List[Dict[str, Any]]]
        A dictionary of a list of dictionaries, with each dictionary defining a
        handler.
    """
    handlers = {}

    for root, _, files in os.walk(path):
        for file in files:
            # FIXME: Checking the file should be done dynamically because static
            # references are fragile. Filename changes won't be picked up here
            # automatically.
            if file.endswith(".py") and file not in [
                "__init__.py",
                "_formulas.py",
                "handler.py",
                "utils.py",
            ]:
                var = file.split(".")[0]
                filepath = os.path.join(root, file)
                module = imp.load_source(var, filepath)

                # NOTE: The value is set to a list with a single dict entry
                # so that it is compatible with the data structure for storing
                # all var handlers (which can have multiple handlers per
                # variable).
                handlers[var] = [
                    {
                        "name": module.VAR_NAME,
                        "units": module.VAR_UNITS,
                        "table": module.TABLE,
                        "method": module.handle,
                        "raw_variables": module.RAW_VARIABLES,
                        "positive": module.POSITIVE
                        if hasattr(module, "POSITIVE")
                        else None,
                        "levels": module.LEVELS if hasattr(module, "LEVELS") else None,
                    }
                ]

    return handlers
