import copy
import os
from collections import defaultdict
from importlib.machinery import SourceFileLoader
from typing import Any, Literal, get_args

import pandas as pd
import yaml

from e3sm_to_cmip import (
    HANDLER_DEFINITIONS_PATH,
    LEGACY_HANDLER_DIR_PATH,
    MPAS_HANDLER_DIR_PATH,
)
from e3sm_to_cmip._logger import _setup_child_logger
from e3sm_to_cmip.cmor_handlers.handler import VarHandler
from e3sm_to_cmip.util import FREQUENCY_TO_CMIP_TABLES, _get_table_for_non_monthly_freq

logger = _setup_child_logger(__name__)

# Type aliases
Frequency = Literal["mon", "day", "6hrLev", "6hrPlev", "6hrPlevPt", "3hr", "1hr"]

Realm = Literal["atm", "lnd", "fx"]
REALMS = get_args(Realm)

MPASRealm = Literal["mpaso", "mpassi", "SImon", "Omon"]
MPAS_REALMS = get_args(MPASRealm)

# Used by areacella.py
RADIUS = 6.37122e6


def load_all_handlers(
        realm: Realm | MPASRealm, cmip_vars: list[str], mip_era: str,
) -> list[dict[str, Any]]:
    """Loads variable handlers based on a list of variable names.

    This function is used specifically for printing out the handler information
    (info_mode).

    Parameters
    ----------
    realm: Realm | MPASRealm
        The realm.
    cmip_vars : list[str]
        The list of CMIP6 variables to CMORize.
    mip_era : str
        CMIP era (cmip6 or cmip6plus)
    Returns
    -------
    list[dict[str, Any]]:
        A list of the dictionary representation of VarHandler objects.

    Raises
    ------
    KeyError
        If no handlers are defined for a CMIP6 variable in `handlers.yaml`.
    """
    handlers_by_var: dict[str, list[dict[str, Any]]] = _get_handlers_by_var()

    missing_handlers: list[str] = []

    if realm in REALMS:
        handlers: list[dict[str, Any]] = []

        for var in cmip_vars:
            var_handler = handlers_by_var.get(var)

            if var_handler is None:
                missing_handlers.append(var)
                continue

            for var_handlers in var_handler:
                if type(var_handlers["table"]) is list: 
                    if mip_era == "cmip6" and len(var_handlers["table"]) > 1:
                        var_handlers["table"] = str(var_handlers["table"][0])

                    elif mip_era == "cmip6plus" and len(var_handlers["table"]) > 1:
                        var_handlers["table"] = str(var_handlers["table"][1])
            
        handlers = handlers + var_handler

        if len(missing_handlers) > 0:
            logger.warning(
                f"No handlers are defined for the variables: {missing_handlers}. "
                "Make sure at least one variable handler is defined for each of these "
                f"variables in `{HANDLER_DEFINITIONS_PATH}`."
            )
    else:
        handlers = _get_mpas_handlers(cmip_vars, mip_era)

       
    return handlers


def _get_mpas_handlers(cmip_vars: list[str], mip_era: str):
    """Get MPAS variable handlers using the list of CMIP variables.

    All current MPAS variable handlers are defined as modules and there is only
    1 handler module defined per variable.

    Parameters
    ----------
    cmip_vars : list[str]
        The list of CMIP6 variables to CMORize.

    Returns
    -------
    KeyError
        If no handlers are defined for the MPAS CMIP6 variable.
    """
    handlers = _get_handlers_from_modules(MPAS_HANDLER_DIR_PATH)

    derived_handlers: list[dict[str, Any]] = []
    missing_handlers: list[str] = []

    for var in cmip_vars:
        var_handler = handlers.get(var)

        if var_handler is None:
            missing_handlers.append(var)
            continue
        for var_handlers in var_handler:
            if type(var_handlers["table"]) is list:
                if mip_era == "cmip6" and len(var_handlers["table"]) > 1:
                    var_handlers["table"] = str(var_handlers["table"][0])

                elif mip_era == "cmip6plus" and len(var_handlers["table"]) > 1:
                    var_handlers["table"] = str(var_handlers["table"][1])
        derived_handlers.append(var_handler[0])

    if len(missing_handlers) > 0:
        logger.warning(
            f"No variable handlers are defined for {missing_handlers}. Make sure at "
            "least one variable handler is defined for each of these variables in "
            f"`{MPAS_HANDLER_DIR_PATH}`."
        )

    return derived_handlers


def derive_handlers(
    cmip_tables_path: str,
    cmip_vars: list[str],
    e3sm_vars: list[str],
    freq: Frequency,
    realm: Realm | MPASRealm,
    mip_era: str,
) -> list[dict[str, Any]]:
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
    cmip_vars : list[str]
        The list of CMIP6 variables to CMORize.
    e3sm_vars : list[str]
        The list of E3SM variables from the input files to use for CMORizing.
    freq : Frequency
        The frequency used to derive the appropriate CMIP6 table for each
        variable handler.
    realm : str
        The realm.
    mip_era : str
        CMIP era 

    Returns
    -------
    list[dict[str, Any]]:
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
    handlers_by_var: dict[str, list[dict[str, Any]]] = _get_handlers_by_var()
    derived_handlers: list[dict[str, Any]] = []

    # Stores variable names that are missing handlers or the handler cannot
    # be derived using the input E3SM variables.
    missing_handlers: list[str] = []
    cannot_derive: list[str] = []

    for var in cmip_vars:
        var_handlers = handlers_by_var.get(var)

        # If no handlers are defined for the variable, add it to the missing
        # handlers list and continue to the next variable.
        # This can happen if the variable is not defined in `handlers.yaml` or
        # if the variable is not defined in any of the legacy handler modules.
        if var_handlers is None:
            missing_handlers.append(var)
            continue

        for var_handler in var_handlers:
            if type(var_handler["table"]) is list:
                if mip_era == "cmip6" and len(var_handler["table"]) > 1:
                    var_handler["table"]= str(var_handler["table"][0])
                    
                elif mip_era == "cmip6plus" and len(var_handler["table"]) > 1:
                    var_handler["table"] = str(var_handler["table"][1])
                # If more than one handler table corresponding to different mip_era is
                # defined for a variable, the coresponding handler table for the mip_era is selected. 
                # Handler table is assumed to be list type and cmip6 is first item and cmip6plus
                # else if only one mip era table is defined for a variable nothing is done

        derived_handler = _derive_handler(
            var_handlers, freq, realm, cmip_tables_path, e3sm_vars
        )

        # If no handler could be derived, add it to the cannot_derive list.
        # This can happen if the handler has no matching CMIP table for the
        # requested frequency, or if the handler's raw E3SM variables do not
        # match the input E3SM variables.
        if derived_handler is None:
            cannot_derive.append(var)
            continue

        derived_handlers.append(derived_handler)

    if len(missing_handlers) > 0:
        logger.warning(
            f"No handlers are defined for the variables: {missing_handlers}. "
            "Make sure handlers are defined for these variables in `handlers.yaml`."
        )

    if len(cannot_derive) > 0:
        logger.warning(
            f"No handlers could be derived for the variables: {cannot_derive}. "
            "Make sure the input E3SM datasets have the variables needed for "
            "derivation."
        )

    return derived_handlers


def _get_handlers_by_var() -> dict[str, list[dict[str, Any]]]:
    """Retrieve all variable handlers from YAML and legacy module sources.

    This function combines handlers loaded from a YAML configuration and from
    legacy Python modules, merging them into a single dictionary keyed by
    variable name.

    Returns
    -------
    dict[str, list[dict[str, Any]]]
        A dictionary mapping variable names to a list of handler definitions,
        where each handler is represented as a dictionary containing handler
        metadata and logic.
    """
    handlers_from_yaml = _get_handlers_from_yaml()
    handlers_from_modules = _get_handlers_from_modules(LEGACY_HANDLER_DIR_PATH)
    all_handlers = {**handlers_from_yaml, **handlers_from_modules}

    return all_handlers


def _get_handlers_from_yaml() -> dict[str, list[dict[str, Any]]]:
    """Get VarHandler objects using the `handlers.yaml` file.

    Returns
    -------
    dict[str, list[dict[str, Any]]]
        A dictionary, with the key being the CMIP6 variable ID and the value
        being a list of VarHandler objects.
    """
    with open(HANDLER_DEFINITIONS_PATH, "r") as infile:
        handlers_file = yaml.load(infile, yaml.SafeLoader)

    df_in = pd.DataFrame.from_dict(handlers_file)

    handlers = defaultdict(list)
    for row in df_in.itertuples():
        var_handler = VarHandler(
            name=row.name,  # type: ignore
            units=row.units,  # type: ignore
            raw_variables=row.raw_variables,  # type: ignore
            table=row.table,  # type: ignore
            formula=row.formula,  # type: ignore
            unit_conversion=row.unit_conversion,  # type: ignore
            positive=row.positive,  # type: ignore
            levels=row.levels,  # type: ignore
        ).to_dict()
        handlers[row.name].append(var_handler)

    return dict(handlers)  # type: ignore


def _get_handlers_from_modules(path: str) -> dict[str, list[dict[str, Any]]]:
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
    dict[str, list[dict[str, Any]]]
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
                module = _get_handler_module(var, filepath)

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


def _get_handler_module(module_name: str, module_path: str):
    """Get the variable handler Python module.

    Parameters
    ----------
    module_name : str
        The name of the module, which should be the key of the variable (e.g.,
        "orog").
    module_path : str
        The absolute path to the variable handler Python module.

    Returns
    -------
    module
        The module.
    """
    module = SourceFileLoader(module_name, module_path).load_module()

    return module


def _derive_handler(
    var_handlers: list[dict[str, Any]],
    freq: Frequency,
    realm: Realm | MPASRealm,
    cmip_tables_path: str,
    e3sm_vars: list[str],
) -> dict[str, Any] | None:
    """Attempts to derive a handler for a CMIP variable.

    The function first filters the handlers to those compatible with the
    requested frequency. It then tries to find a handler whose required raw
    E3SM variables are present in the input E3SM variable list. If no compatible
    handler is found, and no handlers match the requested frequency, it attempts
    to update the handler's table to match the requested frequency and tries again.

    Parameters
    ----------
    var_handlers : list[dict[str, Any]]
        List of variable handler dictionaries.
    freq : Frequency
        The requested output frequency.
    realm : Realm | MPASRealm
        The realm.
    cmip_tables_path : str
        Path to the CMIP6 tables.
    e3sm_vars : list[str]
        List of available E3SM variables.

    Returns
    -------
    dict[str, Any] | None
        The derived handler dictionary if found, otherwise None.
    """
    # Step 1: Filter handlers by frequency and attempt to derive a handler.
    filtered_handlers = _select_handlers_for_freq(var_handlers, freq)
    derived_handler = _find_handler_by_e3sm_vars(e3sm_vars, filtered_handlers)

    if derived_handler is not None:
        return derived_handler

    # Step 2: Fallback for non-monthly frequencies
    # If no suitable handler can be derived, try updating the table for each handler
    # and try deriving a handler again. This maintains backwards compatibility.
    if freq != "mon":
        adjusted_handlers = _adjust_handlers_cmip_table_for_freq(
            var_handlers, freq, realm, cmip_tables_path
        )

        if adjusted_handlers is not None:
            derived_handler = _find_handler_by_e3sm_vars(e3sm_vars, adjusted_handlers)

            if derived_handler is not None:
                return derived_handler

    return None


def _select_handlers_for_freq(
    handlers: list[dict[str, Any]], freq: Frequency
) -> list[dict[str, Any]]:
    """
    Filters a list of variable handlers to include only those with CMIP
    tables that are compatible with the requested frequency.

    For example, if the requested frequency is "mon", it will include
    handlers that use tables like "CMIP6_Amon.json". If the requested
    frequency is "day", it will include handlers that use tables like
    "CMIP6_Aday.json" or "CMIP6_Amon.json" (if the handler can be
    adjusted to match the frequency).

    Parameters
    ----------
    handlers : list[dict[str, Any]]
        The list of variable handlers to filter.
    freq : Frequency
        The requested output frequency (e.g., "mon", "day", "1hr", etc.).

    Returns
    -------
    list[dict[str, Any]]
        A filtered list of variable handlers that match the requested frequency.
    """
    handlers_filtered = []

    for handler in handlers:
        handler_table = handler["table"]

        if table_matches_freq(freq, handler_table):
            handlers_filtered.append(handler)

    return handlers_filtered


def table_matches_freq(freq: str, handler_table: str) -> bool:
    """
    Checks if the requested frequency is compatible with the CMIP table
    frequency defined by the handler.

    Parameters
    ----------
    freq : str
        The requested output frequency (e.g., "mon", "day", "1hr", etc.).
    handler_table : str
        The name of the CMIP6 table (e.g., "CMIP6_Amon.json") used by the handler.

    Returns
    -------
    bool
        True if the table frequency matches the requested frequency, False otherwise.
    """
    valid_tables = FREQUENCY_TO_CMIP_TABLES.get(freq, [])

    if handler_table in valid_tables:
        return True

    logger.debug(
        f"Handler table '{handler_table}' is not compatible with frequency '{freq}'."
    )
    return False


def _find_handler_by_e3sm_vars(
    e3sm_vars: list[str], handlers: list[dict[str, Any]]
) -> dict[str, Any] | None:
    """Finds a handler a CMIP variable based on the input E3SM variables.

    This function loops through a list of VarHandler objects defined for a
    CMIP6 variable. It checks if any of the handler's raw E3SM variables exist
    in the list of input variables. If there is a match, break out of the for
    loop and return the matching handler. Otherwise, return None.

    Parameters
    ----------
    e3sm_vars : list[str]
        The list of E3SM variables from the input files to use for CMORizing.
    handlers : list[dict[str, Any]]
        The list of VarHandler objects as dictionaries, defined for a CMIP6
        variable.

    Returns
    -------
    dict[str, Any] | None
        A derived handler.

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

    return matching_handler


def _adjust_handlers_cmip_table_for_freq(
    var_handlers: list[dict[str, Any]],
    freq: Frequency,
    realm: Realm | MPASRealm,
    cmip_tables_path: str,
) -> list[dict[str, Any]] | None:
    """Update the 'table' field of each handler for the requested frequency and realm.

    This function is used as a fallback when no handler matches the requested
    frequency directly. It updates the handler's table to the appropriate CMIP6
    table for the given frequency and realm, then checks if a handler can be
    derived using the available E3SM variables.

    Parameters
    ----------
    var_handlers : list[dict[str, Any]]
        List of handler dictionaries.
    freq : Frequency
        Requested output frequency.
    realm : Realm | MPASRealm
        Realm.
    cmip_tables_path : str
        Path to CMIP6 tables.

    Returns
    -------
    list[dict[str, Any]] | None
        Handlers with updated 'table' fields, if any handlers could be adjusted.
        If no handlers could be adjusted, returns None.
    """
    handlers_new = []

    for var_handler in var_handlers:
        # Create a deep copy of the handler to avoid modifying the original
        # This is necessary because the handler may be used in multiple places
        # and we want to keep the original intact.
        handler_copy = copy.deepcopy(var_handler)

        # Update the table to match the requested frequency and realm
        handler_copy["table"] = _get_table_for_non_monthly_freq(
            handler_copy["name"],
            handler_copy["table"],
            freq,
            realm,
            cmip_tables_path,
        )
        handlers_new.append(handler_copy)

    if len(handlers_new) == 0:
        logger.debug(
            f"No handlers could be adjusted for frequency '{freq}' in realm '{realm}'. "
            "Make sure the handlers are defined correctly in `handlers.yaml`."
        )
        return None

    return handlers_new
