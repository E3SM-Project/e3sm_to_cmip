import abc
from typing import Any, Dict, List, Literal, Optional, TypedDict, Union

import cmor
import numpy as np
import xarray as xr
import yaml

from e3sm_to_cmip.cmor_handlers import FILL_VALUE, _formulas
from e3sm_to_cmip.default import default_handler
from e3sm_to_cmip.lib import handle_variables
from e3sm_to_cmip.util import _get_table_for_non_monthly_freq

# Used by areacella.py
RADIUS = 6.37122e6


class BaseVarHandler(abc.ABC):
    def __init__(
        self,
        name: str,
        units: str,
        raw_variables: List[str],
        table: str,
    ):
        # The CMIP variable name.
        # Example: "cli"
        self.name = name

        # The CMIP6 variable units.
        # Example: "kg kg-1"
        self.units = units

        # The E3SM variable name(s) used in the conversion to the CMIP6
        # variable.
        # Example: ["CLDICE"]
        self.raw_variables = raw_variables

        # The CMOR table filename. This attribute can be updated based on the
        # frequency the user specifies when running e3sm_to_cmip.
        # Source: https://github.com/PCMDI/cmip6-cmor-tables/Tables
        # Example: "CMIP6_Amon.json"
        self.table = table


class VarHandler(BaseVarHandler):
    """A class representing VarHandler.

    It will be used for handlers defined in cmor_handlers and
    default_handlers_info.yml

    Parameters
    ----------
    BaseHandler : abc.ABC
        _description_
    """

    class Levels(TypedDict):
        name: str
        units: str
        e3sm_axis_name: str
        e3sm_axis_bnds: Optional[str]
        time_name: Optional[str]

    def __init__(
        self,
        name: str,
        units: str,
        table: str,
        raw_variables: List[str],
        unit_conversion: Optional[str] = None,
        formula: Optional[str] = None,
        positive: Optional[Literal["up", "down"]] = None,
        levels: Optional[Levels] = None,
    ):
        super().__init__(name, units, raw_variables, table)

        # An optional unit conversion formula of the final output data.
        # Example: "g-to-kg"
        self.unit_conversion = unit_conversion

        # An optional conversion formula for calculating the final output data.
        # Usually this is defined if is arithmetic involving more than one raw
        # E3SM variable.
        # Example: "FSNTOA - FSNT + FLNT" for "rlut" variable
        self.formula = formula

        if self.unit_conversion is not None and self.formula is not None:
            raise ValueError(
                f"'{self.name}' handler has `unit_conversion` and `formula` attributes "
                "defined, but only one should be defined."
            )

        if self.formula is not None:
            try:
                self.formula_method = getattr(_formulas, self.name)
            except AttributeError:
                raise AttributeError(
                    f"The formula method for the '{self.name}' VarHandler "
                    "is not defined. Define a function of the same name in the "
                    "`_formulas.py` file."
                )

        # The "positive" directive to CMOR enables data providers to specify
        # the direction that they have assumed in fields  (i.g. radiation fluxes
        # has up or down direction) passed to CMOR. If their direction is
        # opposite that required by CMIP6 (as specified in the CMOR tables),
        # then CMOR will multiply the field by -1, reversing the sign for
        # consistency with the data request.
        self.positive = positive

        # Distinguishes model-level variables, which require remapping from the
        # default model level to the level defined in the levels dictionary.
        self.levels = levels

        # Output data for CMORizing.
        self.output_data: Optional[np.ndarray] = None

    def __eq__(self, other):
        if isinstance(self, other.__class__):
            return (
                self.name == other.name
                and self.units == other.units
                and self.table == other.table
                and self.raw_variables == other.raw_variables
                and self.unit_conversion == other.unit_conversion
                and self.formula == other.formula
                and self.positive == other.positive
                and self.levels == other.levels
            )

        return False

    def __str__(self):
        return yaml.dump(self.__dict__, default_flow_style=False, sort_keys=False)

    def to_dict(self) -> Dict[str, Any]:
        """
        Return __dict__ with additional entries to support existing e3sm_to_cmip
        functions.

        Additional entries include:
          - "method" key mapped to the `self.handle()` method,
          - "levels" key mapped to `self.levels`

        Returns
        -------
        Dict[str, Any]
            __dict__ with additional entries.
        """
        # TODO: Remove this method e3sm_to_cmip functions parse VarHandler
        # objects instead of its dictionary representation.
        return {**self.__dict__, "method": self.cmorize}

    def cmorize(
        self,
        infiles: List[str],  # command-line arg (--input-path)
        tables: str,  # command-line arg (--tables-path)
        user_input_path: str,  # command-line arg (--user-metadata)
        **kwargs,
    ):
        # TODO: Refactor calls to `default_handler()` and `handle_variables()`
        # TODO: Replace **kwargs with explicit method parameters.
        # -------------------------------------------------
        # serial=None,  # command-line arg (--serial)
        # logdir=None,  # command-line arg (--logdir)
        # simple=False,  # command-line arg (--simple)
        # outpath=None,  # command-line arg (--output-path)
        if self.unit_conversion is not None:
            return default_handler(
                infiles,
                tables=tables,
                user_input_path=user_input_path,
                raw_variables=self.raw_variables,
                name=self.name,
                units=self.units,
                table=kwargs.get("table", self.table),
                unit_conversion=self.unit_conversion,
                serial=kwargs.get("serial"),
                logdir=kwargs.get("logdir"),
                simple=kwargs.get("simple"),
                outpath=kwargs.get("outpath"),
            )
        elif self.formula is not None:
            return handle_variables(
                infiles=infiles,
                raw_variables=self.raw_variables,
                write_data=self._write_data,
                outvar_name=self.name,
                outvar_units=self.units,
                table=kwargs.get("table", self.table),
                tables=tables,
                metadata_path=user_input_path,
                serial=kwargs.get("serial"),
                positive=self.positive,
                levels=self.levels,
                axis=kwargs.get("axis"),
                logdir=kwargs.get("logdir"),
                simple=kwargs.get("simple"),
                outpath=kwargs.get("outpath"),
            )

    def _write_data(
        self,
        varid: int,
        data: Dict[str, Union[xr.DataArray, np.ndarray]],
        timeval: float,
        timebnds: List[List[float]],
        index: int,
        **kwargs,
    ):
        # TODO: Replace **kwargs with explicit method parameters.

        # Convert all variable arrays to np.ndarray to ensure compatibility with
        # the CMOR library. For handlers that use `self.levels`, the array data
        # structures are already `np.ndarray` due to additional processing.
        for var in self.raw_variables:
            if isinstance(data[var], xr.DataArray):
                data[var] = data[var].values  # type: ignore

        if self.formula is not None:
            outdata = self.formula_method(data, index)
        else:
            outdata = data[self.raw_variables[0]][index, :]

        # Replace `np.nan` with static FILL_VALUE
        outdata[np.isnan(outdata)] = FILL_VALUE

        if kwargs["simple"]:
            return outdata

        cmor.write(varid, outdata, time_vals=timeval, time_bnds=timebnds)

        if self.levels and self.levels.get("name") in [
            "standard_hybrid_sigma",
            "standard_hybrid_sigma_half",
        ]:
            cmor.write(
                data["ips"],
                data["ps"],
                time_vals=timeval,
                time_bnds=timebnds,
                store_with=varid,
            )

    def _update_table_ref(self, freq: str, realm: str, cmip_tables_path: str):
        """
        Update the referenced CMIP table for cmorizing based on the selected
        frequency and realm.

        Parameters
        ----------
        freq : str
            The frequency for CMORizing, which is used to derive the appropriate
            CMIP6 table.
        realm : str
            The realm.
        cmip_tables_path : str
            The path to the CMIP6 tables.
        """
        # TODO: Implement logic from
        # `e3sm_to_cmip.util._get_table_for_non_monthly_freq()`.
        self.table = _get_table_for_non_monthly_freq(
            self.name, self.table, freq, realm, cmip_tables_path
        )
