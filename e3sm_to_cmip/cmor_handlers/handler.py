import json
import logging
import os
import re
from typing import Any, KeysView, Literal, TypedDict

import cmor
import numpy as np
import xarray as xr
import xcdat as xc
import yaml

from e3sm_to_cmip import LEGACY_XARRAY_MERGE_SETTINGS
from e3sm_to_cmip._logger import _setup_child_logger
from e3sm_to_cmip.cmor_handlers import FILL_VALUE, _formulas
from e3sm_to_cmip.util import _get_table_for_non_monthly_freq

logger = _setup_child_logger(__name__)

# The names for valid hybrid sigma levels.
HYBRID_SIGMA_LEVEL_NAMES = [
    "standard_hybrid_sigma",
    "standard_hybrid_sigma_half",
]
# A list of valid time dimension names, which is used to check if
# a output CMIP variable has a time dimension based on the CMOR table. If the
# CMIP variable does have a time dimension, subsequent CMOR operations are
# handled appropriately.
TIME_DIMS = ["time", "time1", "time2"]

# Bounds-dimension names that appear on `*_bnds`/`*_bounds` coord variables
# alongside the spatial/temporal dim (e.g. lat_bnds has dims (lat, nbnd),
# E3SM land time_bounds has dims (time, hist_interval)). Treated as
# transparent when matching input variables to the output variable's dims in
# simple mode.
BOUNDS_DIM_NAMES = {"nbnd", "bnds", "d2", "nv", "hist_interval"}

# Matches the zppy time-series filename convention `..._YYYYMM_YYYYMM.nc`,
# used to mirror the input time range on simple-mode output filenames.
_TIME_RANGE_RE = re.compile(r"_(\d{6})_(\d{6})\.nc$", re.IGNORECASE)

# Type alias for the dictionary representation of a VarHandler object.
VarHandlerDict = dict[str, Any]


class BaseVarHandler:
    def __init__(
        self,
        name: str,
        units: str,
        raw_variables: list[str],
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
        e3sm_axis_bnds: str | None
        time_name: str | None

    def __init__(
        self,
        name: str,
        units: str,
        table: str,
        raw_variables: list[str],
        unit_conversion: str | None = None,
        formula: str | None = None,
        positive: Literal["up", "down"] | None = None,
        levels: Levels | None = None,
    ):
        super().__init__(name, units, raw_variables, table)

        # An optional unit conversion formula of the final output data.
        # Example: "g-to-kg"
        self.unit_conversion = unit_conversion

        # If unit_conversion is specified, then there should be a single
        # E3SM raw variable to convert units for.
        if self.unit_conversion is not None and len(self.raw_variables) > 1:
            raise ValueError(
                f"The handler for {self.name} has a unit_conversion specified ",
                f"({self.unit_conversion}) and should only have on raw_variable "
                f"defined. More than one raw_variable defined ({self.raw_variables})",
            )

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
            except AttributeError as e:
                raise AttributeError(
                    f"The formula method for the '{self.name}' VarHandler "
                    "is not defined. Define a function of the same name in the "
                    "`_formulas.py` file."
                ) from e

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
        self.output_data: np.ndarray | None = None

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

    def to_dict(self) -> VarHandlerDict:
        """
        Return __dict__ with additional entries to support existing e3sm_to_cmip
        functions.

        Additional entries include:
          - "method" key mapped to the `self.handle()` method,
          - "levels" key mapped to `self.levels`

        Returns
        -------
        VarHandlerDict
            __dict__ with additional entries.
        """
        # TODO: Remove this method e3sm_to_cmip functions parse VarHandler
        # objects instead of its dictionary representation.
        return {**self.__dict__, "method": self.cmorize}

    def cmorize(
        self,
        vars_to_filepaths: dict[str, list[str]],
        tables_path: str,
        metadata_path: str,
        cmor_log_dir: str,
        table: str | None = None,
        simple: bool = False,
        output_path: str | None = None,
    ) -> bool:
        """CMORizes a list of E3SM raw variables to a CMIP variable.

        Parameters
        ----------
        vars_to_filepaths : dict[str, list[str]]
            A dictionary mapping E3SM raw variables to a list of filepath(s).
        tables_path : str
            The path to directory containing CMOR Tables directory.
        metadata_path : str
            The path to user json file for CMIP6 metadata
        cmor_log_dir : str
            The directory that stores the CMOR logs.
        table : str | None
            The CMOR table filename, derived from a custom `freq`, by default
            None.
        simple : bool
            If True, skip the full CMOR pipeline and write a plain netCDF file
            containing the output variable and its coordinates. Requires
            `output_path`.
        output_path : str | None
            Directory to write simple-mode output files into. Required when
            `simple=True`; ignored otherwise.

        Returns
        -------
        bool
            If CMORizing was successful, return True, else False.
        """
        logger.info(f"{self.name}: Starting CMORizing")

        if table is not None:
            self.table = table

        # If at least one E3SM raw variable has no file(s) found, return False to
        # represent a failed operation.
        if not self._all_vars_have_filepaths(vars_to_filepaths):
            return False

        table_abs_path = os.path.join(tables_path, self.table)
        time_dim: str | None = self._get_var_time_dim(table_abs_path)

        if simple:
            if output_path is None:
                raise ValueError(
                    "output_path is required when calling cmorize(simple=True)"
                )
            return self._write_simple(
                vars_to_filepaths, output_path, time_dim, table_abs_path
            )

        # Create the logging directory and setup the CMOR module globally before
        # running any CMOR functions.
        # ----------------------------------------------------------------------
        self._setup_cmor_module(self.name, tables_path, metadata_path, cmor_log_dir)

        # Assuming all year ranges are the same for every variable.
        # TODO: Is this a good keep this legacy assumption?
        num_files_per_variable = len(list(vars_to_filepaths.values())[0])

        # CMORize and write out via cmor.write.
        # ----------------------------------------------------------------------
        for index in range(num_files_per_variable):
            logger.info(
                f"{self.name}: loading E3SM variables {vars_to_filepaths.keys()}"
            )
            ds = self._get_mfdataset(vars_to_filepaths, index, time_dim)

            # Create the base CMOR variable object using CMOR axis objects,
            # which are all set globally in the CMOR module with unique IDs (later
            # referenced when writing out to a file with cmor.write()).
            logger.info(f"{self.name}: creating CMOR variable with CMOR axis objects.")
            cmor_axis_id_map, cmor_ips_id = self._get_cmor_axis_ids_and_ips_id(
                ds=ds, time_dim=time_dim
            )
            cmor_axis_ids = list(cmor_axis_id_map.values())
            cmor_var_id = cmor.variable(
                self.name,
                units=self.units,
                axis_ids=cmor_axis_ids,
                positive=self.positive,
            )

            if time_dim is None:
                is_cmor_successful = self._cmor_write(ds, cmor_var_id)
            else:
                is_cmor_successful = self._cmor_write_with_time(
                    ds, cmor_var_id, time_dim, cmor_ips_id
                )

            ds.close()

        # NOTE: It is important to close the CMOR module AFTER CMORizing all of
        # the variables. Otherwise, the IDs of cmor objects gets wiped after
        # every loop.
        cmor.close()
        logger.debug(
            f"{self.name}: CMORized and file write complete, closing CMOR I/O."
        )

        return is_cmor_successful

    def _write_simple(
        self,
        vars_to_filepaths: dict[str, list[str]],
        output_path: str,
        time_dim: str | None,
        table_abs_path: str,
    ) -> bool:
        """Write simple (non-CMORized) output netCDF files."""
        # Sort to match _get_mfdataset's ordering so the filename derived
        # below points to the same input file _get_mfdataset reads at `index`.
        primary_filepaths = sorted(list(vars_to_filepaths.values())[0])
        num_files_per_variable = len(primary_filepaths)
        var_attrs = self._read_simple_var_attrs(table_abs_path)

        for index in range(num_files_per_variable):
            ds = self._get_mfdataset(
                vars_to_filepaths, index, time_dim, convert_hybrid_lev=False
            )
            da_output = self._get_output_data_array(ds)

            ds_out = xr.Dataset(attrs=ds.attrs)
            # Use `.data` to avoid xarray's ambiguity error when constructing a
            # variable from `(dims, data)` with a DataArray object.
            ds_out[self.name] = (da_output.dims, da_output.data)
            ds_out[self.name].attrs = var_attrs
            ds_out[self.name].encoding["_FillValue"] = FILL_VALUE

            output_dims = set(da_output.dims)
            skip = set(self.raw_variables) | {self.name}
            for name, var in ds.variables.items():
                if name in skip or name in ds_out.variables or not var.dims:
                    continue
                if set(var.dims) - BOUNDS_DIM_NAMES <= output_dims:
                    if name in ds.coords:
                        ds_out.coords[name] = var
                    else:
                        ds_out[name] = var

            self._carry_hybrid_sigma_support(ds, ds_out)
            self._rewrite_time_to_bnds_midpoint(ds_out, time_dim)

            output_filename = self._simple_output_filename(
                primary_filepaths[index], index
            )
            ds_out.to_netcdf(os.path.join(output_path, output_filename))

            ds.close()
            ds_out.close()

        return True

    def _simple_output_filename(self, input_filepath: str, index: int) -> str:
        """Mirror the zppy `_YYYYMM_YYYYMM.nc` suffix on the input filename;
        fall back to a zero-padded index when no time range is present (e.g.
        time-invariant `fx` inputs).
        """
        match = _TIME_RANGE_RE.search(os.path.basename(input_filepath))
        if match:
            return f"{self.name}_{match.group(1)}_{match.group(2)}.nc"

        return f"{self.name}_{index:04d}.nc"

    def _carry_hybrid_sigma_support(self, ds: xr.Dataset, ds_out: xr.Dataset) -> None:
        """Copy complete hybrid-sigma metadata into simple-mode output.

        Hybrid formula inputs can be excluded by the raw-variable filter or
        dimension-subset matching. Copy the complete support set whenever the
        input carries the required ``hyai, hybi, hyam, hybm`` coefficients and
        a surface pressure variable (``PS`` in EAM, ``ps`` in EAMxx). Preserve
        an input ``P0`` when present.
        """
        if not self._has_hybrid_sigma_levels(ds):
            return

        support_names = [
            "lev",
            "lev_bnds",
            "ilev",
            "ilev_bnds",
            "hyam",
            "hybm",
            "hyai",
            "hybi",
            "P0",
        ]

        ps_name = _formulas.get_surface_pressure_name(ds)
        if ps_name is not None:
            support_names.append(ps_name)

        for name in support_names:
            if name not in ds.variables or name in ds_out.variables:
                continue
            if name in ds.coords:
                ds_out.coords[name] = ds[name]
            else:
                ds_out[name] = ds[name]

    def _rewrite_time_to_bnds_midpoint(
        self, ds_out: xr.Dataset, time_dim: str | None
    ) -> None:
        """Set ``time`` to the midpoint of its bounds in place.

        E3SM stamps monthly time series at the end of each interval; CF tooling
        (and CMOR mode) expects the coord to fall inside its bounds. Rewriting
        to the bounds midpoint puts simple-mode output on the same time axis
        as CMOR mode. No-op if the variable is time-invariant or the dataset
        carries no bounds variable.
        """
        if time_dim is None or time_dim not in ds_out.coords:
            return
        try:
            bnds_key = self._get_time_bnds_key(ds_out.data_vars.keys())
        except KeyError:
            return

        bnds = ds_out[bnds_key]
        other_dims = [d for d in bnds.dims if d != time_dim]
        if not other_dims:
            return

        midpoints = bnds.mean(dim=other_dims).values
        attrs = dict(ds_out[time_dim].attrs)
        encoding = dict(ds_out[time_dim].encoding)
        ds_out[time_dim] = (time_dim, midpoints)
        ds_out[time_dim].attrs = attrs
        ds_out[time_dim].encoding.update(encoding)

    def _read_simple_var_attrs(self, table_path: str) -> dict[str, str]:
        """Pull CF/CMIP6-canonical attrs for ``self.name`` from a CMIP6 table.

        ``units`` is taken from the handler (the canonical unit string the
        runner targets) rather than the table, to stay consistent with what
        ``_get_output_data_array`` produces after any unit conversion.
        """
        with open(table_path) as f:
            entry = json.load(f).get("variable_entry", {}).get(self.name, {})

        keys = (
            "long_name",
            "standard_name",
            "comment",
            "cell_methods",
            "cell_measures",
        )
        attrs = {k: entry[k] for k in keys if entry.get(k)}
        attrs["units"] = self.units
        if self.positive:
            attrs["positive"] = self.positive

        return attrs

    def _all_vars_have_filepaths(
        self, vars_to_filespaths: dict[str, list[str]]
    ) -> bool:
        """Checks if all raw variables have filepaths found.

        Parameters
        ----------
        vars_to_filespaths : dict[str, list[str]]
            A dictionary of raw variables to filepaths.

        Returns
        -------
        bool
            True if all variables have filepaths, else False.
        """
        for var, filepaths in vars_to_filespaths.items():
            if len(filepaths) == 0:
                logging.error(f"{var}: Unable to find input files for {var}")
                return False

        return True

    def _setup_cmor_module(
        self, var_name: str, tables_path: str, metadata_path: str, cmor_log_dir: str
    ):
        """Set up the CMOR module before CMORzing variables.

        Parameters
        ----------
        var_name : str
            The name of the variable.
        table : str
            The table filename.
        tables_path : str
            The tables path.
        metadata_path : str
            The metadata path.
        cmor_log_dir : str
            The directory that stores the CMOR logs.
        """
        logfile = os.path.join(cmor_log_dir, var_name + ".log")

        cmor.setup(
            inpath=tables_path, netcdf_file_action=cmor.CMOR_REPLACE, logfile=logfile
        )
        cmor.dataset_json(metadata_path)
        cmor.load_table(self.table)

        logging.info(f"{var_name}: CMOR setup complete")

    def _get_var_time_dim(self, table_path: str) -> str | None:
        """Get the CMIP variable's time dimension, if it exists.

        Parameters
        ----------
        table_path : str
            The absolute path to the CMOR table.

        Returns
        -------
        str | None
            The optional name of the time dimension if it exists for the CMIP
            variable defined in the CMOR table.
        """
        with open(table_path, "r") as inputstream:
            table_info = json.load(inputstream)

        axis_info = table_info["variable_entry"][self.name]["dimensions"].split(" ")

        for dim in TIME_DIMS:
            if dim in axis_info:
                return dim

        return None

    def _get_mfdataset(
        self,
        vars_to_filepaths: dict[str, list[str]],
        index: int,
        time_dim: str | None,
        convert_hybrid_lev: bool = True,
    ) -> xr.Dataset:
        """Get the xr.Dataset using the filepaths for all raw variables.

        Parameters
        ----------
        vars_to_filepaths : dict[str, list[str]]
            A dictionary mapping E3SM raw variables to a list of filepath(s).
        index : int
            The index representing the time range for the file.
            For example, with this list:
                [
                    v2.LR.historical_0101.eam.h0.1850-01.nc,
                    v2.LR.historical_0101.eam.h0.1850-02.nc,
                    ...
                ]
            the value at index 0 is v2.LR.historical_0101.eam.h0.1850-01.nc.
        time_dim : str | None
            Whether or not the output CMIP variable has a time dimension.

        Returns
        -------
        xr.Dataset
            The dataset containing all of the rwar variables.
        """
        # Sort the input filepath names for each variable to ensure time axis data
        # is aligned and in order across variables.
        sorted_v_to_fp: dict[str, list[str]] = {
            var: sorted(vars_to_filepaths[var]) for var in vars_to_filepaths
        }

        all_filepaths = []
        for filepaths in sorted_v_to_fp.values():
            filepath = filepaths[index]
            all_filepaths.append(filepath)

        ds = xc.open_mfdataset(
            all_filepaths,
            add_bounds=["X", "Y"],
            decode_times=False,
            combine="nested",
            data_vars="minimal",
            coords="minimal",
            **LEGACY_XARRAY_MERGE_SETTINGS,
        )

        # If the output CMIP variable has an alternative time dimension name (e.g.,
        # "time2") add that to the xr.Dataset by copying the "time" dimension.
        if time_dim is not None and time_dim != "time":
            with xr.set_options(keep_attrs=True):
                ds = ds.rename({"time": time_dim})

        # Convert lev/ilev from E3SM's stored 1000*(A+B) scaling into the
        # dimensionless A+B form CMOR's `standard_hybrid_sigma` axis expects.
        # Disabled in simple mode so output preserves the E3SM input form
        # (values + units="hPa" + long_name reflecting 1000*(A+B)).
        if convert_hybrid_lev:
            if "lev" in ds:
                ds["lev"] = ds["lev"] / 1000
            if "ilev" in ds:
                ds["ilev"] = ds["ilev"] / 1000

        # If the variable has levels for "sdepth", make sure it has bounds
        # for the "levgrnd" axis using a statically defined list of bound
        # values.
        if self.levels is not None and self.levels["name"] == "sdepth":
            ds["levgrnd_bnds"] = _formulas.LEVGRND_BNDS

        return ds

    def _get_time_bnds_key(self, data_vars: KeysView[Any]) -> str:
        """Get the key for the time bounds.

          * "atm" variables use "time_bnds"
          * "lnd" variables use "time_bounds"

        Parameters
        ----------
        data_vars : KeysView[Any]
            A list of data_var keys in the xr.Dataset.

        Returns
        -------
        str
           The key of the time bounds, or None if it does not exist.

        Raises
        ------
        KeyError
            If no matching time bounds key was found in the dataset.
        """
        if "time_bnds" in data_vars:
            return "time_bnds"
        elif "time_bounds" in data_vars:
            return "time_bounds"

        raise KeyError("No matching time bounds found in the dataset")

    def _get_cmor_axis_ids_and_ips_id(
        self, ds: xr.Dataset, time_dim: str | None
    ) -> tuple[dict[str, int], int | None]:
        """Create the CMOR axes objects, which are set globally in the CMOR module.

        The CMOR ids for "time" and "lev" should be the starting elements of the
        axis ID map to align with how they are defined in CMOR tables.
        Otherwise, CMOR will need to reorder the dimensions and add the message
        below to the "history" attribute of a variable: "2023-11-02T21:06:00Z
        altered by CMOR: Reordered dimensions, original order: lat lon time.",
        which can produce unwanted side-effects with the structure/shape of
        the final array axes.

        Parameters
        ----------
        ds : xr.Dataset
            The dataset containing axes information.
        time_dim : str | None
            An optional time dimension for the output CMIP variable (if set).

        Returns
        -------
        tuple[dict[str, int], int | None]
            A tuple with the first element being a dictionary (value is the
            CMOR axis and key is the ID of the CMOR axis), and the second
            element being the CMOR zfactor ID for ips if the dataset and handler
            have hybrid sigma levels.

            Example:
                ({"time": 0, "lev": 1, "lat": 2, "lon": 3}, 4)
        """
        axis_id_map: dict[str, int] = {}
        cmor_ips_id = None

        if time_dim is not None:
            units = ds[time_dim].attrs["units"]
            axis_id_map["time"] = cmor.axis(time_dim, units=units)

        if self.levels is not None:
            axis_id_map["lev"] = self._get_cmor_lev_axis_id(ds)

        # Datasets will always have a "lat" and "lon" dimension.
        axis_id_map["lat"] = cmor.axis(
            "latitude",
            units=ds["lat"].units,
            coord_vals=ds["lat"].values,
            cell_bounds=ds["lat_bnds"].values,
        )
        axis_id_map["lon"] = cmor.axis(
            "longitude",
            units=ds["lon"].units,
            coord_vals=ds["lon"].values,
            cell_bounds=ds["lon_bnds"].values,
        )

        if self._has_hybrid_sigma_levels(ds):
            self._set_cmor_zfactor_for_hybrid_levels(ds, axis_id_map)

            cmor_ips_id = self._set_and_get_cmor_zfactor_ips_id(axis_id_map)

        return axis_id_map, cmor_ips_id

    def _get_cmor_lev_axis_id(self, ds: xr.Dataset) -> cmor.axis:
        """Get the CMOR lev axis using the xr.Dataset.

        Parameters
        ----------
        ds : xr.Dataset
            The xr.Dataset containing the `lev` axis data.

        Returns
        -------
        int
            The lev CMOR axis id.
        """
        axis_name = self.levels["e3sm_axis_name"]  # type: ignore
        axis_bnds = self.levels.get("e3sm_axis_bnds")  # type: ignore

        coord_vals = ds[axis_name].values

        if axis_bnds is not None:
            cell_bounds = ds[axis_bnds].values
        else:
            cell_bounds = None

        lev_id = cmor.axis(
            table_entry=self.levels["name"],  # type: ignore
            units=self.levels["units"],  # type: ignore
            coord_vals=coord_vals,
            cell_bounds=cell_bounds,
        )

        return lev_id

    def _has_hybrid_sigma_levels(self, ds: xr.Dataset):
        hybrid_sigma_levels = ["hyai", "hybi", "hybm", "hyam"]
        has_coefficients = set(hybrid_sigma_levels).issubset(ds.data_vars)

        # Surface pressure is matched by name rather than membership because
        # EAM writes "PS" and EAMxx writes "ps".
        has_surface_pressure = _formulas.get_surface_pressure_name(ds) is not None

        return has_coefficients and has_surface_pressure

    def _set_cmor_zfactor_for_hybrid_levels(
        self, ds: xr.Dataset, cmor_axis_id_map: dict[str, cmor.axis]
    ):
        lev_id = cmor_axis_id_map["lev"]
        lev_name = self.levels["name"]  # type: ignore

        if lev_name == "standard_hybrid_sigma":
            a_name = "a"
            b_name = "b"
            a_bounds = ds["hyai"].values
            b_bounds = ds["hybi"].values
        elif lev_name == "standard_hybrid_sigma_half":
            a_name = "a_half"
            b_name = "b_half"
            a_bounds = None
            b_bounds = None

        cmor.zfactor(
            zaxis_id=lev_id,
            zfactor_name=a_name,
            axis_ids=[lev_id],
            zfactor_values=ds["hyam"].values,
            zfactor_bounds=a_bounds,
        )

        cmor.zfactor(
            zaxis_id=lev_id,
            zfactor_name=b_name,
            axis_ids=[lev_id],
            zfactor_values=ds["hybm"].values,
            zfactor_bounds=b_bounds,
        )

        cmor.zfactor(
            zaxis_id=lev_id,
            zfactor_name="p0",
            units="Pa",
            zfactor_values=_formulas.P0_VALUE,
        )

    def _set_and_get_cmor_zfactor_ips_id(
        self, cmor_axis_id_map: dict[str, cmor.axis]
    ) -> int:
        """Creates ips as a cmor.zfactor and returns its global CMOR id.

        Parameters
        ----------
        cmor_axis_id_map : dict[str, cmor.axis]
            A dictionary mapping the name of a CMOR axis set globally to its CMOR axis
            ID.

        Returns
        -------
        int
            The ips CMOR axis id.
        """
        name = "ps"

        # NOTE: This maintains the legacy name from pfull.py and phalf.py. I'm
        # not sure why this is done and what the difference is with "ps".
        if self.name in ["pfull", "phalf"]:
            name = "ps2"

        cmor_ips_id = cmor.zfactor(
            zaxis_id=cmor_axis_id_map["lev"],
            zfactor_name=name,
            axis_ids=[
                cmor_axis_id_map["time"],
                cmor_axis_id_map["lat"],
                cmor_axis_id_map["lon"],
            ],
            units="Pa",
        )

        return cmor_ips_id

    def _cmor_write(
        self,
        ds: xr.Dataset,
        cmor_var_id: int,
    ) -> bool:
        """Writes the output CMIP variable.

        Returns
        -------
        bool
            True if write succeeded, False otherwise.
        """
        output_data = self._get_output_data(ds)

        try:
            cmor.write(var_id=cmor_var_id, data=output_data)
        except Exception as e:
            logger.error(f"Error writing variable {self.name} to file: {e}")

            return False

        return True

    def _cmor_write_with_time(
        self,
        ds: xr.Dataset,
        cmor_var_id: int,
        time_dim: str,
        cmor_ips_id: int | None,
    ) -> bool:
        """Writes the output CMIP variable and IPS variable (if it exists).

        Parameters
        ----------
        ds : xr.Dataset
            The dataset containing the E3SM raw variable and axes info.
        cmor_var_id : int
            The CMOR variable ID.
        time_dim : str
            The key of the time dimension.
        cmor_ips_id : int | None
            The optional CMOR zfactor ips ID.

        Returns
        -------
        bool
            True if write succeeded, False otherwise.
        """
        output_data = self._get_output_data(ds)

        time_vals = ds[time_dim].values
        time_bnds_key = self._get_time_bnds_key(ds.data_vars.keys())
        time_bnds = ds[time_bnds_key].values

        logger.info(
            f"{self.name}: time span {time_bnds[0][0]:1.1f} - {time_bnds[-1][-1]:1.1f}"
        )
        logger.info(f"{self.name}: Writing variable to file...")

        try:
            cmor.write(
                var_id=cmor_var_id,
                data=output_data,
                time_vals=time_vals,
                time_bnds=time_bnds,
            )
        except Exception as e:
            logger.error(e)

            return False
        else:
            if cmor_ips_id is not None:
                logger.info(f"{self.name}: Writing IPS variable to file...")
                try:
                    cmor.write(
                        var_id=cmor_ips_id,
                        data=_formulas.get_surface_pressure(ds).values,
                        time_vals=time_vals,
                        time_bnds=time_bnds,
                        store_with=cmor_var_id,
                    )
                except Exception as e:
                    logger.error(e)

                    return False

        return True

    def _get_output_data(self, ds: xr.Dataset) -> np.ndarray:
        """Get the variable output data.

        The variable output data is retrieved by:
          1. Unit conversion (defined in handler)
          2. Formula (defined in handler)
          3. The first and only variable in the dataset.

        Afterwards, the missing values represented by `np.nan` are replaced
        with the E3SM defined `FILL_VALUE`, then the xr.DataArray is converted
        to an `np.ndarray`. It is important that an `np.ndarray` is returned
        because `cmor.write` does not support Xarray objects.

        Parameters
        ----------
        ds : xr.Dataset
            The dataset containing the raw variables.

        Returns
        -------
        np.ndarray
            The final variable output data to pass to ``cmor.write``.
        """
        da_output = self._get_output_data_array(ds)
        output = da_output.values

        return output

    def _get_output_data_array(self, ds: xr.Dataset) -> xr.DataArray:
        """Get the output data as an ``xr.DataArray`` with NaNs filled.

        Parameters
        ----------
        ds : xr.Dataset
            The dataset containing raw variables used to derive output.

        Returns
        -------
        xr.DataArray
            The output variable data with NaNs replaced by ``FILL_VALUE``.
        """
        if self.unit_conversion is not None:
            var = ds[self.raw_variables[0]]
            da_output = _formulas.convert_units(var, self.unit_conversion)
        elif self.formula is not None:
            da_output = self.formula_method(ds)
        else:
            da_output = ds[self.raw_variables[0]]

        return da_output.fillna(FILL_VALUE)

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
