import json
import os
from pathlib import Path

import numpy as np
import pytest
import xarray as xr

from e3sm_to_cmip import cmor_handlers, resources
from e3sm_to_cmip.cmor_handlers import _formulas
from e3sm_to_cmip.cmor_handlers.handler import VarHandler


class TestVarHandler:
    @pytest.fixture(autouse=True)
    def setup(self, tmp_path):
        self.handlers_path = os.path.dirname(cmor_handlers.__file__)

        # Create temporary directory to save CMOR tables.
        self.tables_path = tmp_path / "cmip6-cmor-tables"
        self.tables_path.mkdir()

        # Create a CMOR table for testing.
        file_path = f"{self.tables_path}/CMIP6_3hr.json"
        with open(file_path, "w") as json_file:
            json.dump(
                {
                    "variable_entry": {
                        "pr": {
                            "frequency": "3hr",
                            "modeling_realm": "atmos",
                            "standard_name": "precipitation_flux",
                            "units": "kg m-2 s-1",
                            "cell_methods": "area: time: mean",
                            "cell_measures": "area: areacella",
                            "long_name": "Precipitation",
                            "comment": "includes both liquid and solid phases",
                            "dimensions": "longitude latitude time",
                            "out_name": "pr",
                            "type": "real",
                            "positive": "",
                            "valid_min": "",
                            "valid_max": "",
                            "ok_min_mean_abs": "",
                            "ok_max_mean_abs": "",
                        },
                        "clt": {
                            "frequency": "3hr",
                            "modeling_realm": "atmos",
                            "standard_name": "cloud_area_fraction",
                            "units": "%",
                            "cell_methods": "area: time: mean",
                            "cell_measures": "area: areacella",
                            "long_name": "Total Cloud Cover Percentage",
                            "comment": "Total cloud area fraction (reported as a percentage) for the whole atmospheric column, as seen from the surface or the top of the atmosphere. Includes both large-scale and convective cloud.",
                            "dimensions": "longitude latitude time",
                            "out_name": "clt",
                            "type": "real",
                            "positive": "",
                            "valid_min": "",
                            "valid_max": "",
                            "ok_min_mean_abs": "",
                            "ok_max_mean_abs": "",
                        },
                    }
                },
                json_file,
            )

    def test__init__(self):
        VarHandler(
            raw_variables=["CLDTOT"],
            name="cmip_name",
            units="units",
            table="CMIP6_AMON.json",
            unit_conversion="1-to-%",
        )

    def test__init__raises_error_unit_conversion_and_formula_are_both_defined(self):
        with pytest.raises(ValueError):
            VarHandler(
                name="pr",
                units="kg m-2 s-1",
                raw_variables=["PRECC", "PRECL"],
                table="CMIP6_Amon.json",
                unit_conversion="g to kg",
                formula="(PRECC + PRECL) * 1000.0",
                positive=None,
                levels=None,
            )

    def test___init__raises_error_if_formula_attr_is_set_but_no_formula_method_is_found(
        self,
    ):
        with pytest.raises(AttributeError):
            VarHandler(
                name="non-existent-var",
                units="W m-2",
                raw_variables=["FSNTOA", "FSNT", "FLNT"],
                table="CMIP6_Amon.json",
                formula="FSNTOA - FSNT + FLNT",
            )

    def test__init__derives_formula_method_attr_based_on_name_and_formula_attrs(self):
        handler = VarHandler(
            name="pr",
            units="kg m-2 s-1",
            raw_variables=["PRECC", "PRECL"],
            table="CMIP6_Amon.json",
            unit_conversion=None,
            formula="(PRECC + PRECL) * 1000.0",
            positive=None,
            levels=None,
        )

        assert handler.formula_method == _formulas.pr

    def test__eq__(self):
        levels: VarHandler.Levels = {
            "name": "atmosphere_sigma_coordinate",
            "units": "1",
            "e3sm_axis_name": "lev",
            "e3sm_axis_bnds": "ilev",
            "time_name": "time2",
        }
        obj1 = VarHandler(
            name="pr",
            units="kg m-2 s-1",
            raw_variables=["PRECC", "PRECL"],
            table="CMIP6_3hr.json",
            unit_conversion=None,
            formula="(PRECC + PRECL) * 1000.0",
            levels=levels,
        )

        obj2 = VarHandler(
            name="pr",
            units="kg m-2 s-1",
            raw_variables=["PRECC", "PRECL"],
            table="CMIP6_3hr.json",
            unit_conversion=None,
            formula="(PRECC + PRECL) * 1000.0",
            levels=levels,
        )
        assert obj1 == obj2

        obj3 = VarHandler(
            name="clt",
            units="%",
            raw_variables=["CLDTOT"],
            table="CMIP6_3hr.json",
            unit_conversion="1-to-%",
        )

        assert not obj1 == obj3

    def test__str__(self):
        levels: VarHandler.Levels = {
            "name": "atmosphere_sigma_coordinate",
            "units": "1",
            "e3sm_axis_name": "lev",
            "e3sm_axis_bnds": "ilev",
            "time_name": "time2",
        }
        obj1 = VarHandler(
            name="pr",
            units="kg m-2 s-1",
            raw_variables=["PRECC", "PRECL"],
            table="CMIP6_3hr.json",
            unit_conversion=None,
            formula="(PRECC + PRECL) * 1000.0",
            levels=levels,
        )

        assert obj1.__str__() == (
            "name: pr\nunits: kg m-2 s-1\nraw_variables:\n- PRECC\n- PRECL\n"
            "table: CMIP6_3hr.json\nunit_conversion: null\n"
            "formula: (PRECC + PRECL) * 1000.0\n"
            "formula_method: !!python/name:e3sm_to_cmip.cmor_handlers._formulas.pr ''\n"
            "positive: null\nlevels:\n  name: atmosphere_sigma_coordinate\n  "
            "units: '1'\n  e3sm_axis_name: lev\n  e3sm_axis_bnds: ilev\n  "
            "time_name: time2\noutput_data: null\n"
        )

    @pytest.mark.xfail
    def test__update_table_ref_updates_table_attr(self):
        assert 0


class TestCmorizeMethod:
    @pytest.fixture(autouse=True)
    def setup(self, tmp_path):
        self.tables_path = tmp_path / "cmip6-cmor-tables"
        self.tables_path.mkdir()

        file_path = f"{self.tables_path}/CMIP6_Lmon.json"
        with open(file_path, "w") as json_file:
            json.dump(
                {
                    "variable_entry": {
                        "mrsos": {
                            "dimensions": "time lat lon",
                            "long_name": "Moisture in Upper Portion of Soil Column",
                            "standard_name": "mass_content_of_water_in_soil_layer",
                            "units": "kg m-2",
                            "cell_methods": "area: mean where land time: mean",
                            "cell_measures": "area: areacella",
                            "comment": "Mass of water in upper 10cm of soil layer.",
                        }
                    }
                },
                json_file,
            )

        file_path = f"{self.tables_path}/CMIP6_Amon.json"
        with open(file_path, "w") as json_file:
            json.dump(
                {
                    "variable_entry": {
                        "pfull": {
                            "dimensions": (
                                "time2 standard_hybrid_sigma latitude longitude"
                            ),
                            "long_name": "Pressure on Model Levels",
                            "standard_name": "air_pressure",
                            "units": "Pa",
                        },
                        "phalf": {
                            "dimensions": (
                                "time2 standard_hybrid_sigma_half latitude longitude"
                            ),
                            "long_name": "Pressure on Model Half-Levels",
                            "standard_name": "air_pressure",
                            "units": "Pa",
                        },
                    }
                },
                json_file,
            )

        self.output_path = tmp_path / "output"
        self.output_path.mkdir()
        self.cmor_log_dir = tmp_path / "logs"
        self.cmor_log_dir.mkdir()

    @pytest.mark.xfail
    def test_cmorizes_serial_and_returns_output_variable_name(self):
        assert 0

    def test_writes_and_validates_output_in_simple_mode(self, monkeypatch):
        handler = VarHandler(
            name="mrsos",
            units="kg m-2",
            raw_variables=["SOILWATER_10CM"],
            table="CMIP6_Lmon.json",
        )
        data = xr.DataArray(
            np.ones((1, 2, 2)),
            dims=("time", "lat", "lon"),
            coords={
                "time": [0],
                "lat": [-1.0, 1.0],
                "lon": [0.0, 2.0],
            },
            name="SOILWATER_10CM",
        )
        ds = xr.Dataset(
            {
                "SOILWATER_10CM": data,
                "lat_bnds": (("lat", "nbnd"), np.array([[-2.0, 0.0], [0.0, 2.0]])),
                "lon_bnds": (("lon", "nbnd"), np.array([[-1.0, 1.0], [1.0, 3.0]])),
                "time_bounds": (("time", "nbnd"), np.array([[0.0, 1.0]])),
            }
        )

        def mock_get_mfdataset(*_args, **_kwargs):
            return ds

        monkeypatch.setattr(handler, "_get_mfdataset", mock_get_mfdataset)

        result = handler.cmorize(
            vars_to_filepaths={"SOILWATER_10CM": ["SOILWATER_10CM_185001_185412.nc"]},
            tables_path=str(self.tables_path),
            metadata_path="unused.json",
            cmor_log_dir=str(self.cmor_log_dir),
            simple=True,
            output_path=str(self.output_path),
        )

        # Filename mirrors the zppy input convention (YYYYMM_YYYYMM).
        output_file = self.output_path / "mrsos_185001_185412.nc"
        assert result is True
        assert output_file.exists()

        with xr.open_dataset(output_file) as output_ds:
            assert "mrsos" in output_ds.data_vars
            np.testing.assert_array_equal(output_ds["mrsos"].values, data.values)
            # Coords and bounds are auto-copied via dim-subset match.
            assert "lat" in output_ds.coords
            assert "lon" in output_ds.coords
            assert "lat_bnds" in output_ds.data_vars
            assert "lon_bnds" in output_ds.data_vars
            assert "time_bounds" in output_ds.data_vars
            # Raw variable must not leak into the simple output.
            assert "SOILWATER_10CM" not in output_ds.variables
            # CMIP6 table attrs are propagated; units come from the handler.
            mrsos_attrs = output_ds["mrsos"].attrs
            assert mrsos_attrs["units"] == "kg m-2"
            assert mrsos_attrs["long_name"] == (
                "Moisture in Upper Portion of Soil Column"
            )
            assert mrsos_attrs["standard_name"] == "mass_content_of_water_in_soil_layer"
            assert mrsos_attrs["cell_methods"] == ("area: mean where land time: mean")
            assert mrsos_attrs["cell_measures"] == "area: areacella"

    def test_simple_mode_rewrites_time_to_bnds_midpoint(self, monkeypatch):
        handler = VarHandler(
            name="mrsos",
            units="kg m-2",
            raw_variables=["SOILWATER_10CM"],
            table="CMIP6_Lmon.json",
        )
        data = xr.DataArray(
            np.ones((2, 2, 2)),
            dims=("time", "lat", "lon"),
            coords={
                # E3SM stamps at end-of-interval; expect rewrite to midpoint.
                "time": [31.0, 59.0],
                "lat": [-1.0, 1.0],
                "lon": [0.0, 2.0],
            },
            name="SOILWATER_10CM",
        )
        ds = xr.Dataset(
            {
                "SOILWATER_10CM": data,
                "lat_bnds": (("lat", "nbnd"), np.array([[-2.0, 0.0], [0.0, 2.0]])),
                "lon_bnds": (("lon", "nbnd"), np.array([[-1.0, 1.0], [1.0, 3.0]])),
                "time_bounds": (
                    ("time", "nbnd"),
                    np.array([[0.0, 31.0], [31.0, 59.0]]),
                ),
            }
        )
        ds["time"].attrs["units"] = "days since 1850-01-01"

        monkeypatch.setattr(handler, "_get_mfdataset", lambda *a, **k: ds)

        handler.cmorize(
            vars_to_filepaths={"SOILWATER_10CM": ["SOILWATER_10CM_185001_185412.nc"]},
            tables_path=str(self.tables_path),
            metadata_path="unused.json",
            cmor_log_dir=str(self.cmor_log_dir),
            simple=True,
            output_path=str(self.output_path),
        )

        with xr.open_dataset(
            self.output_path / "mrsos_185001_185412.nc", decode_times=False
        ) as out:
            np.testing.assert_array_equal(out["time"].values, [15.5, 45.0])
            # Bounds themselves are preserved untouched.
            np.testing.assert_array_equal(
                out["time_bounds"].values, [[0.0, 31.0], [31.0, 59.0]]
            )
            # Units attribute survives the rewrite.
            assert out["time"].attrs["units"] == "days since 1850-01-01"

    def test_cmorize_simple_requires_output_path(self, monkeypatch):
        handler = VarHandler(
            name="mrsos",
            units="kg m-2",
            raw_variables=["SOILWATER_10CM"],
            table="CMIP6_Lmon.json",
        )
        monkeypatch.setattr(handler, "_get_mfdataset", lambda *a, **k: xr.Dataset())

        with pytest.raises(ValueError, match="output_path is required"):
            handler.cmorize(
                vars_to_filepaths={"SOILWATER_10CM": ["dummy.nc"]},
                tables_path=str(self.tables_path),
                metadata_path="unused.json",
                cmor_log_dir=str(self.cmor_log_dir),
                simple=True,
            )

    def test_simple_output_filename_falls_back_to_index(self):
        handler = VarHandler(
            name="mrsos",
            units="kg m-2",
            raw_variables=["SOILWATER_10CM"],
            table="CMIP6_Lmon.json",
        )
        # YYYYMM_YYYYMM suffix present -> mirrored on output.
        assert (
            handler._simple_output_filename("SOILWATER_10CM_185001_185412.nc", 0)
            == "mrsos_185001_185412.nc"
        )
        # No time-range suffix (e.g. fx data) -> indexed fallback.
        assert handler._simple_output_filename("LANDFRAC.nc", 3) == "mrsos_0003.nc"

    @pytest.mark.parametrize(
        ("name", "formula", "include_p0"),
        [
            ("pfull", "hyam * p0 + hybm * ps", True),
            ("phalf", "hyai * p0 + hybi * ps", False),
        ],
    )
    def test_simple_mode_carries_available_hybrid_sigma_support(
        self, monkeypatch, name, formula, include_p0
    ):
        handler = VarHandler(
            name=name,
            units="Pa",
            raw_variables=["hyai", "hybi", "hyam", "hybm", "PS"],
            table="CMIP6_Amon.json",
            formula=formula,
        )
        ds = self._get_hybrid_sigma_dataset(include_p0)
        monkeypatch.setattr(handler, "_get_mfdataset", lambda *a, **k: ds)

        handler.cmorize(
            vars_to_filepaths={
                var: ["T_185001_185012.nc"] for var in handler.raw_variables
            },
            tables_path=str(self.tables_path),
            metadata_path="unused.json",
            cmor_log_dir=str(self.cmor_log_dir),
            simple=True,
            output_path=str(self.output_path),
        )

        with xr.open_dataset(self.output_path / f"{name}_185001_185012.nc") as out:
            expected_support = {
                "PS",
                "hyam",
                "hybm",
                "hyai",
                "hybi",
                "lev",
                "ilev",
            }
            assert expected_support <= set(out.variables)
            assert ("P0" in out.variables) is include_p0
            if include_p0:
                assert out["P0"].attrs == ds["P0"].attrs
                assert out["P0"].item() == ds["P0"].item()

    def test_hybrid_sigma_support_preserves_source_variables(self):
        handler = VarHandler(
            name="pfull",
            units="Pa",
            raw_variables=["hyai", "hybi", "hyam", "hybm", "PS"],
            table="CMIP6_Amon.json",
            formula="hyam * p0 + hybm * ps",
        )
        ds = self._get_hybrid_sigma_dataset(include_p0=True)
        ds["P0"].encoding["dtype"] = "float64"
        ds_out = xr.Dataset()

        handler._carry_hybrid_sigma_support(ds, ds_out)

        for name in ("lev", "ilev", "hyam", "hybm", "hyai", "hybi", "PS", "P0"):
            xr.testing.assert_identical(ds_out[name], ds[name])
            assert (name in ds_out.coords) == (name in ds.coords)
        assert ds_out["P0"].encoding == ds["P0"].encoding

    def test_incomplete_hybrid_sigma_support_does_not_synthesize_p0(self):
        handler = VarHandler(
            name="pfull",
            units="Pa",
            raw_variables=["hyai", "hybi", "hyam", "hybm", "PS"],
            table="CMIP6_Amon.json",
            formula="hyam * p0 + hybm * ps",
        )
        ds = self._get_hybrid_sigma_dataset(include_p0=False).drop_vars("PS")
        ds_out = xr.Dataset()

        handler._carry_hybrid_sigma_support(ds, ds_out)

        assert "P0" not in ds_out.variables
        assert not ds_out.variables

    def test_simple_mode_uses_bundled_limon_table(self, monkeypatch):
        handler = VarHandler(
            name="snc",
            units="%",
            raw_variables=["FSNO"],
            table="CMIP6_LImon.json",
            unit_conversion="1-to-%",
        )
        ds = xr.Dataset(
            {
                "FSNO": (
                    ("time", "lat", "lon"),
                    np.array([[[0.5]]]),
                ),
                "time_bounds": (("time", "nbnd"), np.array([[0.0, 1.0]])),
            },
            coords={"time": [1.0], "lat": [0.0], "lon": [0.0]},
        )
        monkeypatch.setattr(handler, "_get_mfdataset", lambda *a, **k: ds)
        resources_path = Path(resources.__file__).parent

        handler.cmorize(
            vars_to_filepaths={"FSNO": ["FSNO_185001_185012.nc"]},
            tables_path=str(resources_path),
            metadata_path="unused.json",
            cmor_log_dir=str(self.cmor_log_dir),
            simple=True,
            output_path=str(self.output_path),
        )

        with xr.open_dataset(self.output_path / "snc_185001_185012.nc") as out:
            np.testing.assert_array_equal(out["snc"].values, [[[50.0]]])
            assert out["snc"].attrs["long_name"] == "Snow Area Percentage"

    def _get_hybrid_sigma_dataset(self, include_p0: bool) -> xr.Dataset:
        ds = xr.Dataset(
            {
                "hyam": (("lev",), [0.1, 0.2]),
                "hybm": (("lev",), [0.9, 0.8]),
                "hyai": (("ilev",), [0.0, 0.15, 0.3]),
                "hybi": (("ilev",), [1.0, 0.85, 0.7]),
                "PS": (
                    ("time2", "lat", "lon"),
                    np.full((1, 1, 1), 100000.0),
                ),
                "time_bnds": (("time2", "nbnd"), [[0.0, 1.0]]),
            },
            coords={
                "time2": [1.0],
                "lat": [0.0],
                "lon": [0.0],
                "lev": [900.0, 800.0],
                "ilev": [1000.0, 850.0, 700.0],
            },
        )
        if include_p0:
            ds["P0"] = xr.DataArray(
                100000.0,
                attrs={"units": "Pa", "long_name": "Reference pressure"},
            )

        return ds

    @pytest.mark.xfail
    def test_returns_error_if_unable_to_find_input_files_for_variables(self):
        assert 0

    @pytest.mark.xfail
    def test_cmorizes_variables(self):
        assert 0

    @pytest.mark.xfail
    def test_regrids_hybrid_to_pressure_level(self):
        assert 0

    @pytest.mark.xfail
    def test_updates_table_reference_based_on_input_freq_and_realm(self):
        assert 0
