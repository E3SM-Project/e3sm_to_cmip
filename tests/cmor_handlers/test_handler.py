import json
import os

import pytest

from e3sm_to_cmip import cmor_handlers
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


class TestCmorizeMethod:
    @pytest.mark.xfail
    def test_cmorizes_serial_and_returns_output_variable_name(self):
        assert 0

    @pytest.mark.xfail
    def test_returns_output_variable_with_simple_mode(self):
        assert 0

    @pytest.mark.xfail
    def test_returns_error_if_unable_to_find_input_files_for_variables(self):
        assert 0

    @pytest.mark.xfail
    def test_cmorizes_variables(self):
        assert 0

    @pytest.mark.xfail
    def test_updates_table_reference_based_on_input_freq_and_realm(self):
        assert 0
