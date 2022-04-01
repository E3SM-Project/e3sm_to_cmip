import json
import os
from unittest import result

import pytest

from e3sm_to_cmip import cmor_handlers
from e3sm_to_cmip.cmor_handlers import pr_highfreq
from e3sm_to_cmip.default import default_handler
from e3sm_to_cmip.util import (
    _get_available_handlers,
    _get_complex_handlers,
    _get_default_handlers,
    _get_table_for_freq,
    _get_table_for_non_monthly_freq,
    _get_table_info,
    _is_table_supported_by_realm,
    _use_highfreq_handler,
    load_handlers,
)


class TestLoadHandlers:
    @pytest.fixture(autouse=True)
    def setup(self, tmp_path):
        self.handlers_path = os.path.dirname(cmor_handlers.__file__)

        # Create temporary directory to save files.
        self.tables_path = tmp_path / "cmip6-cmor-tables"
        self.tables_path.mkdir()
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

    def test_raises_error_if_variable_does_not_have_a_handler(self):
        with pytest.raises(KeyError):
            load_handlers(
                self.handlers_path,
                self.tables_path,
                var_list=["undefined_handler"],
                freq="mon",
                realm="atm",
                simple=False,
            )


    # TODO: Figure out an efficient way to write this test.
    # This test is tricky to create because it loops through all of the
    # default handlers in `default_handlers_info.yaml` and complex handlers
    # under `/cmor_handlers`, then checks the table names of each handler to
    # make sure it exists in the tables directory (a temp one created through
    # pytest). Currently, we would have to create all of the handler JSON
    # tables under the temp directory for the test to pass.
    @pytest.mark.xfail
    def test_returns_all_handlers_when_all_string_in_var_list(self):
        handlers = load_handlers(
            self.handlers_path,
            self.tables_path,
            var_list=["all"],
            freq="3hr",
            realm="atm",
            simple=False,
        )

        # Only check an individual handler because there are many.
        result_default = next(
            (handler for handler in handlers if handler["cmip_name"] == "clt"), None
        )
        expected_default = {
            "cmip_name": "clt",
            "units": "%",
            "table": "CMIP6_3hr.json",
            "method": default_handler,
            "raw_variables": ["CLDTOT"],
            "unit_conversion": "1-to-%",
        }
        assert result_default == expected_default

        result_complex = next(
            (handler for handler in handlers if handler["cmip_name"] == "pr"), None
        )
        # Update "method" value to the name of the method because the memory
        # address changes with imports, so the handler dict won't align with the
        # expected output.
        result_complex["method"] = result_complex["method"].__name__
        expected_complex = {
            "cmip_name": "pr",
            "units": "kg m-2 s-1",
            "table": "CMIP6_3hr.json",
            "method": pr_highfreq.handle.__name__,
            "raw_variables": ["PRECT"],
            "positive": None,
            "levels": None,
        }

        assert result_complex == expected_complex

    def test_returns_handlers_with_simple_mode_and_no_highfreq(self):
        handlers = load_handlers(
            self.handlers_path,
            self.tables_path,
            var_list=["clt", "pr"],
            freq="mon",
            realm="atm",
            simple=True,
        )

        # Only check an individual handler because there are many.
        result_default = next(
            (handler for handler in handlers if handler["cmip_name"] == "clt"), None
        )
        expected_default = {
            "cmip_name": "clt",
            "units": "%",
            "table": "CMIP6_Amon.json",
            "method": default_handler,
            "raw_variables": ["CLDTOT"],
            "unit_conversion": "1-to-%",
        }
        assert result_default == expected_default

        result_complex = next(
            (handler for handler in handlers if handler["cmip_name"] == "pr"), None
        )
        # Update "method" value to the name of the method because the memory
        # address changes with imports, so the handler dict won't align with the
        # expected output.
        result_complex["method"] = result_complex["method"].__name__
        expected_complex = {
            "cmip_name": "pr",
            "units": "kg m-2 s-1",
            "table": "CMIP6_Amon.json",
            "method": pr_highfreq.handle.__name__,
            "raw_variables": ["PRECC", "PRECL"],
            "positive": None,
            "levels": None,
        }

        assert result_complex == expected_complex

    def test_returns_handlers_for_selected_vars_with_highfreq(self):
        handlers = load_handlers(
            self.handlers_path,
            self.tables_path,
            var_list=["clt", "pr"],
            freq="3hr",
            realm="atm",
            simple=False,
        )

        # Only check an individual handler because there are many.
        result_default = next(
            (handler for handler in handlers if handler["cmip_name"] == "clt"), None
        )
        expected_default = {
            "cmip_name": "clt",
            "units": "%",
            "table": "CMIP6_3hr.json",
            "method": default_handler,
            "raw_variables": ["CLDTOT"],
            "unit_conversion": "1-to-%",
        }
        assert result_default == expected_default

        result_complex = next(
            (handler for handler in handlers if handler["cmip_name"] == "pr"), None
        )
        # Update "method" value to the name of the method because the memory
        # address changes with imports, so the handler dict won't align with the
        # expected output.
        result_complex["method"] = result_complex["method"].__name__
        expected_complex = {
            "cmip_name": "pr",
            "units": "kg m-2 s-1",
            "table": "CMIP6_3hr.json",
            "method": pr_highfreq.handle.__name__,
            "raw_variables": ["PRECT"],
            "positive": None,
            "levels": None,
        }

        assert result_complex == expected_complex


class TestGetAvailableHandlers:
    def test_returns_all_default_and_complex_handlers(self):
        handlers_path = os.path.dirname(cmor_handlers.__file__)
        result = _get_available_handlers(handlers_path)

        # Only check an individual handler because there are many.
        result_default = result["abs550aer"]
        expected_default = {
            "cmip_name": "abs550aer",
            "units": "1",
            "table": "CMIP6_AERmon.json",
            "method": default_handler,
            "raw_variables": ["AODABS"],
        }
        assert result_default == expected_default

        result_complex = result["pr_highfreq"]
        # Update "method" value to the name of the method because the memory
        # address changes with imports, so the handler dict won't align with the
        # expected output.
        result_complex["method"] = result_complex["method"].__name__
        expected_complex = {
            "cmip_name": "pr",
            "units": "kg m-2 s-1",
            "table": "CMIP6_day.json",
            "method": pr_highfreq.handle.__name__,
            "raw_variables": ["PRECT"],
            "positive": None,
            "levels": None,
        }

        assert result_complex == expected_complex


class TestGetDefaultHandlers:
    def test_returns_default_handler_from_yaml_file(self):
        result = _get_default_handlers()

        # Only check an individual handler because there are many.
        handler = result["abs550aer"]
        expected = {
            "cmip_name": "abs550aer",
            "units": "1",
            "table": "CMIP6_AERmon.json",
            "method": default_handler,
            "raw_variables": ["AODABS"],
        }

        assert handler == expected


class TestGetComplexHandlers:
    def test_returns_complex_handler_from_cmor_handlers_dir(self):
        handlers_path = os.path.dirname(cmor_handlers.__file__)
        result = _get_complex_handlers(handlers_path)

        handler = result["pr_highfreq"]

        # Update "method" value to the name of the method because the memory
        # address changes with imports, so the handler dict won't align with the
        # expected output.
        handler["method"] = handler["method"].__name__
        expected = {
            "cmip_name": "pr",
            "units": "kg m-2 s-1",
            "table": "CMIP6_day.json",
            "method": pr_highfreq.handle.__name__,
            "raw_variables": ["PRECT"],
            "positive": None,
            "levels": None,
        }

        assert handler == expected


class TestGetTableForNonMonthlyFreq:
    @pytest.fixture(autouse=True)
    def setup(self, tmp_path):
        # Create temporary directory to save files.
        self.tables_path = tmp_path / "cmip6-cmor-tables"
        self.tables_path.mkdir()
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

    def test_raises_error_if_table_does_not_exist_for_freq(self):
        with pytest.raises(ValueError):
            assert _get_table_for_non_monthly_freq(
                var="pr",
                base_table="CMIP6_Lmon.json",
                freq="3hr",
                realm="atm",
                tables_path=None,
            )

    def test_raises_error_if_table_could_not_be_found_in_table_path(self):
        with pytest.raises(ValueError):
            _get_table_for_non_monthly_freq(
                var="pr",
                base_table="CMIP6_Amon.json",
                freq="day",
                realm="atm",
                tables_path=self.tables_path,
            )

    def test_raises_error_if_variable_is_not_included_in_the_table(self):
        with pytest.raises(KeyError):
            _get_table_for_non_monthly_freq(
                var="undefined_var",
                base_table="CMIP6_Amon.json",
                freq="3hr",
                realm="atm",
                tables_path=self.tables_path,
            )

    def test_raises_error_if_table_is_not_supported_by_realm(self):
        with pytest.raises(ValueError):
            _get_table_for_non_monthly_freq(
                var="pr",
                base_table="CMIP6_Amon.json",
                freq="3hr",
                realm="ice",
                tables_path=self.tables_path,
            )

    def test_returns_table_name(self):
        result = _get_table_for_non_monthly_freq(
            var="pr",
            base_table="CMIP6_Amon.json",
            freq="3hr",
            realm="atm",
            tables_path=self.tables_path,
        )

        assert result == "CMIP6_3hr.json"


class TestGetTableForFreq:
    def test_returns_table_for_freq(self):
        assert _get_table_for_freq("CMIP6_Amon.json", "3hr") == "CMIP6_3hr.json"

    def test_returns_none_if_table_is_not_supported_for_non_monthly_freqs(self):
        assert _get_table_for_freq("CMIP6_Lmon.json", "3hr") is None

    def test_returns_none_if_table_includes_fx_string_and_frequency_is_non_monthly(
        self,
    ):
        assert _get_table_for_freq("fxCMIP6_Lmon.json", "3hr") is None


class TestGetTableInfo:
    @pytest.fixture(autouse=True)
    def setup(self, tmp_path):
        # Create temporary directory to save files.
        self.tables_path = tmp_path / "cmip6-cmor-tables"
        self.tables_path.mkdir()
        file_path = f"{self.tables_path}/CMIP6_3hr.json"
        self.table_info = {
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
            }
        }

        with open(file_path, "w") as json_file:
            json.dump(
                self.table_info,
                json_file,
            )

    def test_raises_error_if_cmip6_table_does_not_exist(self):
        with pytest.raises(ValueError):
            _get_table_info(self.tables_path, "CMIP6_invalid_table.json")

    def test_returns_table_info(self):
        result = _get_table_info(self.tables_path, "CMIP6_3hr.json")
        expected = self.table_info

        assert result == expected


class TestUseHighFreqHandler:
    def test_returns_true_if_variable_supports_high_freq(self):
        highfreq_vars = ["pr", "rlut"]
        highfreqs = ["day", "6hrLev", "6hrPlev", "6hrPlevPt", "3hr", "1hr"]

        for var in highfreq_vars:
            for freq in highfreqs:
                assert _use_highfreq_handler(var, freq) is True

    def test_returns_false_if_variable_does_not_support_high_freq(self):
        assert _use_highfreq_handler("pfull", "day") is False

    def test_returns_false_if_frequency_is_not_high(self):
        assert _use_highfreq_handler("pr", "mon") is False


class TestIsTableSupportedByRealm:
    def test_returns_true_if_table_is_supported_by_realm(self):
        realm_to_tables = {
            "atm": [
                "CMIP6_Amon.json",
                "CMIP6_day.json",
                "CMIP6_3hr.json",
                "CMIP6_6hrLev.json",
                "CMIP6_6hrPlev.json",
                "CMIP6_6hrPlevPt.json",
                "CMIP6_AERmon.json",
                "CMIP6_AERday.json",
                "CMIP6_AERhr.json",
                "CMIP6_CFmon.json",
                "CMIP6_CF3hr.json",
                "CMIP6_CFday.json",
                "CMIP6_fx.json",
            ],
            "lnd": ["CMIP6_Lmon.json", "CMIP6_LImon.json"],
            "ocn": ["CMIP6_Omon.json", "CMIP6_Ofx.json"],
            "ice": ["CMIP6_SImon.json"],
        }

        for realm, tables in realm_to_tables.items():
            for table in tables:
                assert _is_table_supported_by_realm(table, realm) is True

    def test_returns_false_if_table_is_not_supported_by_realm(self):
        realm = "atm"
        table = "CMIP6_Lmon.json"

        assert _is_table_supported_by_realm(realm, table) is False
