import json

import pytest

from e3sm_to_cmip.util import (
    _get_table_for_freq,
    _get_table_for_non_monthly_freq,
    _get_table_info,
    _is_table_supported_by_realm,
    _use_highfreq_handler,
)


class TestGetTableForNonMonthlyFreq:
    @pytest.fixture(autouse=True)
    def setup(self, tmp_path):
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

    def test_raises_error_if_table_does_not_exist_for_freq(self):
        with pytest.raises(ValueError):
            assert _get_table_for_non_monthly_freq(
                var="pr",
                base_table="CMIP6_Lmon.json",
                freq="3hr",
                realm="atm",
                tables_path=None,  # type: ignore
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
        # Create temporary directory to save CMOR tables.
        self.tables_path = tmp_path / "cmip6-cmor-tables"
        self.tables_path.mkdir()

        # Create temporary directory to save CMOR tables.
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
