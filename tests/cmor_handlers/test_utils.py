import json

import pytest

from e3sm_to_cmip.cmor_handlers import _formulas
from e3sm_to_cmip.cmor_handlers.handler import VarHandler
from e3sm_to_cmip.cmor_handlers.mpas_vars import so, uo
from e3sm_to_cmip.cmor_handlers.utils import derive_handlers, load_all_handlers
from e3sm_to_cmip.cmor_handlers.vars import orog, sftlf


class TestLoadAllHandlers:
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
                    }
                },
                json_file,
            )

    def test_prints_logger_warning_if_handler_does_not_exist_for_var(self, caplog):
        load_all_handlers("lnd", ["invalid_var"])

        for record in caplog.records:
            assert record.levelname == "WARNING"

    def test_prints_logger_warning_if_mpas_handler_does_not_exist_for_var(self, caplog):
        load_all_handlers("mpaso", cmip_vars=["invalid_var"])

        for record in caplog.records:
            assert record.levelname == "WARNING"

    def test_updates_CMIP_table_for_variable_based_on_freq_param(self):
        result = load_all_handlers("lnd", cmip_vars=["pr"])
        expected = [
            dict(
                name="pr",
                units="kg m-2 s-1",
                raw_variables=["PRECT"],
                table="CMIP6_day.json",
                unit_conversion=None,
                formula="PRECT * 1000.0",
                formula_method=_formulas.pr,
                positive=None,
                levels=None,
                output_data=None,
                method=VarHandler.cmorize,
            ),
            dict(
                name="pr",
                units="kg m-2 s-1",
                raw_variables=["PRECC", "PRECL"],
                table="CMIP6_Amon.json",
                unit_conversion=None,
                formula="(PRECC + PRECL) * 1000.0",
                formula_method=_formulas.pr,
                positive=None,
                levels=None,
                output_data=None,
                method=VarHandler.cmorize,
            ),
        ]

        # Update each handler objects' BoundMethod to the underlying function.
        for handler in result:
            handler["method"] = handler["method"].__func__

        assert result == expected

    def test_returns_handlers_based_on_var_list(self):
        result = load_all_handlers("lnd", cmip_vars=["orog", "sftlf"])
        expected = [
            {
                "name": "orog",
                "units": "m",
                "table": "CMIP6_fx.json",
                "method": orog.handle.__name__,
                "raw_variables": ["PHIS"],
                "positive": None,
                "levels": None,
            },
            {
                "name": "sftlf",
                "units": "%",
                "table": "CMIP6_fx.json",
                "method": sftlf.handle.__name__,
                "raw_variables": ["LANDFRAC"],
                "positive": None,
                "levels": None,
            },
        ]

        # Update "method" value to the name of the method because the memory
        # address changes with imports, so the handler dict won't align with the
        # expected output.
        for handler in result:
            handler["method"] = handler["method"].__name__

        assert result == expected

    def test_returns_mpas_var_handlers_based_on_var_list(self):
        result = load_all_handlers("Omon", cmip_vars=["so", "uo"])
        expected = [
            {
                "name": "so",
                "units": "0.001",
                "table": "CMIP6_Omon.json",
                "method": so.handle.__name__,
                "raw_variables": ["MPASO", "MPAS_mesh", "MPAS_map"],
                "positive": None,
                "levels": None,
            },
            {
                "name": "uo",
                "units": "m s-1",
                "table": "CMIP6_Omon.json",
                "method": uo.handle.__name__,
                "raw_variables": ["MPASO", "MPAS_mesh", "MPAS_map"],
                "positive": None,
                "levels": None,
            },
        ]

        # Update "method" value to the name of the method because the memory
        # address changes with imports, so the handler dict won't align with the
        # expected output.
        for handler in result:
            handler["method"] = handler["method"].__name__

        assert result == expected


class TestDeriveHandlers:
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
                    }
                },
                json_file,
            )

    def test_prints_logger_warning_if_handler_is_not_defined_for_a_variable(
        self, caplog
    ):
        derive_handlers(
            self.tables_path,
            cmip_vars=["undefined_var"],
            e3sm_vars=["incorrect_e3sm_var"],
            freq="mon",
            realm="atm",
        )

        for record in caplog.records:
            assert record.levelname == "WARNING"

    def test_prints_logger_warning_if_handler_cannot_be_derived_from_input_e3sm_vars(
        self, caplog
    ):
        derive_handlers(
            self.tables_path,
            cmip_vars=["pr"],
            e3sm_vars=["incorrect_e3sm_var"],
            freq="mon",
            realm="atm",
        )

        for record in caplog.records:
            assert record.levelname == "WARNING"

    def test_raises_error_if_CMIP6_table_entry_for_variable_and_freq_arg(self):
        with pytest.raises(KeyError):
            derive_handlers(
                self.tables_path,
                cmip_vars=["rlut"],
                e3sm_vars=["FSNTOA", "FSNT", "FLNT"],
                freq="3hr",
                realm="atm",
            )

    def test_raises_error_when_table_does_not_exist(self):
        with pytest.raises(
            ValueError, match="Table `CMIP6_6hrLev.json` does not exist"
        ):
            derive_handlers(
                self.tables_path,
                cmip_vars=["pr"],
                e3sm_vars=["PRECT"],
                freq="6hrLev",
                realm="atm",
            )

    def test_returns_handler_with_updated_referenced_CMIP6_table_based_on_freq_arg(
        self,
    ):
        result = derive_handlers(
            self.tables_path,
            cmip_vars=["pr"],
            e3sm_vars=["PRECL", "PRECC"],
            freq="3hr",
            realm="atm",
        )

        expected = [
            dict(
                name="pr",
                units="kg m-2 s-1",
                raw_variables=["PRECC", "PRECL"],
                table="CMIP6_3hr.json",
                unit_conversion=None,
                formula="(PRECC + PRECL) * 1000.0",
                formula_method=_formulas.pr,
                positive=None,
                levels=None,
                output_data=None,
                method=VarHandler.cmorize,
            )
        ]

        # Update each handler objects' BoundMethod to the underlying function.
        for handler in result:
            handler["method"] = handler["method"].__func__

        assert result == expected

    def test_returns_handler_objects_for_Amon_freq_based_on_existing_e3sm_vars(self):
        result = derive_handlers(
            self.tables_path,
            cmip_vars=["pr"],
            e3sm_vars=["PRECL", "PRECC"],
            freq="mon",
            realm="atm",
        )
        expected = [
            dict(
                name="pr",
                units="kg m-2 s-1",
                raw_variables=["PRECC", "PRECL"],
                table="CMIP6_Amon.json",
                unit_conversion=None,
                formula="(PRECC + PRECL) * 1000.0",
                formula_method=_formulas.pr,
                positive=None,
                levels=None,
                output_data=None,
                method=VarHandler.cmorize,
            )
        ]

        # Update each handler objects' BoundMethod to the underlying function.
        for handler in result:
            handler["method"] = handler["method"].__func__

        assert result == expected

    def test_returns_handler_objects_for_day_freq_based_on_existing_e3sm_vars(self):
        result = derive_handlers(
            self.tables_path,
            cmip_vars=["pr"],
            e3sm_vars=["PRECT"],
            freq="day",
            realm="atm",
        )
        expected = [
            dict(
                name="pr",
                units="kg m-2 s-1",
                raw_variables=["PRECT"],
                table="CMIP6_day.json",
                unit_conversion=None,
                formula="PRECT * 1000.0",
                formula_method=_formulas.pr,
                positive=None,
                levels=None,
                output_data=None,
                method=VarHandler.cmorize,
            )
        ]

        # Update each handler objects' BoundMethod to the underlying function.
        for handler in result:
            handler["method"] = handler["method"].__func__

        assert result == expected

    def test_loads_handler_from_module(self):
        result = derive_handlers(
            self.tables_path,
            cmip_vars=["orog", "sftlf"],
            e3sm_vars=["PHIS", "LANDFRAC"],
            freq="mon",
            realm="lnd",
        )
        expected = [
            {
                "name": "orog",
                "units": "m",
                "table": "CMIP6_fx.json",
                "method": orog.handle.__name__,
                "raw_variables": ["PHIS"],
                "positive": None,
                "levels": None,
            },
            {
                "name": "sftlf",
                "units": "%",
                "table": "CMIP6_fx.json",
                "method": sftlf.handle.__name__,
                "raw_variables": ["LANDFRAC"],
                "positive": None,
                "levels": None,
            },
        ]

        # Update "method" value to the name of the method because the memory
        # address changes with imports, so the handler dict won't align with the
        # expected output.
        for handler in result:
            handler["method"] = handler["method"].__name__

        assert result == expected

    def test_returns_empty_list_when_no_cmip_vars_given(self):
        result = derive_handlers(
            self.tables_path,
            cmip_vars=[],
            e3sm_vars=["PRECT"],
            freq="mon",
            realm="atm",
        )
        assert result == []

    def test_returns_empty_list_when_no_e3sm_vars_given(self):
        result = derive_handlers(
            self.tables_path,
            cmip_vars=["pr"],
            e3sm_vars=[],
            freq="mon",
            realm="atm",
        )
        assert result == []
