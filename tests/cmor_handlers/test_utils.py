import json

import pytest

from e3sm_to_cmip.cmor_handlers import _formulas
from e3sm_to_cmip.cmor_handlers.handler import VarHandler
from e3sm_to_cmip.cmor_handlers.utils import derive_handlers, load_all_handlers
from e3sm_to_cmip.cmor_handlers.vars import pfull, phalf


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

    def test_raises_error_if_handler_is_not_defined_for_a_cmip_var_id(self):
        with pytest.raises(KeyError):
            load_all_handlers(["undefined_handler"])

    def test_updates_CMIP_table_for_variable_based_on_freq_param(self):
        result = load_all_handlers(cmip_vars=["pr"])
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

    def test_loads_handler_from_module(self):
        result = load_all_handlers(cmip_vars=["pfull", "phalf"])
        expected = [
            {
                "name": "pfull",
                "units": "Pa",
                "table": "CMIP6_Amon.json",
                "method": pfull.handle.__name__,
                "raw_variables": ["hybi", "hyai", "hyam", "hybm", "PS"],
                "positive": None,
                "levels": {
                    "name": "standard_hybrid_sigma",
                    "units": "1",
                    "e3sm_axis_name": "lev",
                    "e3sm_axis_bnds": "ilev",
                    "time_name": "time2",
                },
            },
            {
                "name": "phalf",
                "units": "Pa",
                "table": "CMIP6_Amon.json",
                "method": phalf.handle.__name__,
                "raw_variables": ["hybi", "hyai", "hyam", "hybm", "PS"],
                "positive": None,
                "levels": {
                    "name": "atmosphere_sigma_coordinate",
                    "units": "1",
                    "e3sm_axis_name": "lev",
                    "e3sm_axis_bnds": "ilev",
                    "time_name": "time2",
                },
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

    def test_raises_error_if_handler_is_not_defined_for_a_variable(self):
        with pytest.raises(KeyError):
            derive_handlers(
                self.tables_path,
                cmip_vars=["undefined_var"],
                e3sm_vars=["incorrect_e3sm_var"],
            )

    def test_raises_error_if_handler_cannot_be_derived_from_input_e3sm_vars(self):
        with pytest.raises(KeyError):
            derive_handlers(
                self.tables_path, cmip_vars=["pr"], e3sm_vars=["incorrect_e3sm_var"]
            )

    def test_raises_error_if_CMIP6_table_entry_for_variable_and_freq_arg(self):
        with pytest.raises(KeyError):
            derive_handlers(
                self.tables_path,
                cmip_vars=["rlut"],
                e3sm_vars=["FSNTOA", "FSNT", "FLNT"],
                freq="3hr",
            )

    def test_updates_referenced_CMIP6_table_based_on_freq_arg(self):
        result = derive_handlers(
            self.tables_path, cmip_vars=["pr"], e3sm_vars=["PRECL", "PRECC"], freq="3hr"
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

    def test_returns_handler_objects_based_on_existing_e3sm_vars(self):
        result = derive_handlers(
            self.tables_path,
            cmip_vars=["pr"],
            e3sm_vars=["PRECL", "PRECC"],
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

        result = derive_handlers(
            self.tables_path,
            cmip_vars=["pr"],
            e3sm_vars=["PRECT"],
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
            cmip_vars=["pfull", "phalf"],
            e3sm_vars=["hybi", "hyai", "hyam", "hybm", "PS"],
        )
        expected = [
            {
                "name": "pfull",
                "units": "Pa",
                "table": "CMIP6_Amon.json",
                "method": pfull.handle.__name__,
                "raw_variables": ["hybi", "hyai", "hyam", "hybm", "PS"],
                "positive": None,
                "levels": {
                    "name": "standard_hybrid_sigma",
                    "units": "1",
                    "e3sm_axis_name": "lev",
                    "e3sm_axis_bnds": "ilev",
                    "time_name": "time2",
                },
            },
            {
                "name": "phalf",
                "units": "Pa",
                "table": "CMIP6_Amon.json",
                "method": phalf.handle.__name__,
                "raw_variables": ["hybi", "hyai", "hyam", "hybm", "PS"],
                "positive": None,
                "levels": {
                    "name": "atmosphere_sigma_coordinate",
                    "units": "1",
                    "e3sm_axis_name": "lev",
                    "e3sm_axis_bnds": "ilev",
                    "time_name": "time2",
                },
            },
        ]

        # Update "method" value to the name of the method because the memory
        # address changes with imports, so the handler dict won't align with the
        # expected output.
        for handler in result:
            handler["method"] = handler["method"].__name__

        assert result == expected
