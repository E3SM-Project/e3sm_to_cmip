from unittest.mock import Mock

import pytest

import e3sm_to_cmip.runner as runner_module
from e3sm_to_cmip.cmor_handlers.handler import VarHandler
from e3sm_to_cmip.cmor_handlers.vars import (
    areacella,
    clisccp,
    clmodis,
    orog,
    sftlf,
)
from e3sm_to_cmip.runner import E3SMtoCMIP
from e3sm_to_cmip.util import CMIPTableNotFoundError

LEGACY_HANDLERS = [areacella, clisccp, clmodis, orog, sftlf]


def _get_runner(on_var_failure="ignore"):
    runner = object.__new__(E3SMtoCMIP)
    runner.simple_mode = True
    runner.output_path = "/output"
    runner.realm = "atm"
    runner.tables_path = "/tables"
    runner.new_metadata_path = "/metadata.json"
    runner.cmor_log_dir = "/logs"
    runner.on_var_failure = on_var_failure
    runner.var_list = []
    runner.missing_handlers = []
    runner.non_derivable_handlers = []
    runner.num_proc = 1
    return runner


def _get_handler(module):
    return {
        "name": module.VAR_NAME,
        "method": module.handle,
        "raw_variables": module.RAW_VARIABLES,
        "table": module.TABLE,
    }


def test_simple_support_requires_explicit_simple_and_output_path():
    runner = _get_runner()
    supported = VarHandler(
        name="tas",
        units="K",
        raw_variables=["TREFHT"],
        table="CMIP6_Amon.json",
    ).cmorize

    assert runner._handler_supports_simple(supported)
    assert runner._get_simple_handler_kwargs(supported) == {
        "simple": True,
        "output_path": "/output",
    }
    for module in LEGACY_HANDLERS:
        assert not runner._handler_supports_simple(module.handle)


@pytest.mark.parametrize("module", LEGACY_HANDLERS)
def test_serial_simple_mode_never_invokes_legacy_handler(monkeypatch, module):
    runner = _get_runner()
    runner.handlers = [_get_handler(module)]
    monkeypatch.setattr(
        runner,
        "_get_handler_input_files",
        Mock(side_effect=AssertionError("input lookup should not run")),
    )
    final_result = Mock()
    monkeypatch.setattr(runner, "_log_final_result", final_result)
    monkeypatch.setattr(runner, "_finalize_on_failure", Mock())

    assert runner._run_serial() is True

    final_result.assert_called_once_with(1, 0, [module.VAR_NAME])


@pytest.mark.parametrize("mode", ["fail", "stop"])
def test_serial_unsupported_handler_honors_failure_mode(monkeypatch, mode):
    runner = _get_runner(on_var_failure=mode)
    runner.handlers = [_get_handler(sftlf)]
    monkeypatch.setattr(runner, "_get_handler_input_files", Mock())

    with pytest.raises(SystemExit):
        runner._run_serial()


def test_serial_simple_mode_continues_with_supported_handler(monkeypatch):
    calls = []

    def supported_handler(
        vars_to_filepaths,
        tables_path,
        metadata_path,
        cmor_log_dir,
        table,
        simple=False,
        output_path=None,
    ):
        calls.append((simple, output_path))
        return True

    runner = _get_runner()
    runner.handlers = [
        _get_handler(sftlf),
        {
            "name": "tas",
            "method": supported_handler,
            "raw_variables": ["TREFHT"],
            "table": "CMIP6_Amon.json",
        },
    ]
    monkeypatch.setattr(
        runner,
        "_get_handler_input_files",
        Mock(return_value={"TREFHT": ["TREFHT_185001_185012.nc"]}),
    )
    final_result = Mock()
    monkeypatch.setattr(runner, "_log_final_result", final_result)
    monkeypatch.setattr(runner, "_finalize_on_failure", Mock())

    assert runner._run_serial() is True

    assert calls == [(True, "/output")]
    final_result.assert_called_once_with(2, 1, ["sftlf"])


def test_parallel_simple_mode_never_submits_legacy_handlers(monkeypatch):
    runner = _get_runner()
    runner.handlers = [_get_handler(module) for module in LEGACY_HANDLERS]
    fake_pool = Mock()
    monkeypatch.setattr(runner_module, "Pool", Mock(return_value=fake_pool))
    monkeypatch.setattr(
        runner,
        "_get_handler_input_files",
        Mock(side_effect=AssertionError("input lookup should not run")),
    )
    final_result = Mock()
    monkeypatch.setattr(runner, "_log_final_result", final_result)
    monkeypatch.setattr(runner, "_finalize_on_failure", Mock())

    assert runner._run_parallel() is True

    fake_pool.submit.assert_not_called()
    final_result.assert_called_once_with(
        len(LEGACY_HANDLERS),
        0,
        [module.VAR_NAME for module in LEGACY_HANDLERS],
    )


@pytest.mark.parametrize("mode", ["fail", "stop"])
def test_parallel_unsupported_handler_honors_failure_mode(monkeypatch, mode):
    runner = _get_runner(on_var_failure=mode)
    runner.handlers = [_get_handler(sftlf)]
    fake_pool = Mock()
    monkeypatch.setattr(runner_module, "Pool", Mock(return_value=fake_pool))
    monkeypatch.setattr(runner, "_get_handler_input_files", Mock())

    with pytest.raises(SystemExit):
        runner._run_parallel()

    fake_pool.submit.assert_not_called()


def test_parallel_simple_mode_submits_supported_handler(monkeypatch):
    def supported_handler(
        vars_to_filepaths,
        tables_path,
        metadata_path,
        cmor_log_dir,
        table,
        simple=False,
        output_path=None,
    ):
        return True

    runner = _get_runner()
    runner.handlers = [
        _get_handler(sftlf),
        {
            "name": "tas",
            "method": supported_handler,
            "raw_variables": ["TREFHT"],
            "table": "CMIP6_Amon.json",
        },
    ]
    future = Mock()
    future.result.return_value = True
    fake_pool = Mock()
    fake_pool.submit.return_value = future
    monkeypatch.setattr(runner_module, "Pool", Mock(return_value=fake_pool))
    monkeypatch.setattr(runner_module, "as_completed", lambda futures: futures)
    monkeypatch.setattr(
        runner,
        "_get_handler_input_files",
        Mock(return_value={"TREFHT": ["TREFHT_185001_185012.nc"]}),
    )
    final_result = Mock()
    monkeypatch.setattr(runner, "_log_final_result", final_result)
    monkeypatch.setattr(runner, "_finalize_on_failure", Mock())

    assert runner._run_parallel() is True

    fake_pool.submit.assert_called_once()
    assert fake_pool.submit.call_args.kwargs == {
        "simple": True,
        "output_path": "/output",
    }
    final_result.assert_called_once_with(2, 1, ["sftlf"])


def test_simple_mode_missing_table_error_requests_tables_path(monkeypatch, tmp_path):
    runner = _get_runner()
    runner.info_mode = False
    runner.input_path = str(tmp_path)
    runner.tables_path = str(tmp_path)
    runner.var_list = ["tas"]
    runner.freq = "mon"
    monkeypatch.setattr(runner, "_get_e3sm_vars", Mock(return_value=["TREFHT"]))
    monkeypatch.setattr(
        runner_module,
        "derive_handlers",
        Mock(
            return_value=(
                [
                    {
                        "name": "tas",
                        "method": Mock(),
                        "raw_variables": ["TREFHT"],
                        "table": "CMIP6_Amon.json",
                    }
                ],
                [],
                [],
            )
        ),
    )

    with pytest.raises(ValueError, match="Pass --tables-path"):
        runner._get_handlers()


def test_simple_mode_derivation_missing_table_error_requests_tables_path(
    monkeypatch, tmp_path
):
    runner = _get_runner()
    runner.info_mode = False
    runner.input_path = str(tmp_path)
    runner.var_list = ["tas"]
    runner.freq = "day"
    monkeypatch.setattr(runner, "_get_e3sm_vars", Mock(return_value=["TREFHT"]))
    monkeypatch.setattr(
        runner_module,
        "derive_handlers",
        Mock(side_effect=CMIPTableNotFoundError("CMIP6_day.json is missing")),
    )

    with pytest.raises(ValueError, match="Pass --tables-path"):
        runner._get_handlers()


def test_simple_mode_preserves_unrelated_derivation_value_error(
    monkeypatch, tmp_path
):
    runner = _get_runner()
    runner.info_mode = False
    runner.input_path = str(tmp_path)
    runner.var_list = ["tas"]
    runner.freq = "day"
    monkeypatch.setattr(runner, "_get_e3sm_vars", Mock(return_value=["TREFHT"]))
    monkeypatch.setattr(
        runner_module,
        "derive_handlers",
        Mock(side_effect=ValueError("Table is not supported by realm lnd.")),
    )

    with pytest.raises(ValueError) as exc_info:
        runner._get_handlers()

    assert str(exc_info.value) == "Table is not supported by realm lnd."
