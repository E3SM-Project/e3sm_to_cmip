# flake8: noqa F401
import numpy as np
import pytest
import xarray as xr

from e3sm_to_cmip.cmor_handlers._formulas import (
    cl,
    cLitter,
    emibc,
    emiso2,
    emiso4,
    lai,
    mmrbc,
    mmrso4,
    mrfso,
    mrso,
    pr,
    prsn,
    rldscs,
    rlus,
    rlut,
    rsus,
    rsuscs,
    rtmt,
    tran,
)


def test_cLitter():
    data = {
        "TOTLITC": np.array([[0, 1, 2], [0, 1, 2], [0, 1, 2]], dtype="float64"),
        "CWDC": np.array([[0, 1, 2], [0, 1, 2], [0, 1, 2]], dtype="float64"),
    }

    result = cLitter(data, index=1)
    expected = np.array([0, 0.002, 0.004])
    np.testing.assert_allclose(result, expected)

    # Test when required variable keys are NOT in the data dictionary.
    with pytest.raises(KeyError):
        cLitter({}, index=1)


def test_cl():
    data = {
        "CLOUD": np.array([[0, 1, 2], [0, 1, 2], [0, 1, 2]], dtype="float64"),
    }

    result = cl(data, index=1)
    expected = np.array([0, 100, 200])
    np.testing.assert_allclose(result, expected)

    # Test when required variable keys are NOT in the data dictionary.
    with pytest.raises(KeyError):
        cl({}, index=1)


def test_emibc():
    data = {
        "SFbc_a4": np.array([[0, 1, 2], [0, 1, 2], [0, 1, 2]], dtype="float64"),
        "bc_a4_CLXF": np.array([[0, 1, 2], [0, 1, 2], [0, 1, 2]], dtype="float64"),
    }

    result = emibc(data, index=1)
    expected = np.array([0, 1, 2])
    np.testing.assert_allclose(result, expected)

    # Test when required variable keys are NOT in the data dictionary.
    with pytest.raises(KeyError):
        emibc({}, index=1)


def test_emiso2():
    data = {
        "SFSO2": np.array([[0, 1, 2], [0, 1, 2], [0, 1, 2]], dtype="float64"),
        "SO2_CLXF": np.array([[0, 1, 2], [0, 1, 2], [0, 1, 2]], dtype="float64"),
    }

    result = emiso2(data, index=1)
    expected = np.array([0, 1, 2])
    np.testing.assert_allclose(result, expected)

    # Test when required variable keys are NOT in the data dictionary.
    with pytest.raises(KeyError):
        emiso2({}, index=1)


def test_emiso4():
    data = {
        "SFso4_a1": np.array([[0, 1, 2], [0, 1, 2], [0, 1, 2]], dtype="float64"),
        "SFso4_a2": np.array([[0, 1, 2], [0, 1, 2], [0, 1, 2]], dtype="float64"),
        "so4_a1_CLXF": np.array([[0, 1, 2], [0, 1, 2], [0, 1, 2]], dtype="float64"),
        "so4_a2_CLXF": np.array([[0, 1, 2], [0, 1, 2], [0, 1, 2]], dtype="float64"),
    }

    result = emiso4(data, index=1)
    expected = np.array([0, 2, 4])
    np.testing.assert_allclose(result, expected)

    # Test when required variable keys are NOT in the data dictionary.
    with pytest.raises(KeyError):
        emiso4({}, index=1)


def test_lai():
    data = {
        "LAISHA": np.array([[0, 1, 2], [0, 1, 2], [0, 1, 2]], dtype="float64"),
        "LAISUN": np.array([[0, 1, 2], [0, 1, 2], [0, 1, 2]], dtype="float64"),
    }

    result = lai(data, index=1)
    expected = np.array([0, 2, 4])
    np.testing.assert_allclose(result, expected)

    # Test when required variable keys are NOT in the data dictionary.
    with pytest.raises(KeyError):
        lai({}, index=1)


def test_mmrbc():
    data = {
        "bc_a1": np.array([[0, 1, 2], [0, 1, 2], [0, 1, 2]], dtype="float64"),
        "bc_a4": np.array([[0, 1, 2], [0, 1, 2], [0, 1, 2]], dtype="float64"),
        "bc_c1": np.array([[0, 1, 2], [0, 1, 2], [0, 1, 2]], dtype="float64"),
        "bc_c4": np.array([[0, 1, 2], [0, 1, 2], [0, 1, 2]], dtype="float64"),
    }

    result = mmrbc(data, index=1)
    expected = np.array([0, 4, 8])
    np.testing.assert_allclose(result, expected)

    # Test when required variable keys are NOT in the data dictionary.
    with pytest.raises(KeyError):
        mmrbc({}, index=1)


def test_mmrso4():
    data = {
        "so4_a1": np.array([[0, 1, 2], [0, 1, 2], [0, 1, 2]], dtype="float64"),
        "so4_c1": np.array([[0, 1, 2], [0, 1, 2], [0, 1, 2]], dtype="float64"),
        "so4_a2": np.array([[0, 1, 2], [0, 1, 2], [0, 1, 2]], dtype="float64"),
        "so4_c2": np.array([[0, 1, 2], [0, 1, 2], [0, 1, 2]], dtype="float64"),
        "so4_a3": np.array([[0, 1, 2], [0, 1, 2], [0, 1, 2]], dtype="float64"),
        "so4_c3": np.array([[0, 1, 2], [0, 1, 2], [0, 1, 2]], dtype="float64"),
    }

    result = mmrso4(data, index=1)
    expected = np.array([0, 6, 12])
    np.testing.assert_allclose(result, expected)

    # Test when required variable keys are NOT in the data dictionary.
    with pytest.raises(KeyError):
        mmrso4({}, index=1)


def test_mrfso():
    data = {
        "SOILICE": np.array([[0, 1, 2], [0, 1, 2], [0, 1, 2]], dtype="float64"),
    }

    result = mrfso(data, index=1)
    expected = np.array([3, 3, 3])
    np.testing.assert_allclose(result, expected)

    # Test when required variable keys are NOT in the data dictionary.
    with pytest.raises(KeyError):
        mrfso({}, index=1)


def test_mrso():
    data = {
        "SOILICE": np.array([[0, 1, 2], [0, 1, 2], [0, 1, 2]], dtype="float64"),
        "SOILLIQ": np.array([[0, 1, 2], [0, 1, 2], [0, 1, 2]], dtype="float64"),
    }

    result = mrso(data, index=1)
    expected = np.array([6, 6, 6])
    np.testing.assert_allclose(result, expected)

    # Test when required variable keys are NOT in the data dictionary.
    with pytest.raises(KeyError):
        mrso({}, index=1)


def test_pr():
    data = {
        "PRECC": np.array([[0, 1, 2], [0, 1, 2], [0, 1, 2]], dtype="float64"),
        "PRECL": np.array([[0, 1, 2], [0, 1, 2], [0, 1, 2]], dtype="float64"),
    }

    result = pr(data, index=1)
    expected = np.array([0, 2000, 4000])

    np.testing.assert_array_equal(result, expected)

    data = {"PRECT": np.array([[0, 1, 2], [0, 1, 2], [0, 1, 2]], dtype="float64")}
    result = pr(data, index=1)
    expected = np.array([0, 1000, 2000])

    np.testing.assert_array_equal(result, expected)

    # Test when required variable keys are NOT in the data dictionary.
    with pytest.raises(KeyError):
        pr({}, index=1)


def test_prsn():
    data = {
        "PRECSC": np.array([[0, 1, 2], [0, 1, 2], [0, 1, 2]], dtype="float64"),
        "PRECSL": np.array([[0, 1, 2], [0, 1, 2], [0, 1, 2]], dtype="float64"),
    }

    result = prsn(data, index=1)
    expected = np.array([0, 2000, 4000])
    np.testing.assert_allclose(result, expected)

    # Test when required variable keys are NOT in the data dictionary.
    with pytest.raises(KeyError):
        prsn({}, index=1)


def test_rldscs():
    data = {
        "FLDS": np.array([[0, 1, 2], [0, 1, 2], [0, 1, 2]], dtype="float64"),
        "FLNS": np.array([[0, 1, 2], [0, 1, 2], [0, 1, 2]], dtype="float64"),
        "FLNSC": np.array([[0, 1, 2], [0, 1, 2], [0, 1, 2]], dtype="float64"),
    }

    result = rldscs(data, index=1)
    expected = np.array([0, 1, 2])
    np.testing.assert_allclose(result, expected)

    # Test when required variable keys are NOT in the data dictionary.
    with pytest.raises(KeyError):
        rldscs({}, index=1)


def test_rlut():
    data = {
        "FSNTOA": np.array([[0, 1, 2], [0, 1, 2], [0, 1, 2]], dtype="float64"),
        "FSNT": np.array([[0, 1, 2], [0, 1, 2], [0, 1, 2]], dtype="float64"),
        "FLNT": np.array([[0, 1, 2], [0, 1, 2], [0, 1, 2]], dtype="float64"),
    }

    result = rlut(data, index=1)
    expected = np.array([0, 1, 2])
    np.testing.assert_allclose(result, expected)

    # Test when required variable keys are NOT in the data dictionary.
    with pytest.raises(KeyError):
        rlut({}, index=1)


def test_rlus():
    data = {
        "FLDS": np.array([[0, 1, 2], [0, 1, 2], [0, 1, 2]], dtype="float64"),
        "FLNS": np.array([[0, 1, 2], [0, 1, 2], [0, 1, 2]], dtype="float64"),
    }

    result = rlus(data, index=1)
    expected = np.array([0, 2, 4])
    np.testing.assert_allclose(result, expected)

    # Test when required variable keys are NOT in the data dictionary.
    with pytest.raises(KeyError):
        rlus({}, index=1)


def test_rsus():
    data = {
        "FSDS": np.array([[0, 1, 2], [0, 1, 2], [0, 1, 2]], dtype="float64"),
        "FSNS": np.array([[0, 1, 2], [0, 1, 2], [0, 1, 2]], dtype="float64"),
    }

    result = rsus(data, index=1)
    expected = np.array([0, 0, 0])
    np.testing.assert_allclose(result, expected)

    # Test when required variable keys are NOT in the data dictionary.
    with pytest.raises(KeyError):
        rsus({}, index=1)


def test_rsuscs():
    data = {
        "FSDSC": np.array([[0, 1, 2], [0, 1, 2], [0, 1, 2]], dtype="float64"),
        "FSNSC": np.array([[0, 1, 2], [0, 1, 2], [0, 1, 2]], dtype="float64"),
    }

    result = rsuscs(data, index=1)
    expected = np.array([0, 0, 0])
    np.testing.assert_allclose(result, expected)

    # Test when required variable keys are NOT in the data dictionary.
    with pytest.raises(KeyError):
        rsuscs({}, index=1)


def test_rtmt():
    data = {
        "FSNT": np.array([[0, 1, 2], [0, 1, 2], [0, 1, 2]], dtype="float64"),
        "FLNT": np.array([[0, 1, 2], [0, 1, 2], [0, 1, 2]], dtype="float64"),
    }

    result = rtmt(data, index=1)
    expected = np.array([0, 0, 0])
    np.testing.assert_allclose(result, expected)

    # Test when required variable keys are NOT in the data dictionary.
    with pytest.raises(KeyError):
        rsuscs({}, index=1)


def test_tran():
    data = {
        "QSOIL": np.array([[0, 1, 2], [0, 1, 2], [0, 1, 2]], dtype="float64"),
        "QVEGT": np.array([[0, 1, 2], [0, 1, 2], [0, 1, 2]], dtype="float64"),
    }

    result = tran(data, index=1)
    expected = np.array([0, 2, 4])
    np.testing.assert_allclose(result, expected)

    # Test when required variable keys are NOT in the data dictionary.
    with pytest.raises(KeyError):
        rsuscs({}, index=1)
