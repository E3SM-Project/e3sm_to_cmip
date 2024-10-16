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


def _dummy_dataarray():
    return xr.DataArray(
        dims=["lat", "lon"],
        data=np.array([[0, 1, 2], [0, 1, 2], [0, 1, 2]], dtype="float64"),
    )


def test_cLitter():
    ds = xr.Dataset(
        data_vars={"TOTLITC": _dummy_dataarray(), "CWDC": _dummy_dataarray()}
    )

    result = cLitter(ds)
    expected = xr.DataArray(
        dims=["lat", "lon"],
        data=(
            np.array([[0.0, 0.002, 0.004], [0.0, 0.002, 0.004], [0.0, 0.002, 0.004]])
        ),
    )
    xr.testing.assert_allclose(result, expected)

    # Test when required variable keys are NOT in the data dictionary.
    with pytest.raises(KeyError):
        cLitter(xr.Dataset())


def test_cl():
    ds = xr.Dataset(data_vars={"CLOUD": _dummy_dataarray()})

    result = cl(ds)
    expected = xr.DataArray(
        dims=["lat", "lon"],
        data=np.array([[0, 100, 200], [0, 100, 200], [0, 100, 200]]),
    )
    xr.testing.assert_allclose(result, expected)

    # Test when required variable keys are NOT in the data dictionary.
    with pytest.raises(KeyError):
        cl(xr.Dataset())


def test_emibc():
    ds = xr.Dataset(
        data_vars={"SFbc_a4": _dummy_dataarray(), "bc_a4_CLXF": _dummy_dataarray()}
    )

    result = emibc(ds)
    expected = xr.DataArray(
        dims=["lat", "lon"], data=np.array([[0, 1, 2], [0, 1, 2], [0, 1, 2]])
    )
    xr.testing.assert_allclose(result, expected)

    # Test when required variable keys are NOT in the data dictionary.
    with pytest.raises(KeyError):
        emibc(xr.Dataset())


def test_emiso2():
    ds = xr.Dataset(
        data_vars={"SFSO2": _dummy_dataarray(), "SO2_CLXF": _dummy_dataarray()}
    )

    result = emiso2(ds)
    expected = xr.DataArray(
        dims=["lat", "lon"], data=np.array([[0, 1, 2], [0, 1, 2], [0, 1, 2]])
    )
    xr.testing.assert_allclose(result, expected)

    # Test when required variable keys are NOT in the data dictionary.
    with pytest.raises(KeyError):
        emiso2(xr.Dataset())


def test_emiso4():
    ds = xr.Dataset(
        data_vars={
            "SFso4_a1": _dummy_dataarray(),
            "SFso4_a2": _dummy_dataarray(),
            "so4_a1_CLXF": _dummy_dataarray(),
            "so4_a2_CLXF": _dummy_dataarray(),
        }
    )

    result = emiso4(ds)
    expected = xr.DataArray(
        dims=["lat", "lon"], data=np.array([[0, 2, 4], [0, 2, 4], [0, 2, 4]])
    )
    xr.testing.assert_allclose(result, expected)

    # Test when required variable keys are NOT in the data dictionary.
    with pytest.raises(KeyError):
        emiso4(xr.Dataset())


def test_lai():
    ds = xr.Dataset(
        data_vars={
            "LAISHA": _dummy_dataarray(),
            "LAISUN": _dummy_dataarray(),
        }
    )

    result = lai(ds)
    expected = xr.DataArray(
        dims=["lat", "lon"], data=np.array([[0, 2, 4], [0, 2, 4], [0, 2, 4]])
    )
    xr.testing.assert_allclose(result, expected)

    # Test when required variable keys are NOT in the data dictionary.
    with pytest.raises(KeyError):
        lai(xr.Dataset())


def test_mmrbc():
    ds = xr.Dataset(
        data_vars={
            "bc_a1": _dummy_dataarray(),
            "bc_a4": _dummy_dataarray(),
            "bc_c1": _dummy_dataarray(),
            "bc_c4": _dummy_dataarray(),
        }
    )

    result = mmrbc(ds)
    expected = xr.DataArray(
        dims=["lat", "lon"], data=np.array([[0, 4, 8], [0, 4, 8], [0, 4, 8]])
    )
    xr.testing.assert_allclose(result, expected)

    # Test when required variable keys are NOT in the data dictionary.
    with pytest.raises(KeyError):
        mmrbc(xr.Dataset())


def test_mmrso4():
    ds = xr.Dataset(
        data_vars={
            "so4_a1": _dummy_dataarray(),
            "so4_c1": _dummy_dataarray(),
            "so4_a2": _dummy_dataarray(),
            "so4_c2": _dummy_dataarray(),
            "so4_a3": _dummy_dataarray(),
            "so4_c3": _dummy_dataarray(),
        }
    )

    result = mmrso4(ds)
    expected = xr.DataArray(
        dims=["lat", "lon"], data=np.array([[0, 6, 12], [0, 6, 12], [0, 6, 12]])
    )
    xr.testing.assert_allclose(result, expected)

    # Test when required variable keys are NOT in the data dictionary.
    with pytest.raises(KeyError):
        mmrso4(xr.Dataset())


def test_mrfso():
    ds = xr.Dataset(
        data_vars={
            "SOILICE": xr.DataArray(
                dims=["lat", "levgrnd"],
                data=np.array(
                    [[0, 1, 5000], [0, 1, 6000], [0, 1, 4000]], dtype="float64"
                ),
            )
        }
    )

    result = mrfso(ds)
    expected = xr.DataArray(
        dims=["lat"], data=np.array([5000, 5000, 4001]), coords={"lat": [0, 1, 2]}
    )
    xr.testing.assert_allclose(result, expected)

    # Test when required variable keys are NOT in the data dictionary.
    with pytest.raises(KeyError):
        mrfso(xr.Dataset())


def test_mrso():
    ds = xr.Dataset(
        data_vars={
            "SOILICE": xr.DataArray(
                dims=["lat", "levgrnd"],
                data=np.array(
                    [[0, 1, 5000], [0, 1, 6000], [0, 1, 4000]], dtype="float64"
                ),
            ),
            "SOILLIQ": xr.DataArray(
                dims=["lat", "levgrnd"],
                data=np.array(
                    [[0, 1, 5000], [0, 1, 6000], [0, 1, 4000]], dtype="float64"
                ),
            ),
        }
    )

    result = mrso(ds)
    expected = xr.DataArray(dims=["lat"], data=np.array([5000, 5000, 5000]))
    xr.testing.assert_allclose(result, expected)

    # Test when required variable keys are NOT in the data dictionary.
    with pytest.raises(KeyError):
        mrso(xr.Dataset())


def test_pr():
    ds = xr.Dataset(
        data_vars={"PRECC": _dummy_dataarray(), "PRECL": _dummy_dataarray()}
    )

    result = pr(ds)
    expected = xr.DataArray(
        dims=["lat", "lon"],
        data=np.array([[0, 2000, 4000], [0, 2000, 4000], [0, 2000, 4000]]),
    )

    np.testing.assert_array_equal(result, expected)

    ds = xr.Dataset(data_vars={"PRECT": _dummy_dataarray()})
    result = pr(ds)
    expected = xr.DataArray(
        dims=["lat", "lon"],
        data=np.array([[0, 1000, 2000], [0, 1000, 2000], [0, 1000, 2000]]),
    )

    np.testing.assert_array_equal(result, expected)

    # Test when required variable keys are NOT in the data dictionary.
    with pytest.raises(KeyError):
        pr(xr.Dataset())


def test_prsn():
    ds = xr.Dataset(
        data_vars={"PRECSC": _dummy_dataarray(), "PRECSL": _dummy_dataarray()}
    )

    result = prsn(ds)
    expected = xr.DataArray(
        dims=["lat", "lon"],
        data=np.array([[0, 2000, 4000], [0, 2000, 4000], [0, 2000, 4000]]),
    )
    xr.testing.assert_allclose(result, expected)

    # Test when required variable keys are NOT in the data dictionary.
    with pytest.raises(KeyError):
        prsn(xr.Dataset())


def test_rldscs():
    ds = xr.Dataset(
        data_vars={
            "FLDS": _dummy_dataarray(),
            "FLNS": _dummy_dataarray(),
            "FLNSC": _dummy_dataarray(),
        }
    )

    result = rldscs(ds)
    expected = xr.DataArray(
        dims=["lat", "lon"],
        data=np.array([[0, 1, 2], [0, 1, 2], [0, 1, 2]]),
    )
    xr.testing.assert_allclose(result, expected)

    # Test when required variable keys are NOT in the data dictionary.
    with pytest.raises(KeyError):
        rldscs(xr.Dataset())


def test_rlut():
    ds = xr.Dataset(
        data_vars={
            "FSNTOA": _dummy_dataarray(),
            "FSNT": _dummy_dataarray(),
            "FLNT": _dummy_dataarray(),
        }
    )

    result = rlut(ds)
    expected = xr.DataArray(
        dims=["lat", "lon"],
        data=np.array([[0, 1, 2], [0, 1, 2], [0, 1, 2]]),
    )
    xr.testing.assert_allclose(result, expected)

    # Test when required variable keys are NOT in the data dictionary.
    with pytest.raises(KeyError):
        rlut(xr.Dataset())


def test_rlus():
    ds = xr.Dataset(data_vars={"FLDS": _dummy_dataarray(), "FLNS": _dummy_dataarray()})

    result = rlus(ds)
    expected = xr.DataArray(
        dims=["lat", "lon"],
        data=np.array([[0, 2, 4], [0, 2, 4], [0, 2, 4]]),
    )
    xr.testing.assert_allclose(result, expected)

    # Test when required variable keys are NOT in the data dictionary.
    with pytest.raises(KeyError):
        rlus(xr.Dataset())


def test_rsus():
    ds = xr.Dataset(data_vars={"FSNS": _dummy_dataarray(), "FSDS": _dummy_dataarray()})

    result = rsus(ds)
    expected = xr.DataArray(
        dims=["lat", "lon"],
        data=np.array([[0, 0, 0], [0, 0, 0], [0, 0, 0]]),
    )
    xr.testing.assert_allclose(result, expected)

    # Test when required variable keys are NOT in the data dictionary.
    with pytest.raises(KeyError):
        rsus(xr.Dataset())


def test_rsuscs():
    ds = xr.Dataset(
        data_vars={"FSDSC": _dummy_dataarray(), "FSNSC": _dummy_dataarray()}
    )

    result = rsuscs(ds)
    expected = xr.DataArray(
        dims=["lat", "lon"],
        data=np.array([[0, 0, 0], [0, 0, 0], [0, 0, 0]]),
    )
    xr.testing.assert_allclose(result, expected)

    # Test when required variable keys are NOT in the data dictionary.
    with pytest.raises(KeyError):
        rsuscs(xr.Dataset())


def test_rtmt():
    ds = xr.Dataset(data_vars={"FSNT": _dummy_dataarray(), "FLNT": _dummy_dataarray()})

    result = rtmt(ds)
    expected = xr.DataArray(
        dims=["lat", "lon"],
        data=np.array([[0, 0, 0], [0, 0, 0], [0, 0, 0]]),
    )
    xr.testing.assert_allclose(result, expected)

    # Test when required variable keys are NOT in the data dictionary.
    with pytest.raises(KeyError):
        rsuscs(xr.Dataset())


def test_tran():
    ds = xr.Dataset(
        data_vars={"QSOIL": _dummy_dataarray(), "QVEGT": _dummy_dataarray()}
    )

    result = tran(ds)
    expected = xr.DataArray(
        dims=["lat", "lon"],
        data=np.array([[0, 2, 4], [0, 2, 4], [0, 2, 4]]),
    )
    xr.testing.assert_allclose(result, expected)

    # Test when required variable keys are NOT in the data dictionary.
    with pytest.raises(KeyError):
        rsuscs(xr.Dataset())
