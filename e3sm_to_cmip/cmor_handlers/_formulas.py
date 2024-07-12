"""This module defines formulas involving arithmetic for variable handlers.

If a variable handler is not listed here, then the formula involves no
arithmetic (e.g., "o3 = O3") or uses the `convert_units` function
if `unit_conversion` is defined instead.
"""
import numpy as np
import xarray as xr

# Used by areacella.py
RADIUS = 6.37122e6

# P0 defined in Pa units (Pascal), used for reconstructing pressure to hybrid.
P0_VALUE = 100000

LEVGRND_BNDS = [
    0,
    0.01751106046140194,
    0.045087261125445366,
    0.09055273048579693,
    0.16551261954009533,
    0.28910057805478573,
    0.4928626772016287,
    0.8288095649331808,
    1.3826923426240683,
    2.2958906944841146,
    3.801500206813216,
    6.28383076749742,
    10.376501685008407,
    17.124175196513534,
    28.249208575114608,
    42.098968505859375,
]


def convert_units(var: xr.DataArray, unit_conversion: str) -> xr.DataArray:
    """Convert the variable's units using a unit conversion formula.

    Parameters
    ----------
    var : xr.DataArray
        The variable dataarray.
    unit_conversion : str
        The unit conversion formula defined in in the variable's handler.

    Returns
    -------
    xr.DataArray
        The variable dataarray with units converted.

    Raises
    ------
    ValueError
        If the unit conversion is not supported in this function.
    """
    if unit_conversion == "g-to-kg":
        result = var / 1000.0
    elif unit_conversion == "1-to-%":
        result = var * 100.0
    elif unit_conversion == "m/s-to-kg/ms":
        result = var * 1000.0
    elif unit_conversion == "-1":
        result = var * -1.0
    else:
        raise ValueError(
            f"'{unit_conversion}' isn't a supported unit conversion formula."
        )

    return result


def cLitter(ds: xr.Dataset) -> xr.DataArray:
    """
    cLitter = (TOTLITC + CWDC)/1000.0
    """
    result = (ds["TOTLITC"] + ds["CWDC"]) / 1000.0

    return result


def cl(ds: xr.Dataset) -> xr.DataArray:
    """
    cl = CLOUD * 100.0
    """
    result = ds["CLOUD"] * 100.0

    return result


def emibc(ds: xr.Dataset) -> xr.DataArray:
    """
    emibc = SFbc_a4 (surface emission kg/m2/s) + bc_a4_CLXF (vertically integrated molec/cm2/s) x 12.011 / 6.02214e+22
    """
    result = ds["SFbc_a4"] + ds["bc_a4_CLXF"] * 12.011 / 6.02214e22

    return result


def emiso2(ds: xr.Dataset) -> xr.DataArray:
    """
    emiso2 = SFSO2 (surface emission kg/m2/s) + SO2_CLXF (vertically integrated molec/cm2/s) x 64.064800 / 6.02214e+22
    """
    result = ds["SFSO2"] + ds["SO2_CLXF"] * 64.064800 / 6.02214e22

    return result


def emiso4(ds: xr.Dataset) -> xr.DataArray:
    """
    emiso4 = SFso4_a1 (kg/m2/s) + SFso4_a2 (kg/m2/s) + (so4_a1_CLXF (molec/cm2/s) + \
        so4_a2_CLXF(molec/cm2/s)) x 115.107340 (sulfate mw) / 6.02214e+22
    """
    result = (
        ds["SFso4_a1"]
        + ds["SFso4_a2"]
        + (ds["so4_a1_CLXF"] + ds["so4_a2_CLXF"]) * 115.107340 / 6.02214e22
    )

    return result


def lai(ds: xr.Dataset) -> xr.DataArray:
    """
    lai = LAISHA + LAISUN
    """
    result = ds["LAISHA"] + ds["LAISUN"]

    return result

def co2(ds: xr.Dataset) -> xr.DataArray:
    """
    co2 = CO2*29./44.
    """
    result = ds["CO2"]*29./44.

    return result


def mmrbc(ds: xr.Dataset) -> xr.DataArray:
    """
    mmrbc = bc_a1+bc_a4+bc_c1+bc_c4
    """
    result = ds["bc_a1"] + ds["bc_a4"] + ds["bc_c1"] + ds["bc_c4"]

    return result


def mmrso4(ds: xr.Dataset) -> xr.DataArray:
    """
    mmrso4 = so4_a1+so4_c1+so4_a2+so4_c2+so4_a3+so4_c3
    """
    result = (
        ds["so4_a1"]
        + ds["so4_c1"]
        + ds["so4_a2"]
        + ds["so4_c2"]
        + ds["so4_a3"]
        + ds["so4_c3"]
    )

    return result


def mrfso(ds: xr.Dataset) -> xr.DataArray:
    """
    mrfso = verticalSum(SOILICE, capped_at=5000)
    """
    var = ds["SOILICE"]
    axis = var.dims.index("levgrnd")

    # NOTE: `np.sum` is used instead of `xarray.DataArray.sum()` because
    # it maintains `np.nan` values, while xarray replaces `np.nan` with 0's.
    result: np.ndarray = np.sum(var.values, axis=axis)
    result = np.where(result > 5000.0, 5000.0, result)

    # Reconstruct the xarray.DataArray. Make sure not include "levgrnd" since
    # it has been summed over.
    dims = [dim for dim in var.dims if dim != "levgrnd"]
    coords = {dim: var[dim] for dim in dims}
    da = xr.DataArray(dims=dims, coords=coords, data=result, attrs=var.attrs)

    return da


def mrso(ds: xr.Dataset) -> xr.DataArray:
    """
    mrso = verticalSum(SOILICE + SOILLIQ, capped_at=5000)
    """
    soil_ice = ds["SOILICE"]
    soil_liq = ds["SOILLIQ"]

    # 1. Get the total summed over "levgrnd" dimension.
    sum_soil_ice = soil_ice.sum(dim="levgrnd")
    sum_soil_liq = soil_liq.sum(dim="levgrnd")
    result = sum_soil_ice + sum_soil_liq

    # 2. Replace all 0 values with nan
    result = xr.where(result <= 0, np.nan, result)

    # 3. Replace all values greater than 5K with 5K
    result_capped = xr.where(result > 5000, 5000.0, result)

    return result_capped


def pfull(ds: xr.Dataset):
    """Regrid hybrid-sigma levels to pressure coordinates (Pa units).

    Formula: pfull = hyam * p0 + hybm * ps

      * hyam: 1-D array equal to hybrid A coefficients
      * p0: Scalar numeric value equal to surface reference pressure with
            the same units as "ps" (Pa).
      * hybm: 1-D array equal to hybrid B coefficients
      * ps: 2-D array equal to surface pressure data (Pa).

    Parameters
    ----------
    ds : xr.Dataset
        The dataset containing the hybrid-sigma level variables to convert
        to pressure.

    Returns
    -------
    xr.DataArray
        The pfull pressure coordinates variable

    Notes
    -----
    This function is equivalent to `geocat.comp.interp_hybrid_to_pressure()`
    and `cdutil.vertical.reconstructPressureFromHybrid()`.
    """
    result = ds["hyam"] * P0_VALUE + ds["hybm"] * ds["PS"]

    # After Xarray broadcasting, the dimensions need to be reordered from
    # ["lev", "time2", "lat", "lon"] to ["time2", "lev", "lat", "lon"].
    result = result.transpose("time2", "lev", "lat", "lon")

    return result


def phalf(ds: xr.Dataset):
    """Regrid hybrid-sigma levels to pressure coordinates (Pa units).

    Formula: phalf = hyai * p0 + hybi * ps

      * hyam: 1-D array equal to hybrid A coefficients
      * p0: Scalar numeric value equal to surface reference pressure with
            the same units as "ps" (Pa).
      * hybm: 1-D array equal to hybrid B coefficients
      * ps: 2-D array equal to surface pressure data (Pa).

    Parameters
    ----------
    ds : xr.Dataset
        The dataset containing the hybrid-sigma level variables to convert
        to pressure.

    Returns
    -------
    xr.DataArray
        The half pressure coordinates variable

    Notes
    -----
    This function is equivalent to `geocat.comp.interp_hybrid_to_pressure()`
    and `cdutil.vertical.reconstructPressureFromHybrid()`.
    """
    result = ds["hyai"] * P0_VALUE + ds["hybi"] * ds["PS"]

    # After Xarray broadcasting, the dimensions need to be reordered from
    # ["ilev", "time2", "lat", "lon"] to ["time2", "ilev", "lat", "lon"].
    result = result.transpose("time2", "ilev", "lat", "lon")

    return result


def pr(ds: xr.Dataset) -> xr.DataArray:
    """
    pr = (PRECC  + PRECL) * 1000.0

    High frequency version:
    pr = PRECT * 1000.0
    """
    if all(key in ds.data_vars for key in ["PRECC", "PRECL"]):
        result = (ds["PRECC"] + ds["PRECL"]) * 1000.0
    elif "PRECT" in ds:
        result = ds["PRECT"] * 1000.0
    else:
        raise KeyError(
            "No formula could be applied for 'pr'. Check the handler entry for 'pr' "
            "and input file(s) contain either 'PRECC' and 'PRECL', or 'PRECT."
        )

    return result


def prsn(ds: xr.Dataset) -> xr.DataArray:
    """
    prsn = (PRECSC  + PRECSL) * 1000.0
    """
    result = (ds["PRECSC"] + ds["PRECSL"]) * 1000.0

    return result


def rldscs(ds: xr.Dataset) -> xr.DataArray:
    """
    rldscs = FLDS + FLNS - FLNSC
    """
    result = ds["FLDS"] + ds["FLNS"] - ds["FLNSC"]
    return result


def rlut(ds: xr.Dataset) -> xr.DataArray:
    """
    rlut = FSNTOA - FSNT + FLNT

    High frequency version:
    rlut = FLUT
    """
    try:
        result = ds["FSNTOA"] - ds["FSNT"] + ds["FLNT"]
    except KeyError:
        result = ds["FLUT"]

    return result


def rlus(ds: xr.Dataset) -> xr.DataArray:
    """
    rlus = FLDS + FLNS
    """
    result = ds["FLDS"] + ds["FLNS"]

    return result


def rsus(ds: xr.Dataset) -> xr.DataArray:
    """
    rsus = FSDS - FSNS
    """
    result = ds["FSDS"] - ds["FSNS"]

    return result


def rsuscs(ds: xr.Dataset) -> xr.DataArray:
    """
    rsuscs = FSDSC - FSNSC
    """
    result = ds["FSDSC"] - ds["FSNSC"]

    return result


def rsut(ds: xr.Dataset) -> xr.DataArray:
    """
    rsut = SOLIN - FSNTOA

    pre v3 version:
    rsut = FSUTOA
    """
    try:
        result = ds["SOLIN"] - ds["FSNTOA"]
    except KeyError:
        result = ds["FSUTOA"]

    return result


def rsutcs(ds: xr.Dataset) -> xr.DataArray:
    """
    rsutcs = SOLIN - FSNTOAC

    pre v3 version:
    rsutcs = FSUTOAC
    """
    try:
        result = ds["SOLIN"] - ds["FSNTOAC"]
    except KeyError:
        result = ds["FSUTOAC"]

    return result


def rtmt(ds: xr.Dataset) -> xr.DataArray:
    """
    rtmt = FSNT - FLNT
    """
    result = ds["FSNT"] - ds["FLNT"]

    return result


def tran(ds: xr.Dataset) -> xr.DataArray:
    """
    tran = QSOIL + QVEGT
    """
    result = ds["QSOIL"] + ds["QVEGT"]

    return result
