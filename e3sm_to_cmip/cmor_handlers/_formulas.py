"""This module defines formulas involving arithmetic for variable handlers.

If a variable handler is not listed here, then the formula involves no
arithmetic (e.g., "o3 = O3") and is generalized.

TODO: There is probably a more modular way of defining formulas that is
scalable.
"""
from typing import Dict

import numpy as np


def cLitter(data: Dict[str, np.ndarray], index: int) -> np.ndarray:
    """
    cLitter = (TOTLITC + CWDC)/1000.0
    """
    outdata = (data["TOTLITC"][index, :] + data["CWDC"][index, :]) / 1000.0

    return outdata


def cl(data: Dict[str, np.ndarray], index: int) -> np.ndarray:
    """
    cl = CLOUD * 100.0
    """
    outdata = data["CLOUD"][index, :] * 100.0

    return outdata


def emibc(data: Dict[str, np.ndarray], index: int) -> np.ndarray:
    """
    emibc = SFbc_a4 (surface emission kg/m2/s) + bc_a4_CLXF (vertically integrated molec/cm2/s) x 12.011 / 6.02214e+22
    """
    outdata = (
        data["SFbc_a4"][index, :] + data["bc_a4_CLXF"][index, :] * 12.011 / 6.02214e22
    )
    return outdata


def emiso2(data: Dict[str, np.ndarray], index: int) -> np.ndarray:
    """
    emiso2 = SFSO2 (surface emission kg/m2/s) + SO2_CLXF (vertically integrated molec/cm2/s) x 64.064800 / 6.02214e+22
    """
    outdata = (
        data["SFSO2"][index, :] + data["SO2_CLXF"][index, :] * 64.064800 / 6.02214e22
    )

    return outdata


def emiso4(data: Dict[str, np.ndarray], index: int) -> np.ndarray:
    """
    emiso4 = SFso4_a1 (kg/m2/s) + SFso4_a2 (kg/m2/s) + (so4_a1_CLXF (molec/cm2/s) + \
        so4_a2_CLXF(molec/cm2/s)) x 115.107340 (sulfate mw) / 6.02214e+22
    """
    outdata = (
        data["SFso4_a1"][index, :]
        + data["SFso4_a2"][index, :]
        + (data["so4_a1_CLXF"][index, :] + data["so4_a2_CLXF"][index, :])
        * 115.107340
        / 6.02214e22
    )

    return outdata


def lai(data: Dict[str, np.ndarray], index: int) -> np.ndarray:
    """
    lai = LAISHA + LAISUN
    """
    outdata = data["LAISHA"][index, :] + data["LAISUN"][index, :]

    return outdata


def mmrbc(data: Dict[str, np.ndarray], index: int) -> np.ndarray:
    """
    mmrbc = bc_a1+bc_a4+bc_c1+bc_c4
    """
    outdata = (
        data["bc_a1"][index, :]
        + data["bc_a4"][index, :]
        + data["bc_c1"][index, :]
        + data["bc_c4"][index, :]
    )

    return outdata


def mmrso4(data: Dict[str, np.ndarray], index: int) -> np.ndarray:
    """
    mmrso4 = so4_a1+so4_c1+so4_a2+so4_c2+so4_a3+so4_c3
    """
    outdata = (
        data["so4_a1"][index, :]
        + data["so4_c1"][index, :]
        + data["so4_a2"][index, :]
        + data["so4_c2"][index, :]
        + data["so4_a3"][index, :]
        + data["so4_c3"][index, :]
    )

    return outdata


def mrfso(data: Dict[str, np.ndarray], index: int) -> np.ndarray:
    """
    mrfso = verticalSum(SOILICE, capped_at=5000)
    """
    soil_ice = data["SOILICE"][index, :]

    # we only care about data with a value greater then 0
    mask = np.greater(soil_ice, 0.0)

    # sum the data over the levgrnd axis
    outdata = np.sum(soil_ice, axis=0)

    # replace all values greater then 5k with 5k
    capped = np.where(np.greater(outdata, 5000.0), 5000.0, outdata)
    outdata = np.where(mask, capped, outdata)

    return outdata


def mrso(data: Dict[str, np.ndarray], index: int) -> np.ndarray:
    """
    mrso = verticalSum(SOILICE + SOILLIQ, capped_at=5000)
    """
    soil_ice = data["SOILICE"][index, :]
    soil_liq = data["SOILLIQ"][index, :]

    icemask = np.greater(soil_ice, 0.0)
    liqmask = np.greater(soil_liq, 0.0)
    total_mask = np.logical_or(icemask, liqmask)

    outdata = np.sum(soil_ice + soil_liq, axis=0)
    capped = np.where(np.greater(outdata, 5000.0), 5000.0, outdata)
    outdata = np.where(total_mask, capped, outdata)

    return outdata


def pr(data: Dict[str, np.ndarray], index: int) -> np.ndarray:
    """
    pr = (PRECC  + PRECL) * 1000.0

    High frequency version:
    pr = PRECT * 1000.0
    """
    if all(key in data for key in ["PRECC", "PRECL"]):
        outdata = (data["PRECC"][index, :] + data["PRECL"][index, :]) * 1000.0
    elif "PRECT" in data:
        outdata = data["PRECT"][index, :] * 1000.0
    else:
        raise KeyError(
            "No formula could be applied for 'pr'. Check the handler entry for 'pr' "
            "and input file(s) contain either 'PRECC' and 'PRECL', or 'PRECT."
        )

    return outdata


def prsn(data: Dict[str, np.ndarray], index: int) -> np.ndarray:
    """
    prsn = (PRECSC  + PRECSL) * 1000.0
    """
    outdata = (data["PRECSC"][index, :] + data["PRECSL"][index, :]) * 1000.0

    return outdata


def rldscs(data: Dict[str, np.ndarray], index: int) -> np.ndarray:
    """
    rldscs = FLDS + FLNS - FLNSC
    """
    outdata = data["FLDS"][index, :] + data["FLNS"][index, :] - data["FLNSC"][index, :]
    return outdata


def rlut(data: Dict[str, np.ndarray], index: int) -> np.ndarray:
    """
    rlut = FSNTOA - FSNT + FLNT

    High frequency version:
    rlut = FLUT
    """
    try:
        outdata = (
            data["FSNTOA"][index, :] - data["FSNT"][index, :] + data["FLNT"][index, :]
        )
    except KeyError:
        outdata = data["FLUT"][index, :]

    return outdata


def rlus(data: Dict[str, np.ndarray], index: int) -> np.ndarray:
    """
    rlus = FLDS + FLNS
    """
    outdata = data["FLDS"][index, :] + data["FLNS"][index, :]

    return outdata


def rsus(data: Dict[str, np.ndarray], index: int) -> np.ndarray:
    """
    rsus = FSDS - FSNS
    """
    outdata = data["FSDS"][index, :] - data["FSNS"][index, :]

    return outdata


def rsuscs(data: Dict[str, np.ndarray], index: int) -> np.ndarray:
    """
    rsuscs = FSDSC - FSNSC
    """
    outdata = data["FSDSC"][index, :] - data["FSNSC"][index, :]

    return outdata


def rsut(data: Dict[str, np.ndarray], index: int) -> np.ndarray:
    """
    rsut = SOLIN - FSNTOA

    pre v3 version:
    rsut = FSUTOA
    """
    try:
        outdata = (
            data["SOLIN"][index, :] - data["FSNTOA"][index, :]
        )
    except KeyError:
        outdata = data["FSUTOA"][index, :]

    return outdata


def rsutcs(data: Dict[str, np.ndarray], index: int) -> np.ndarray:
    """
    rsutcs = SOLIN - FSNTOAC

    pre v3 version:
    rsutcs = FSUTOAC
    """
    try:
        outdata = (
            data["SOLIN"][index, :] - data["FSNTOAC"][index, :]
        )
    except KeyError:
        outdata = data["FSUTOAC"][index, :]

    return outdata


def rtmt(data: Dict[str, np.ndarray], index: int) -> np.ndarray:
    """
    rtmt = FSNT - FLNT
    """
    outdata = data["FSNT"][index, :] - data["FLNT"][index, :]

    return outdata


def tran(data: Dict[str, np.ndarray], index: int) -> np.ndarray:
    """
    tran = QSOIL + QVEGT
    """
    outdata = data["QSOIL"][index, :] + data["QVEGT"][index, :]

    return outdata
