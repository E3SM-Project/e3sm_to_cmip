import numpy as np

from e3sm_to_cmip.cmor_handlers import FILL_VALUE


def fill_nan(arr: np.ndarray, fill_value: float = FILL_VALUE) -> np.ndarray:
    """Replace NaNs in a numpy array with a specified fill value.

    NOTE: This function is intended for use in CMOR handlers that are still
    defined as Python modules. Once all CMOR handlers are converted to classes,
    this function should be deprecated.

    Parameters
    ----------
    arr : np.ndarray
        Input array that may contain NaNs.
    fill_value : float
        The value to replace NaNs with.

    Returns
    -------
    np.ndarray
        The modified array with NaNs replaced by the fill value.
    """
    return np.where(np.isnan(arr), fill_value, arr)
