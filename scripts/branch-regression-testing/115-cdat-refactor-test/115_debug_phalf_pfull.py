# %%
"""
1. Write the output datasets out to .netcdf on master and dev
2. Compare closeness
3. Run functions for calculating phalf/pfull
4. Compare outputs
"""
# %%
import numpy as np
import xarray as xr
from xcdat import compare_datasets

# %%
# Compare outputs
path_d = "/home/vo13/E3SM-Project/e3sm_to_cmip/qa/115/CMIP6/CMIP/E3SM-Project/E3SM-1-0/piControl/r1i1p1f1/Amon/pfull/gr/tests/pfull_Amon_E3SM-1-0_piControl_r1i1p1f1_gr_185001-185012-clim.nc"
path_m = "/home/vo13/E3SM-Project/e3sm_to_cmip_main/e3sm_to_cmip/qa/master/CMIP6/CMIP/E3SM-Project/E3SM-1-0/piControl/r1i1p1f1/Amon/pfull/gr/tests/pfull_Amon_E3SM-1-0_piControl_r1i1p1f1_gr_185001-185012-clim.nc"

ds1 = xr.open_dataset(path_d)
ds2 = xr.open_dataset(path_m)

compare_datasets(ds1, ds2)


# %%
"""Findings:
1. Looping over time coordinates results in different "ps"/"PS" values -- removing loop removes this diff
np.testing.assert_allclose(ds1.ps, ds2.ps, atol=0, rtol=0)
"""

"""Findings:
2. Large difference in pfull/phalf values -- might have to do with reconstructPressureFromHybrid
"""

np.testing.assert_allclose(ds1.pfull, ds2.pfull, atol=0, rtol=0)


# Steps:
# 1. Get the MF dataset produced by Xarray and write to netCDF
# 2. Open that MF dataset and run reconstruct pressure to hybrid using Xarray
# and CDAT
# 3. Compare the results
# 4. If results are identical or close, that means those APIs are basically the same
# - Somewhere else in the code is causing differences (maybe CMOR, maybe getting the data?)
# 5. If results are not close, that means those APIs are behaving differenly

import cdms2
import cdutil
import numpy as np
import xarray as xr

from e3sm_to_cmip.cmor_handlers._formulas import pfull

ds_xr = xr.open_dataset("115-branch-raw-pfull.netcdf")
data = cdms2.open("115-branch-raw-pfull.netcdf")

res_xr = pfull(ds_xr)

# Documentation:
# https://github.com/CDAT/cdutil/blob/b823b69db46bb76536db7d435e72075fc3975c65/cdutil/vertical.py#L8-L49
res_cd = cdutil.reconstructPressureFromHybrid(
    data["PS"], data["hyam"], data["hybm"], 100000
)

# Make sure FILL_VALUE is np.nan to align with Xarray.
# res_cd = res_cd.filled(np.nan)
np.testing.assert_allclose(res_xr.values, res_cd.data, atol=0, rtol=0)

# AssertionError:
# Not equal to tolerance rtol=1e-07, atol=0

# (shapes (72, 12, 180, 360), (12, 72, 180, 360) mismatch)
#  x: array([[[[1.238254e+01, 1.238254e+01, 1.238254e+01, ..., 1.238254e+01,
#           1.238254e+01, 1.238254e+01],
#          [1.238254e+01, 1.238254e+01, 1.238254e+01, ..., 1.238254e+01,...
#  y: array([[[[1.238254e+01, 1.238254e+01, 1.238254e+01, ..., 1.238254e+01,
#           1.238254e+01, 1.238254e+01],
#          [1.238254e+01, 1.238254e+01, 1.238254e+01, ..., 1.238254e+01,...
# %%
# Reshape the data
res_xr = res_xr.transpose("time", "lev", "lat", "lon")

# All close
np.testing.assert_allclose(res_xr.values, res_cd.data, atol=0, rtol=0)

# -----
# Next step: Check input datasets, they are good to go.

ds1 = xr.open_dataset("115-branch-raw-pfull.nc")
ds2 = xr.open_dataset("master-branch-raw-pfull.nc")


def allclose(arr1, arr2, atol=0, rtol=0):
    np.testing.assert_allclose(arr1, arr2, atol=atol, rtol=rtol)


for var in ds2.data_vars.keys():
    print(var)
    allclose(ds1[var].values, ds2[var].values)

# -----
# Next step: Check values after reconstructing pressure to hybrid
import xarray as xr
import numpy as np

ds1 = xr.open_dataset("115-branch-final-phalf.nc")
ds2 = xr.open_dataset("master-branch-final-phalf.nc")

# The Xarray result has "lev" first
allclose(ds1.phalf.values, ds2.phalf.values)

# %%

import numpy as np
import xarray as xr

path_d = "/p/user_pub/e3sm/e3sm_to_cmip/test-cases/115-branch/CMIP6/CMIP/E3SM-Project/E3SM-1-0/piControl/r1i1p1f1/Amon/phalf/gr/tests/phalf_Amon_E3SM-1-0_piControl_r1i1p1f1_gr_185001-185012-clim.nc"
path_m = "/p/user_pub/e3sm/e3sm_to_cmip/test-cases/master/CMIP6/CMIP/E3SM-Project/E3SM-1-0/piControl/r1i1p1f1/Amon/phalf/gr/tests/phalf_Amon_E3SM-1-0_piControl_r1i1p1f1_gr_185001-185012-clim.nc"

ds1 = xr.open_dataset(path_d)
ds2 = xr.open_dataset(path_m)


np.testing.assert_allclose(ds1.ps, ds2.ps, atol=0, rtol=0)
np.testing.assert_allclose(ds1.phalf.values, ds2.phalf.values, atol=0, rtol=0)
