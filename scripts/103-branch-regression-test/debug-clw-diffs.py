"""This scripts debugs why the `clw` datasets are not identical between branches.

CONCLUSION: There is no discernible difference between the datasets.
The datasets are not equal or identical because of extremely small floating
point differences in `clw` and `b_bnds` variables.
"""
import xarray as xr
from xcdat import compare_datasets

clw_paths = [
    "/p/user_pub/e3sm/e3sm_to_cmip/test-cases/master/CMIP6/CMIP/E3SM-Project/E3SM-1-0/piControl/r1i1p1f1/Amon/clw/gr/tests/clw_Amon_E3SM-1-0_piControl_r1i1p1f1_gr_185001-185012.nc",
    "/p/user_pub/e3sm/e3sm_to_cmip/test-cases/103-branch/CMIP6/CMIP/E3SM-Project/E3SM-1-0/piControl/r1i1p1f1/Amon/clw/gr/tests/clw_Amon_E3SM-1-0_piControl_r1i1p1f1_gr_185001-185012.nc",
]

ds1 = xr.open_dataset(clw_paths[0])
ds2 = xr.open_dataset(clw_paths[1])

compare_datasets(ds1, ds2)
# {'unique_coords': [],
#  'unique_data_vars': [],
#  'nonidentical_coords': [],
#  'nonidentical_data_vars': ['clw', 'b_bnds'],
#  'nonequal_coords': [],
#  'nonequal_data_vars': ['b_bnds']}

xr.testing.assert_allclose(ds1.clw, ds2.clw)
# No assertion error raised

xr.testing.assert_allclose(ds1.b_bnds, ds2.b_bnds)
# No assertion error raised
