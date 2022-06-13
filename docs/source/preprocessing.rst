.. _preprocessing:

Preprocessing Data by Realm
===========================

The ``e3sm_to_cmip`` package can operate on variables in four realms: **atmosphere, land, ocean, and sea-ice**. Each realm requires different preprocessing steps for its related input datasets.

Atmosphere
~~~~~~~~~~

Processing atmosphere variables requires that each of the input variables be provided in regridded
time-series files (multiple files spanning different time segments is supported), and follow the NCO naming format of ``VARNAME_START_END.nc`` (e.g., ``PRECC_185001_201412.nc``).

* NOTE: 3D atmosphere variables

A subset of the 3D atmosphere variables require that they be converted from the internal model vertical levels over to the ``"plev19"`` levels. `A copy of the vertical remap file can be found here <https://github.com/E3SM-Project/e3sm_to_cmip/raw/master/e3sm_to_cmip/resources/vrt_remap_plev19.nco>`_

CMIP6 files requiring this vertical level change are: ``"hur", "hus", "ta", "ua", "va", "wap", and "zg"``.

Land
~~~~

Similarly to the atmospheric variables, land variables must be provided as regridded time-series files in the NCO naming format.

MPAS ocean
~~~~~~~~~~

MPAS Ocean variables are regridded at run time, so they don't require preprocessing. These files should all be present in the input directory. Data required for ocean processing are as follows:

1. ``mpaso.hist.am.timeSeriesStatsMonthly``
2. mpaso_in or mpas-o_in
3. one mpaso restart file

MPAS sea-ice
~~~~~~~~~~~~

MPAS sea-ice variables are regridded at run time, and they don't preprocessing. These files should all be present in the input directory. Data required for ocean processing are as follows:

1. ``mpascice.hist.am.timeSeriesStatsMonthly`` or ``mpassi.hist.am.timeSeriesStatsMonthly``
2. mpassi_in or mpas-cice_in
3. one mpassi or mpascice restart file
