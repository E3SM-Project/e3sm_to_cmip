.. e3sm_to_cmip documentation master file, created by
   sphinx-quickstart on Tue Apr 13 10:10:31 2021.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

****************************
e3sm_to_cmip's documentation
****************************

The e3sm_to_cmip tool converts E3SM model output variables to the CMIP standard format. 
The tool works for atmospheric, land, MPAS-Ocean, and MPAS-Sea-Ice variables. The handling of each component is slightly different, so care
must be made when dealing with the various data types. 

Along with the e3sm_to_cmip command-line tool, there is also a set of workflow scripts for each component, 
which break the input files up into manageable segment size and perform all the required input processing needed before
invoking the e3sm_to_cmip tool itself. These scripts have been designed to run on a SLURM cluster in parallel and will 
process an arbitrarily large set of simulation data in whatever chunk size required.

.. toctree::
   :maxdepth: 1

   usage
   high_freq
   cwl
   examples

Installation
############


There are two ways to install the e3sm_to_cmip package, either use the pre-build conda version which will bring all the dependencies with it, or install the dependencies yourself and use the source code.
conda

conda
*****

.. code-block:: text

   conda create -n e2c -c conda-forge -c e3sm e3sm_to_cmip

Get a copy of the CMIP6 Controlled Vocabulary tables

.. code-block:: text

   git clone https://github.com/PCMDI/cmip6-cmor-tables.git

Source
******

To install from source, you'll need to setup your own environment and install the dependencies yourself. The required packages are:

.. code-block:: text

   - python >=3.7
   - nco
   - cmor >=3.6.0
   - cdutil
   - cdms2 >=3.1
   - tqdm
   - pyyaml
   - xarray
   - netcdf4
   - dask
   - scipy

Once you have the required packages installed, simply run

.. code-block:: text

   git clone https://github.com/E3SM-Project/e3sm_to_cmip.git
   cd e3sm_to_cmip
   python setup.py install


Usage
#####

There are two main ways to run the CMIP converters, either by invoking the e3sm_to_cmip package directly 
on the appropriately pre-processed input files,
or by using the automated CWL workflows provided in the scripts/cwl_workflows directory in the repository. 

For an example on how to run manually see :ref:`the examples page<examples>`.

.. raw:: html

   <br />

For ane example on how to use the CWL workflows see :ref:`the CWL examples page<CWL Workflows>`.

The e3sm_to_cmip package can operate on 4 different components: atmosphere, land, ocean, and sea-ice. 
The input data for each of these is different.

Atmosphere
**********

Processing atmosphere variables requires that each of the input variables be provided in regridded 
time-series files (multiple files spanning different time segments is supported), 
and follow the NCO naming format of VARNAME_START_END.nc, for example PRECC_185001_201412.nc 

* NOTE: 3D atmosphere variables

A subset of the 3D atmosphere variables require that they be converted from the internal model vertical 
levels over to the "plev19" levels. `A copy of the vertical remap file can be found here <https://github.com/E3SM-Project/e3sm_to_cmip/raw/master/e3sm_to_cmip/resources/vrt_remap_plev19.nco>`_

CMIP6 files requiring this vertical level change are: hur, hus, ta, ua, va, wap, and zg.

Land
****

Similarly to the atmospheric variables, land variables must be provided as regridded time-series files in the NCO naming format.

Ocean
*****

Ocean variables are regridded at run time, and so dont require pre-processing. These files should all be present
in the input directory. Data required for ocean processing are as follows:

1. mpaso.hist.am.timeSeriesStatsMonthly
2. mpaso_in or mpas-o_in
3. one mpaso restart file

Sea-ice
*******

Sea-ice variables are regridded at run time, and so dont require pre-processing. These files should all be present
in the input directory. Data required for ocean processing are as follows:

1. mpascice.hist.am.timeSeriesStatsMonthly or mpassi.hist.am.timeSeriesStatsMonthly
2. mpassi_in or mpas-cice_in
3. one mpassi or mpascice restart file
