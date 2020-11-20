.. e3sm_to_cmip documentation master file, created by
   sphinx-quickstart on Thu May 28 14:08:09 2020.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

****************************
e3sm_to_cmip's documentation
****************************

Welcome to e3sm_to_cmip's documentation!
========================================

The e3sm_to_cmip tool converts E3SM model output variables to the CMIP standard format. 
The tool works for atmospheric, land, MPAS-Ocean, and MPAS-Sea-Ice variables. The handling of each component is slightly different, so care
must be made when dealing with the various data types. 

Along with the e3sm_to_cmip command-line tool, there is also a set of workflow scripts for each component, 
which break the input files up into managable segment size and perform all the required input processing needed before
invoking the e3sm_to_cmip tool itself. These scripts have been designed to run on a SLURM cluster in parallel and will 
process an arbitraraly large set of simulation data in whatever chunk size required.


**INSTALLATION**

conda:

.. code-block:: bash

   conda create -n e2c -c conda-forge -c e3sm e3sm_to_cmip==1.4.1=1 libcdms==4.7.4


source:

.. code-block:: bash

   git clone https://github.com/E3SM-Project/e3sm_to_cmip.git
   cd e3sm_to_cmip
   python setup.py install


If installing from source, the tool will require an environment with the following packages:

.. code-block::

   - python >=3
   - nco
   - cmor >=3.5.0
   - libcdms >=4.7.4
   - cdutil
   - cdms2 >=3.1
   - tqdm
   - pathos
   - pyyaml
   - xarray
   - netcdf4
   - dask
   - scipy


**Usage**

There are two main ways to run the CMIP converters, either by invoking the e3sm_to_cmip package directly on the appropriately pre-processed input files,
or by using the automated CWL workflows provided in the scripts/cwl_workflows directory in the repository.

The e3sm_to_cmip package can operate on 4 different components: atmosphere, land, ocean, and sea-ice. The input data for each of these is different.

atmosphere
----------

Processing atmosphere variables requires that each of the input variables be provided in regridded time-series files (multiple files spanning different time segments is allowed), 
and follow the NCO naming format of VARNAME_START_END.nc, for example PRECC_185001_201412.nc 

Additionally, a set of 3D atmosphere variables need to be converted from the internal model vertical levels over to the "plev19" levels.


.. toctree::
   :maxdepth: 2
   :caption: Contents:

   usage
   var_map_atm
   var_map_lnd
   var_map_ocn
   var_map_ice

