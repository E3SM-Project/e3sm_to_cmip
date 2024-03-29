.. e3sm_to_cmip documentation master file, created by
   sphinx-quickstart on Tue Apr 13 10:10:31 2021.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

****************************
e3sm_to_cmip's documentation
****************************

The ``e3sm_to_cmip`` package converts E3SM model output variables to the CMIP standard format.
The tool supports variables in the atmospheric, land, MPAS ocean, and MPAS sea-ice realms. The handling of each realm is slightly different, so care must be made when dealing with the various data types (refer to :ref:`Preprocessing Data by Realm <preprocessing>`)

Usage
~~~~~

First, follow the :ref:`Getting Started <getting started>` page for how to access ``e3sm_to_cmip`` in an Anaconda environment.

To invoke ``e3sm_to_cmip`` package directly on appropriately pre-processed input files:

   - :ref:`Usage Guide<usage>`
   - :ref:`Examples<examples>`

.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: For users

   Getting Started <getting-started>
   Preprocessing Data by Realm <preprocessing>
   Usage Guide <usage>
   Examples <examples>

.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: For developers/contributors

   Variable Handlers <var-handlers>
   Dealing with High Frequency Data <high_freq>
   Development Guide <development-guide>
