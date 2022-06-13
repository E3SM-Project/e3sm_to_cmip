e3sm_to_cmip
============

A cli utility to transform E3SM model output into CMIP compatible data.

|Anaconda-Server Badge| |Anaconda-Server Downloads|

|CI/CD Build Workflow|

`Documentation <https://e3sm-to-cmip.readthedocs.io/en/latest/>`__
`Usage Guide <https://e3sm-project.github.io/e3sm_to_cmip/_build/html/master/usage.html>`__
`Installation Instructions <https://e3sm-project.github.io/e3sm_to_cmip/_build/html/master/getting-started.html>`__

Example
-------

Here’s an example of the tool usage, with the variables tas, prc, and
rlut. The time-series files containing the regridded output are in a
directory named input_path, and a directory named output_path will be
used to hold the CMIP6 output.

::

   e3sm_to_cmip -v tas, prc, rlut --realm atm --input ./input_path/ --output ./output_path/ -t ~/cmip6-cmor-tables -u e3sm_user_config_picontrol.json

This will produce a directory tree named CMIP6 below the output_path,
with the CMIP6 directory tree based on the metadata json file.

Acknowledgement
---------------

This work was produced under the auspices of the U.S. Department of
Energy by Lawrence Livermore National Laboratory under Contract
DE-AC52-07NA27344.

This work was prepared as an account of work sponsored by an agency of
the United States Government. Neither the United States Government nor
Lawrence Livermore National Security, LLC, nor any of their employees
makes any warranty, expressed or implied, or assumes any legal liability
or responsibility for the accuracy, completeness, or usefulness of any
information, apparatus, product, or process disclosed, or represents
that its use would not infringe privately owned rights.

Reference herein to any specific commercial product, process, or service
by trade name, trademark, manufacturer, or otherwise does not
necessarily constitute or imply its endorsement, recommendation, or
favoring by the United States Government or Lawrence Livermore National
Security, LLC.

The views and opinions of authors expressed herein do not necessarily
state or reflect those of the United States Government or Lawrence
Livermore National Security, LLC, and shall not be used for advertising
or product endorsement purposes.

.. |Anaconda-Server Badge| image:: https://anaconda.org/conda-forge/e3sm_to_cmip/badges/version.svg
   :target: https://anaconda.org/conda-forge/e3sm_to_cmip
.. |Anaconda-Server Downloads| image:: https://anaconda.org/conda-forge/e3sm_to_cmip/badges/downloads.svg
   :target: https://anaconda.org/conda-forge/e3sm_to_cmip
.. |CI/CD Build Workflow| image:: https://github.com/E3SM-Project/e3sm_to_cmip/actions/workflows/build_workflow.yml/badge.svg
   :target: https://github.com/E3SM-Project/e3sm_to_cmip/actions/workflows/build_workflow.yml
.. |CI/CD Release Workflow| image:: https://github.com/E3SM-Project/e3sm_to_cmip/actions/workflows/release_workflow.yml/badge.svg
   :target: https://github.com/E3SM-Project/e3sm_to_cmip/actions/workflows/release_workflow.yml
