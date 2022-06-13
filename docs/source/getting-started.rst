.. _getting started:

Getting Started
===============

Prerequisites
-------------

``e3sm_to_cmip`` is distributed through conda, which is available through Anaconda and Miniconda.
The instruction to install conda from Miniconda is provided as follows:

::

   wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
   bash Miniconda3-latest-Linux-x86_64.sh

Then follow the instructions for installation. To have conda added to
your path you will need to type ``yes`` in response to "Do you wish the
installer to initialize Miniconda3 by running conda init?" (we recommend
that you do this). Note that this will modify your shell profile (e.g.,
``~/.bashrc``) to add ``conda`` to your path.

Note: After installation completes you may need to type ``bash`` to
restart your shell (if you use bash). Alternatively, you can log out and
log back in.


Installation
------------

1. Create a conda environment from scratch with ``e3sm_to_cmip`` (`conda create`_)

   We recommend using the Conda environment creation procedure to install ``e3sm_to_cmip``.
   The advantage with following this approach is that Conda will attempt to resolve
   dependencies (e.g. ``python >= 3.8``) for compatibility.

   To create an ``e3sm_to_cmip`` conda environment run:

   .. code-block:: console

      conda create -n <ENV_NAME> -c conda-forge e3sm_to_cmip
      conda activate <ENV_NAME>

2. Install ``e3sm_to_cmip`` in an existing conda environment (`conda install`_)

   You can also install ``e3sm_to_cmip`` in an existing Conda environment, granted that Conda
   is able to resolve the compatible dependencies.

   .. code-block:: console

      conda activate <ENV_NAME>
      conda install -c conda-forge e3sm_to_cmip

.. _conda create: https://docs.conda.io/projects/conda/en/latest/commands/create.html?highlight=create
.. _conda install: https://docs.conda.io/projects/conda/en/latest/commands/install.html?highlight=install

3. Conda Development Environment and Source Code

   This environment is intended for developers.

   First, clone the repo and set up the conda dev environment:

   .. code-block:: console

      git clone https://github.com/E3SM-Project/e3sm_to_cmip.git
      cd e3sm_to_cmip
      conda env create -f conda/dev.yml
      conda activate e3sm_to_cmip_dev

   Once you have dev environment setup, simply run:

   .. code-block:: console

      pip install .

Additional Dependencies
-----------------------

The following dependencies are required to run ``e3sm_to_cmip`` for all normal runs (``--info`` or ``--simple`` flag not specified).

CMIP6 Controlled Vocabulary Tables
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This repository needs to be cloned because it is not currently available as a data package on ``conda-forge``.

1. Clone the repository

.. code-block:: console

   git clone https://github.com/PCMDI/cmip6-cmor-tables.git


2. Example usage with ``e3sm_to_cmip``

.. code-block:: console

      e3sm_to_cmip --help

     -t <tables-path>, --tables-path <tables-path>
                        Path to directory containing CMOR Tables directory,
                        required unless the --simple flag is used.

CMIP6 Metadata Tables
~~~~~~~~~~~~~~~~~~~~~

This repository needs to be cloned because it is not currently available as a data package on ``conda-forge``.

1. Clone the repository

.. code-block:: console

   git clone https://github.com/E3SM-Project/CMIP6-Metadata.git

2. Example usage with ``e3sm_to_cmip``

.. code-block:: console

      e3sm_to_cmip --help

     -u <user_input_json_path>, --user-metadata <user_input_json_path>
                        Path to user json file for CMIP6 metadata, required
                        unless the --simple flag is used.
