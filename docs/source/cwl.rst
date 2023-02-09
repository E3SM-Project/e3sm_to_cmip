.. _cwl workflows:

*************
CWL Workflows
*************

There is a set of CWL workflow scripts in the repository (``/scripts/cwl_workflows``) for each realm. Each workflow breaks the input files up into manageable segment size and perform all the required input processing needed before invoking ``e3sm_to_cmip``. These scripts have been designed to run on a SLURM cluster in parallel and will process an arbitrarily large set of simulation data in whatever chunk size required.


Setting up your CWL environment
###############################

To use the CWL workflows you will need additional dependencies in your environment:

.. code-block:: text

    conda install -c conda-forge cwltool nodejs

When CWL runs it needs somewhere to store its intermediate files. By default it will use the systems $TMPDIR
but in some cases that wont work, for example on NERSC the compute nodes wont have access to the login nodes /tmp directory.
An easy solution for this is to create a directory on a shared mount, and run ``export TMPDIR=/path/to/shared/location`` and
then when running the cwltool use the ``--tmpdir-prefix=$TMPDIR`` argument.

Using the CWL Workflows
#######################

Each of the directories under ``scripts/cwl_workflows`` holds a single self-contained workflow.
The name of the workflow matches the name of the directory, for example under the mpaso directory is a file named ``mpaso.cwl`` which contains the workflow.

The beginning of each workflow contains an ``inputs`` section which defines the required parameters, for example

.. code-block:: yaml

    inputs:
        data_path: string
        metadata: File
        workflow_output: string

        mapfile: File
        frequency: int

        namelist_path: string
        region_path: string
        restart_path: string

        tables_path: string
        cmor_var_list: string[]

        timeout: int
        partition: string
        account: string

Along with each of the cwl workflows is an example yaml parameter file, for example along with ``mpaso.cwl`` is 
``mpaso-job.yaml`` which contains the following:

.. code-block:: yaml

    data_path: /p/user_pub/e3sm/staging/prepub/1_1_ECA/ssp585-BDRD//1deg_atm_60-30km_ocean/ocean/native/model-output/mon/ens1/v0/
    workflow_output: /p/user_pub/e3sm/baldwin32/workshop/ssp585/ssp585/output/pp/cmor/ssp585/2015_2100
    
    metadata:
        class: File
        path: /p/user_pub/e3sm/baldwin32/workshop/ssp585/ssp585/output/pp/cmor/ssp585/2015_2100/user_metadata.json
    mapfile:
        class: File
        path: /export/zender1/data/maps/map_oEC60to30v3_to_cmip6_180x360_aave.20181001.nc

    frequency: 5
    namelist_path: /p/user_pub/e3sm/baldwin32/workshop/E3SM-1-1-ECA.hist-bgc/mpaso_in
    region_path: /p/user_pub/e3sm/baldwin32/resources/oEC60to30v3_Atlantic_region_and_southern_transect.nc
    restart_path: /p/user_pub/e3sm/baldwin32/workshop/E3SM-1-1-ECA.hist-bgc/mpaso.rst.1851-01-01_00000.nc
    tables_path: /export/baldwin32/projects/cmor/Tables

    timeout: 10:00:00
    account: e3sm
    partition: debug

    cmor_var_list: [masso, volo, thetaoga, tosga, soga, sosga, zos, masscello, tos, tob, sos, sob, mlotst, fsitherm, wfo, sfdsi, hfds, tauuo, tauvo, thetao, so, uo, vo, wo, hfsifrazil, zhalfo]

Once the parameter file is complete, the workflow can be executed by calling the cwltool

.. code-block:: text

    cwltool --tmpdir-prefix=$TMPDIR ~/projects/e3sm_to_cmip/scripts/cwl_workflows/mpaso/mpaso.cwl mpaso-job.yaml

