.. _usage:

Usage
=====

The code block below shows the available flags when running ``e3sm_to_cmip``.
Please be aware that some arguments are required or optional based on how ``e3sm_to_cmip``.

.. code-block:: console

   $ e3sm_to_cmip --help
   usage: e3sm_to_cmip [-h]
   Convert ESM model output into CMIP compatible format
   required arguments (general):
   -v  [ ...], --var-list  [ ...]
                           Space separated list of variables to convert from
                           E3SM to CMIP.
   required arguments (without --info):
   -i , --input-path     Path to directory containing e3sm time series data
                           files. Additionally namelist, restart, and 'region'
                           files if handling MPAS data. Region files are
                           available from https://web.lcrc.anl.gov/public/e3sm/i
                           nputdata/ocn/mpas-o/<mpas_mesh_name>.
   required arguments (without --simple):
   -o , --output-path    Where to store cmorized output.
   -t <tables-path>, --tables-path <tables-path>
                           Path to directory containing CMOR Tables directory,
                           required unless the `--simple` flag is used.
   -u <user_input_json_path>, --user-metadata <user_input_json_path>
                           Path to user json file for CMIP6 metadata, required
                           unless the `--simple` flag is used.
   required arguments (with --realm [mpasso|mpassi]):
   --map <map_mpas_to_std_grid>
                           The path to an mpas remapping file. Required if realm
                           is 'mpaso' or 'mpassi'. Available from
                           https://web.lcrc.anl.gov/public/e3sm/mapping/maps/.
   optional arguments (general):
   -n <nproc>, --num-proc <nproc>
                           Optional: number of processes, default = 6. Not used
                           when -s, --serial specified.
   --debug               Set output level to debug.
   --timeout TIMEOUT     Exit with code -1 if execution time exceeds given
                           time in seconds.
   -H <handler_path>, --handlers <handler_path>
                           Path to cmor handlers directory, default is the
                           (built-in) 'e3sm_to_cmip/cmor_handlers'.
   --precheck PRECHECK   Check for each variable if it's already in the output
                           CMIP6 directory, only run variables that don't have
                           pre-existing CMIP6 output.
   --logdir LOGDIR       Where to put the logging output from CMOR.
   --custom-metadata CUSTOM_METADATA
                           The path to a json file with additional custom
                           metadata to add to the output files.
   optional arguments (run mode):
   --info                Print information about the variables passed in the
                           --var-list argument and exit without doing any
                           processing. There are three modes for getting the
                           info, if you just pass the --info flag with the
                           --var-list then it will print out the information for
                           the requested variable. If the --freq <frequency> is
                           passed along with the --tables-path, then the CMIP6
                           tables will get checked to see if the requested
                           variables are present in the CMIP6 table matching the
                           freq. If the --freq <freq> is passed with the
                           --tables-path, and the --input-path, and the input-
                           path points to raw unprocessed E3SM files, then an
                           additional check will me made for if the required raw
                           variables are present in the E3SM output.
   --simple              Perform a simple translation of the E3SM output to
                           CMIP format, but without the CMIP6 metadata checks.
                           (WARNING: NOT WORKING AS OF 1.8.2)
   -s, --serial          Run in serial mode (by default parallel). Useful for
                           debugging purposes.
   optional arguments (run settings):
   --realm <realm>       The realm to process. Must be atm, lnd, mpaso or
                           mpassi. Default is atm.
   -f FREQ, --freq FREQ  The frequency of the data (default is 'mon' for
                           monthly). Accepted values are 'mon', 'day', '6hrLev',
                           '6hrPlev', '6hrPlevPt', '3hr', '1hr.
   optional arguments (with --info):
   --info-out INFO_OUT   If passed with the --info flag, will cause the
                           variable info to be written out to the specified file
                           path as yaml.
   helper arguments:
   -h, --help            show this help message and exit
   --version             Print the version number and exit.


Additional descriptions of some of the arguments can be found below.

Required arguments (general):
-----------------------------
Variable List
^^^^^^^^^^^^^
The "--var-list" or "-v" flag is a mandatory option, and should be a list of CMIP6 variable names to be output.

Required arguments (without --info)
------------------------------------
Input Path
^^^^^^^^^^
This mandatory flag should point at a directory containing the data files to be processed.

User Input Metadata
^^^^^^^^^^^^^^^^^^^
The "--user-metadata" or "-u" flag should be the path to a json formatted metadata file containing CMIP6 metadata for the case being processed. This flag can be avoided for
non-official data by using the "--simple" flag. Otherwise, the file should look something like the metadata files `that can be found here <https://github.com/E3SM-Project/CMIP6-Metadata>`_

Tables Path
^^^^^^^^^^^
The "--tables-path" or "-t" flag should point to the "Tables" directory of the CMIP6 controlled vocabulary repository.
The repository `can be found here <https://github.com/PCMDI/cmip6-cmor-tables/>`_

Required arguments (without --simple)
-------------------------------------
Output Path
^^^^^^^^^^^
This mandatory flag is the location that all output files will be placed. The main output is a directory named CMIP6, which contains the CMIP6
directory structure, with the output files as leaf nodes. Other output files include a copy of the user metadata (if present), and a directory named
cmor_logs containing the per-variable log files generated by CMOR.


Required arguments (with --realm [mpasso|mpassi])
-------------------------------------------------
MPAS mapfile
^^^^^^^^^^^^
When processing MPAS ocean or sea-ice variables, a mapfile is needed for regridding. Use the "--map" flag to pass the path to this mapfile.

Optional arguments (general)
----------------------------
Numproc
^^^^^^^
By default, the variable converters are run in parallel using a process pool with 6 worker processes. The "--num-proc" or "-n" flag can be used to control the number
of simultaneously executing processes. For example, 3D ocean fields take significantly more RAM then other variables, so the number of converters running at once
may be reduced to accommodate the machine being used.

Handler Path
^^^^^^^^^^^^
A directory of custom variable handlers can be passed using the "--handlers" or "-H" flag.

Custom Metadata
^^^^^^^^^^^^^^^
Additional custom metadata can be added to the global attributes of the output files by using the "--custom-metadata" flag to point to a json formatted
file containing the metadata key value pairs.

Optional arguments (run mode)
-----------------------------
Info
^^^^
The "--info" flag can be used in three different ways to determine information about the variables being requested for processing. In the simplest form, passing only
the "--info" and "--var-list" flags will return information about the required input and CMIP6 output names of the variables passed in the variable list.

If the --freq <frequency> is passed along with the --tables-path, then the CMIP6 tables will get checked to see if the requested variables are present in the CMIP6 table matching the freq.

If the --freq <freq> is passed with the --tables-path, and the --input-path, and the input-path points to raw unprocessed E3SM files, then an additional check will me made for if the required raw variables are present in the E3SM output.
In this last mode, instead of passing a directory of time-series files as the input path, pass the path to raw unprocessed E3SM cam or eam files.

Simple
^^^^^^
This optional flag will cause the tool to run without needing or checking for the custom CMIP metadata usually required for processing. Output from this mode
use the same converter code as the default mode, but the output doesnt contain the required metadata needed for a CMIP publication. This mode should be used
when the output is intended for analysis, but is not suited for publication.

Serial
^^^^^^
For debugging purposes, or when running in a resource constrained environment, the "--serial" or "-s" boolean flag can be used to cause the conversion process
to be run in serial, using the main process.

Optional arguments (run settings)
---------------------------------
Realm
^^^^^^^^^
The type of realm being operated on should be specified using the "--realm" flag. Allowed values are "atm", "lnd", "mpaso" and "mpassi." This is needed so that the package
can correctly determine what type of input files to look for. By default "atm".

Frequency
^^^^^^^^^
The "--freq" and "-f" flags can be used to process high-frequency datasets. By default the tool assumes its working with monthly data. The following submonthly frequencies
are supported: [6hr, 6hrLev, 6hrPlev, 3hr, day]
