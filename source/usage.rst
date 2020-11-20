.. _usage:

*****
Usage
*****

.. code-block:: none

        usage: e3sm_to_cmip [-h]

        Convert ESM model output into CMIP compatible format

        optional arguments:
        -h, --help            show this help message and exit
        -v  [ ...], --var-list  [ ...]
                                Space separated list of variables to convert from e3sm to cmip. Use 'all' to convert all variables or the name of a CMIP6 table to run all handlers from that table
        -i , --input-path     Path to directory containing e3sm time series data files. Additionally namelist, restart, and mappings files if handling MPAS data.
        -o , --output-path    Where to store cmorized output
        --simple              Perform a simple translation of the E3SM output to CMIP format, but without the CMIP6 metadata checks
        -f FREQ, --freq FREQ  The frequency of that data, default is monthly. Accepted values are mon, day, 6hr, 3hr, 1hr
        -u <user_input_json_path>, --user-metadata <user_input_json_path>
                                Path to user json file for CMIP6 metadata, required unless the --simple flag is used
        -t <tables-path>, --tables-path <tables-path>
                                Path to directory containing CMOR Tables directory, required unless the --simple flag is used
        --map <map_mpas_to_std_grid>
                                The path to an mpas remapping file. Required if mode is mpaso or mpassi
        -n <nproc>, --num-proc <nproc>
                                optional: number of processes, default = 6
        -H <handler_path>, --handlers <handler_path>
                                Path to cmor handlers directory, default = e3sm_to_cmip/cmor_handlers
        --custom-metadata CUSTOM_METADATA
                                the path to a json file with additional custom metadata to add to the output files
        -s, --serial          Run in serial mode, usefull for debugging purposes
        --debug               Set output level to debug
        --mode <mode>         The component to analyze, atm, lnd, mpaso or mpassi
        --logdir LOGDIR       Where to put the logging output from CMOR
        --timeout TIMEOUT     Exit with code -1 if execution time exceeds given time in seconds
        --precheck PRECHECK   Check for each variable if its already in the output CMIP6 directory, only run variables that dont have CMIP6 output
        --info                Print information about the variables passed in the --var-list argument and exit without doing any processing. There are three modes for getting the info, if you just pass the --info flag with the --var-list then it will print out the information for the
                                requested variable. If the --freq <frequency> is passed along with the --tables-path, then the CMIP6 tables will get checked to see if the requested variables are present in the CMIP6 table matching the freq. If the --freq <freq> is passed with the
                                --tables-path, and the --input-path, and the input-path points to raw unprocessed E3SM files, then an additional check will me made for if the required raw variables are present in the E3SM output.
        --version             print the version number and exit