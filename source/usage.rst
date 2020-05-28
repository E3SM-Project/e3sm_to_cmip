.. _usage:

*****
Usage
*****


    usage: e3sm_to_cmip

    Convert ESM model output into CMIP compatible format

    optional arguments:
        -h, --help            show this help message and exit
        -v  [ ...], --var-list  [ ...]
                            space seperated list of variables to convert from e3sm to cmip. Use 'all' to convert all variables or the name of a CMIP6 table to run all handlers from that table
        -i, --input-path     path to directory containing e3sm time series data files. Additionally namelist, restart, and mappings files if handling MPAS data.
        -o, --output-path    where to store cmorized output
        -u <user_input_json_path>, --user-metadata <user_input_json_path>
                            path to user json file for CMIP6 metadata
        -t <tables-path>, --tables-path <tables-path>
                            Path to directory containing CMOR Tables directory
        --map <map_mpas_to_std_grid>
                            The path to an mpas remapping file. Must be used when using the --mpaso or --mpassi options
        -n <nproc>, --num-proc <nproc>
                            optional: number of processes, default = 6
        -H <handler_path>, --handlers <handler_path>
                            path to cmor handlers directory, default = e3sm_to_cmip/cmor_handlers
        --no-metadata         Do not add E3SM metadata to the output
        --only-metadata       Update the metadata for any files found and exit
        -s, --serial          Run in serial mode, usefull for debugging purposes
        --debug               Set output level to debug
        --mode <mode>         The component to analyze, atm, lnd, mpaso or mpassi
        --logdir LOGDIR       Where to put the logging output from CMOR
        --timeout TIMEOUT     Exit with code -1 if execution time exceeds given time in seconds
        --precheck PRECHECK   Check for each variable if its already in the output CMIP6 directory, only run variables that dont have CMIP6 output
        --version             print the version number and exit
