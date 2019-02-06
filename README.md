# e3sm_to_cmip

A cli utility to transform E3SM land model output into CMIP compatible data.

## e3sm_to_cmip

Transform e3sm time series variables into cmip compatible data. Each variable needs its own handler script, implemented in the cmor_handlers directory (see directory for current handlers). In addition, you will need to clone [the cmor repo](https://github.com/PCMDI/cmor) to access the Test and Tables directories. Test holds the common_user_input.json file which can be used as a placeholder for the user supplied metadata, and Tables holds all the CMIP6 variable tables.


```
usage: e3sm_to_cmip [-h]

Convert ESM model output into CMIP compatible format

optional arguments:
  -h, --help            show this help message and exit
  -v  [ ...], --var-list  [ ...]
                        space seperated list of variables to convert from e3sm
                        to cmip. Use 'all' to convert all variables
  -i , --input          path to directory containing e3sm data with single
                        variables per file
  -o , --output         where to store cmorized output
  -u <user_input_json_path>, --user-input <user_input_json_path>
                        path to user input json file for CMIP6 metadata
  -n <nproc>, --num-proc <nproc>
                        optional: number of processes, default = 6
  -t <tables-path>, --tables <tables-path>
                        Path to directory containing CMOR Tables directory
  -H <handler_path>, --handlers <handler_path>
                        path to cmor handlers directory, default =
                        ./cmor_handlers
  -N, --proc-vars       Set the number of process to the number of variables
  --version             print the version number and exit
  --debug               Set output level to debug
```
