# e3sm_to_cmip

A cli utility to transform E3SM land model output into CMIP compatible data.

## singlevar_ts

Extract single variables from clm2.h0 files into single-variable-per-file time series. Extract either all the variables in each of the monthly history files, or just selected variables.

```
usage: singlevar_ts [-h]

Single variable time series extraction for ESM data

optional arguments:
  -h, --help            show this help message and exit
  -v  [ ...], --var-list  [ ...]
                        Space sepperated list of variables, use 'all' to
                        extract all variables
  -c <case_id>, --case-id <case_id>
                        Name of case, e.g.
                        20180129.DECKv1b_piControl.ne30_oEC.edison
  -i , --input-path     Path to input directory
  -o , --output-path    Path to output directory
  -s , --start-year     First year to extract
  -e , --end-year       Last year to split
  -n , --num-proc       Number of parallel processes, default = 6
  -d , --data-type      The type of data to extract from, e.g. clm2.h0 or
                        cam.h0. Defaults to cam.h0
  -N, --proc-vars       Set the number of process to the number of variables
  --version             print the version number and exit
  --debug               Set output level to debug
```

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
  -o , --output         where to store cmorized outputoutput
  -u <user_input_json_path>, --user-input <user_input_json_path>
                        path to user input json file for CMIP6 metadata
  -n <nproc>, --num-proc <nproc>
                        optional: number of processes, default = 6
  -t <tables-path>, --tables <tables-path>
                        Path to directory containing CMOR Tables directory
  -H <handler_path>, --handlers <handler_path>
                        path to cmor handlers directory, default =
                        ./cmor_handlers
  --version             print the version number and exit
```