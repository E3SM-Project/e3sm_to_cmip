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
                        space sepperated list of variables, use 'all' to
                        extract all variables
  -c <case_id>, --case-id <case_id>
                        name of case, e.g.
                        20180129.DECKv1b_piControl.ne30_oEC.edison
  -i , --input-path     path to input directory
  -o , --output-path    path to output directory
  -s , --start-year     first year to extract
  -e , --end-year       last year to split
  -n , --num-proc       number of parallel processes, default = 6
  -d , --data-type      The type of data to extract from, e.g. clm2.h0 or
                        cam.h0. Defaults to cam.h0
  -N, --proc-vars       set the number of process to the number of variables
  --version             print the version number and exit
```

## e3sm_to_cmip

Transform e3sm time series variables into cmip compatible data. Each variable needs its own handler script, implemented in the cmor_handlers directory (see directory for current handlers). In addition, you will need to clone [the cmor repo](https://github.com/PCMDI/cmor) and link in the Tables and Test directories.

Currently can only handle SNOWDP and EFLX_LH_TOT variables with more to come as they're written. See clm_to_cmip_translation_reference.txt for a limited human readable reference between the two variable types.

```
usage: e3sm_to_cmip [-h]

Convert ESM model output into CMIP compatible format

optional arguments:
  -h, --help            show this help message and exit
  -v  [ ...], --var-list  [ ...]
                        space seperated list of variables to convert from clm
                        to cmip
  -c <case_id>, --caseid <case_id>
                        name of case e.g.
                        20180129.DECKv1b_piControl.ne30_oEC.edison
  -i , --input          path to directory containing clm data with single
                        variables per file
  -o , --output         where to store cmorized outputoutput
  -n <nproc>, --num-proc <nproc>
                        optional: number of processes, default = 6
  -H <handler_path>, --handlers <handler_path>
                        path to cmor handlers directory, default =
                        ./cmor_handlers
  --version             print the version number and exit
```