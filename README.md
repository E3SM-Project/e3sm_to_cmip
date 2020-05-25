# e3sm_to_cmip

A cli utility to transform E3SM model output into CMIP compatible data.



## Installation


You can install the `e3sm_to_cmip` conda package directly from the `e3sm` channel:
```
conda create -n e2c -c conda-forge -c e3sm e3sm_to_cmip
```

Get a copy of the CMIP6 Controlled Vocabulary tables
```
git clone https://github.com/PCMDI/cmip6-cmor-tables.git
```

### NOTE

Due to a bug in the latest version of CMOR, you will need to either copy or symlink the json table files into the directory which you will be running e3sm_to_cmip from.

If you're not converting a production run, and dont have a defined metadata file for your experiment, you can use the default metadata from the DECK piControl experiment. This will work for converting any E3SM data, but the global attributes will contain some incorrect information.
```
wget https://raw.githubusercontent.com/E3SM-Project/e3sm_to_cmip/master/e3sm_user_config_picontrol.json
```


## Usage

Transform e3sm time series variables into cmip compatible data. Each variable needs its own handler script, implemented in the cmor_handlers directory (see directory for current handlers). The input directory should contain regridded time-series files in the format produced by ncclimo (VARNAME_STARTYEAR_ENDYEAR.nc).


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

### Example

Here's an example of the tool usage, with the variables tas, prc, and rlut. The time-series files containing the regridded output are in a directory named input_path, and a directory named output_path will be used to hold the CMIP6 output.

```
e3sm_to_cmip -v tas, prc, rlut --input ./input_path/ --output ./output_path/ -t ~/cmip6-cmor-tables -u e3sm_user_config_picontrol.json
```

This will produce a directory tree named CMIP6 below the output_path, with the CMIP6 directory tree based on the metadata json file. 


### Debugging

If anything goes wrong, the first thing to try is to envoke the same command, but with the --serial flag turned on. This gives much nicer exception log output. The second thing to look for, is under the output directory is a directory named cmor_logs, inside of which will be detailed per-variable logging output from CMOR.
