# e3sm_to_cmip

A cli utility to transform E3SM model output into CMIP compatible data.

[Documentation](https://e3sm-project.github.io/e3sm_to_cmip)


## Installation

There are two ways to install the e3sm_to_cmip package, either use the pre-build conda version which will bring all the dependencies with it, or install the dependencies yourself and use the source code.

### <ins>conda</ins>

You can install the `e3sm_to_cmip` conda package directly from the `e3sm` channel:
```
conda create -n e2c -c conda-forge -c e3sm e3sm_to_cmip
```

Get a copy of the CMIP6 Controlled Vocabulary tables
```
git clone https://github.com/PCMDI/cmip6-cmor-tables.git
```

### <ins>source</ins>

To install from source, you'll need to setup your own environment and install the dependencies yourself. The required packages are:

    - python >=3.7
    - nco
    - cmor >=3.6.0
    - cdutil
    - cdms2 >=3.1
    - tqdm
    - pyyaml
    - xarray
    - netcdf4
    - dask
    - scipy

Once you have the required packages installed, simply run

```
git clone https://github.com/E3SM-Project/e3sm_to_cmip.git
cd e3sm_to_cmip
python setup.py install
```



### Example

Here's an example of the tool usage, with the variables tas, prc, and rlut. The time-series files containing the regridded output are in a directory named input_path, and a directory named output_path will be used to hold the CMIP6 output.

```
e3sm_to_cmip -v tas, prc, rlut --input ./input_path/ --output ./output_path/ -t ~/cmip6-cmor-tables -u e3sm_user_config_picontrol.json
```

This will produce a directory tree named CMIP6 below the output_path, with the CMIP6 directory tree based on the metadata json file. 