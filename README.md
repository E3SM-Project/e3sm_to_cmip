# e3sm_to_cmip

A cli utility to transform E3SM model output into CMIP compatible data.

[![Anaconda-Server Badge](https://anaconda.org/conda-forge/e3sm_to_cmip/badges/version.svg)](https://anaconda.org/conda-forge/e3sm_to_cmip)
[![Anaconda-Server Badge](https://anaconda.org/conda-forge/e3sm_to_cmip/badges/downloads.svg)](https://anaconda.org/conda-forge/e3sm_to_cmip)
[![CI/CD Build Workflow](https://github.com/E3SM-Project/e3sm_to_cmip/actions/workflows/build_workflow.yml/badge.svg)](https://github.com/E3SM-Project/e3sm_to_cmip/actions/workflows/build_workflow.yml)
[![Ruff Badge](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

[Documentation](https://e3sm-to-cmip.readthedocs.io/en/latest/)

## Installation

There are two ways to install the e3sm_to_cmip package. Either use (1) the pre-built conda package through `conda-forge`, which will bring all the dependencies with it, or (2) setup the local development environment to install dependencies and use the source code directly.

### <ins>(1) Conda</ins>

You can install the `e3sm_to_cmip` conda package directly from the `conda-forge` channel:

```
conda create -n e2c -c conda-forge e3sm_to_cmip
```

Get a copy of the CMIP6 Controlled Vocabulary tables

```
git clone https://github.com/PCMDI/cmip6-cmor-tables.git
```

### <ins>(2) Conda Development Environment and Source Code</ins>

First, clone the repo and set up the conda dev environment:

```
git clone https://github.com/E3SM-Project/e3sm_to_cmip.git
cd e3sm_to_cmip
conda env create -f conda-env/dev.yml
conda activate e3sm_to_cmip_dev
```

Once you have dev environment setup, simply run:

```bash
python -m pip install .
```

Hint: Before re-installing, running `make clean` can ensure a clean installation.

## Example

Here's an example of the tool usage, with the variables tas, prc, and rlut. The time-series files containing the regridded output are in a directory named input_path, and a directory named output_path will be used to hold the CMIP6 output. A copy of an example metadata definition file (default_metadata.json) can be found [here](https://github.com/E3SM-Project/e3sm_to_cmip/blob/master/e3sm_to_cmip/resources/default_metadata.json).

```
e3sm_to_cmip -v tas, prc, rlut --realm atm --input ./input_path/ --output ./output_path/ -t ~/cmip6-cmor-tables/Tables -u default_metadata.json
```

This will produce a directory tree named CMIP6 below the output_path, with the CMIP6 directory tree based on the metadata json file.
