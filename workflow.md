# End to End CMIP6 processing tutorial

## Requirements

The CWL workflows currently assume you have a SLURM controller available to submit jobs to via the `srun` command. 


## Setup

First create a new conda environment and install the required packages.

```bash
conda create -n workflow -c conda-forge cwltool nodejs "python>3" nco "cmor>=3.6.0" tqdm pathos pyyaml xarray netcdf4 dask scipy
```

Then pull down a copy of the e3sm_to_cmip repository and install the python modules. 

```bash
git clone https://github.com/E3SM-Project/e3sm_to_cmip.git
cd e3sm_to_cmip
python setup.py install
```

Note that although the e3sm_to_cmip package itself can be installed via conda, the CWL workflows aren't currently packaged along side, so even if you already have the e3sm_to_cmip package in your environment you will still need to clone the repo to get access to the workflow scripts.

## Case metadata

Processing data with CMOR requires a metadata json file containing all the information about the case being processed. Additionally your activities, sources, and institution must be registered with the CMIP6 project. For more information see the CMOR documentation here: https://cmor.llnl.gov/mydoc_cmor3_CV/

A working example of the case metadata file is contained as part of the e3sm_to_cmip repo, and can be accessed here: https://raw.githubusercontent.com/E3SM-Project/e3sm_to_cmip/master/e3sm_user_config_picontrol.json


## CWL workflows

There are several CWL workflows provided under the scripts/cwl_workflows directory. Each directory contains a fully self contained workflow. The primary workflow will have the same name as the directory, for example the workflow for processing fixed variables for the CMIP6 fx table are included in the scripts/cwl_workflows/fx directory with fx.cwl as the file name.

A set of sample parameters needed by the workflows are provided along side the workflow as a YAML file with a name matching the workflow. For example the fx workflow contains a file named fx-job.yaml with the following parameters:

```yaml
# The path to the raw atmos data as a string, this directory should only contain cam.h0 files
atm_data_path: /p/test/ 

# A list of strings containing the CMIP6 variable names
cmor_var_list: 
  - sftlf
  - orog
  - areacella

# a list of strings containing the E3SM variable names
std_var_list:
  - LANDFRAC
  - PHIS
  - area

# The path to the mapfile to use for regridding
map_path: /p/maps/somemap.nc

# the path to the CMOR metadata file 
metadata_path: /p/metadata/case_metadata.json
```

# Workflow Execution

To run the workflows, first create a case directory to contain the required parameter file and to store the output. Copy the CWL parameter file from the workflow directory into the new case directory, and edit it to add the specific values for your case. Then, invoke the cwltool with the path to the workflow file and the parameter file as arguments.

```bash
cwltool ~/workflows/fx/fx.cwl fx-params.yaml
```

To run the tool in parallel mode, add the "--parallel" flag to the cwltool call as the first argument.

## Example

This example will run the most complex of the workflows, atm-unified. This workflow does the full end-to-end processing for both 2d and 3d atmospheric variables.


First setup a directory for this case
```bash
mkdir bgc-historical
```

Then copy the example parameter file into it
```bash
cp ~/projects/e3sm_to_cmip/scripts/cwl_workflows/atm-unified/atm-unified-job.yaml bgc-historical/
```

In an effort to keep everything together, it helps to also store your metadata.json file along side in the same directory.

atm-unified-job.yaml
```yaml
# the full path to the raw E3SM data, this should only contain cam.h0 files
data_path:  /p/user_pub/work/E3SM/1_1_ECA/hist-BDRD/1deg_atm_60-30km_ocean/atmos/native/model-output/mon/ens1/v2/

# the number of years to include in each output file
frequency: 10

# the number of workers for ncremap to use
num_workers: 6

# the slurm account information
account: e3sm
partition: debug
timeout: 2:00:00

# the horizontal regridding map path
hrz_atm_map_path: /export/zender1/data/maps/map_ne30np4_to_cmip6_180x360_aave.20181001.nc

# the vertical interpolation map path
vrt_map_path: /p/user_pub/e3sm/baldwin32/bgc_processing/cmor_test/vrt_remap_plev19.nc

# the CMIP6 controlled vocabulary directory path
tables_path: /export/baldwin32/projects/cmip6-cmor-tables/Tables/

# the CMOR metadata needed for this case
metadata_path: /p/user_pub/e3sm/baldwin32/workshop/bgc-historical/metadata.json

# a list of 2D E3SM variable 
std_var_list: [hyam, hybm, hyai, hybi, TREFHT, TS, PSL, PS, U10, QREFHT, PRECC, PRECL, PRECSC, PRECSL, QFLX, TAUX, TAUY, LHFLX, CLDTOT, FLDS, FLNS, FSDS, FSNS, SHFLX, CLOUD, CLDICE, TGCLDIWP, CLDLIQ, TGCLDCWP, TMQ, FLNSC, FSNTOA, FSNT, FLNT, FLUTC, FSDSC, SOLIN, FSNSC, FSUTOA, FSUTOAC, AODABS, AODVIS, AREL, FISCCP1_COSP, CLDTOT_ISCCP, MEANCLDALB_ISCCP, MEANPTOP_ISCCP, CLDTOT_CAL, CLDLOW_CAL, CLDMED_CAL, CLDHGH_CAL]

# the corresponding 2d CMIP6 variable names
std_cmor_list: [pfull, phalf, tas, ts, psl, ps, sfcWind, huss, pr, prc, prsn, evspsbl, tauu, tauv, hfls, clt, rlds, rlus, rsds, rsus, hfss, cl, clw, cli, clivi, clwvi, prw, rldscs, rlut, rlutcs, rsdt, rsuscs, rsut, rsutcs, rtmt, abs550aer, od550aer, reffclwtop, rsdscs, clisccp, cltisccp, albisccp, pctisccp, cltcalipso, cllcalipso, clmcalipso, clhcalipso]

# the list of 3D E3SM variable names
plev_var_list: [Q,   O3, T,  U,  V,  Z3, RELHUM, OMEGA]
# the corresponding CMIP6 variable names
plev_cmor_list: [hus, o3, ta, ua, va, zg, hur,    wap]
```

## Tips and tricks

The cwltool command has a large number of flags that can be passed to it to change its behavior. Use `cwltool --help` to get a list of possible flags.

One useful thing to take into account is the `$TMPDIR` environment variable. If you're running on a cluster, its possible that each machine will have its own `TMPDIR` that it uses by default, if these directories arent shared between the machines then its likely that there will be an error when one job finishes and tries to pass its output the the next job. Consider setting the TMPDIR variable via an `export` command before running the cwltool.

