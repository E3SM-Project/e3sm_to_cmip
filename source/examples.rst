.. _examples:

********
Examples
********



Simple atmosphere variable example (no CWL)
===========================================

The first step in converting atmosphere variables by hand is to do the regridding and time-series extraction. For this example, lets assume there's
a directory named "atmos-input" that contains a single year of cam.h0 monthly history files, and we want to end up with the "pr," and "clt" variables.

First we ask the e3sm_to_cmip package what source variables are needed for these two output variables, or look them up in the :ref:`var_map_atm`

.. code-block:: bash

    >> e3sm_to_cmip --info -v pr, clt
    [*]
    CMIP6 Name: clt,
    CMIP6 Table: CMIP6_Amon.json,
    CMIP6 Units: %,
    E3SM Variables: CLDTOT,
    Unit conversion: 1-to-%
    [*]
    CMIP6 Name: pr,
    CMIP6 Table: CMIP6_Amon.json,
    CMIP6 Units: kg m-2 s-1,
    E3SM Variables: PRECC, PRECL

From this we can see that the clt variable needs the CLDTOT input variable, and pr needs both PRECC and PRECL.

Next we use ncclimo to extract the time-series and do the regridding. `A detailed tutorial can be found here <https://www.youtube.com/watch?v=AJyAjH-1HuA>`_
Variables specific to your case will be denoted <LIKE THIS>

.. code-block:: bash

    >> ncclimo --start=0001 --end=0001 --ypf=1 -c <CASENAME> -o ./native -O ./regrid -v CLDTOT,PRECC,PRECL -i ./atmos-input --map=<PATH TO YOUR MAPFILE>
    Climatology operations invoked with command:
    /export/baldwin32/anaconda3/envs/cwl/bin/ncclimo --start=2015 --end=2015 --ypf=1 -c 20191204.BDRD_CNPCTC_SSP585_OIBGC.ne30_oECv3.compy -o ./native --regrid=./regrid -v CLDTOT,PRECC,PRECL -i ./atm_testing -v CLDTOT,PRECC,PRECL --map=/export/zender1/data/maps/map_ne30np4_to_cmip6_180x360_aave.20181001.nc
    Started climatology generation for dataset 20191204.BDRD_CNPCTC_SSP585_OIBGC.ne30_oECv3.compy at Tue Nov 24 15:05:20 PST 2020
    Running climatology script ncclimo from directory /export/baldwin32/anaconda3/envs/cwl/bin
    NCO binaries version 4.9.3 from directory /export/baldwin32/anaconda3/envs/cwl/bin
    Parallelism mode = background
    Timeseries will be created for each of 3 variables
    Background parallelism processing variables in var_nbr/job_nbr = 3/3 = 1 sequential batches of job_nbr = 3 simultaneous jobs (1 per variable), then remaining 0 jobs/variables simultaneously
    Will split data for each variable into one timeseries of length 1 years
    Splitting climatology from 12 raw input files in directory ./atm_testing
    Each input file assumed to contain mean of one month
    Native-grid split files to directory ./native
    Regridded split files to directory ./regrid
    Tue Nov 24 15:05:21 PST 2020: Generated ./native/CLDTOT_201501_201512.nc
    Tue Nov 24 15:05:21 PST 2020: Generated ./native/PRECC_201501_201512.nc
    Tue Nov 24 15:05:21 PST 2020: Generated ./native/PRECL_201501_201512.nc
    Tue Nov 24 15:05:22 PST 2020: Regridded ./regrid/CLDTOT_201501_201512.nc
    Tue Nov 24 15:05:22 PST 2020: Regridded ./regrid/PRECC_201501_201512.nc
    Tue Nov 24 15:05:22 PST 2020: Regridded ./regrid/PRECL_201501_201512.nc
    Quick plots of last variable split in last segment:
    ncview ./regrid/PRECL_201501_201512.nc &
    panoply ./regrid/PRECL_201501_201512.nc &
    Completed 1-year climatology operations for dataset with caseid = 20191204.BDRD_CNPCTC_SSP585_OIBGC.ne30_oECv3.compy at Tue Nov 24 15:05:22 PST 2020
    Elapsed time 0m2s

Now that we have appropriate input files, we can run the converter package. 

.. code-block:: bash

    >> e3sm_to_cmip -i regrid/ -o cmip_output -v prc, clt -t <PATH TO CMIP6 TABLES> -u <PATH TO CMOR USER INPUT JSON>
    [*] Writing log output to: cmip_output/converter.log
    [+] Running CMOR handlers in parallel
    100%|███████████████████████████████████████| 2/2 [00:01<00:00,  1.96it/s]
    [+] 2 of 2 handlers complete

Alternately, if this isnt going to be published to CMIP6, we can use the "simple" mode which doesnt require the full CMIP6 tables or metadata

.. code-block:: bash

    >> python -m e3sm_to_cmip -i regrid -o cmip_output -v prc, clt --simple
    [*] Writing log output to: cmip_output/converter.log
    [+] Running CMOR handlers in parallel       
    [+] writing out variable to file /cmip_output/prc_CMIP6_Amon_201501-201512.nc                                                                                                                                                                                                                                              | 0/2 [00:00<?, ?it/s][+] writing out variable to file /p/user_pub/e3sm/baldwin32/workshop/ssp585/ssp585/output/pp/cmor/ssp585/2015_2100/cmip_output/prc_CMIP6_Amon_201501-201512.nc
    [+] writing out variable to file /cmip_output/clt_CMIP6_Amon_201501-201512.nc
    100%|███████████████████████████████████████| 2/2 [00:00<00:00,  6.79it/s]
    [+] 2 of 2 handlers complete



Plev atmosphere variable example (no CWL)
=========================================


 

