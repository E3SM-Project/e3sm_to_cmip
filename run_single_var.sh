#!/bin/bash
#SBATCH -N 1
#SBATCH -n 20
#SBATCH -o singlevar.out

time python singlevar_ts.py -c 20180129.DECKv1b_piControl.ne30_oEC.edison -i /p/user_pub/work/E3SM/1_0/piControl/1deg_atm_60-30km_ocean/atmos/129x256/model-output/mon/ens1/v1/ -o /p/user_pub/e3sm/baldwin32/cmip/piControl/ts/ -s 1 -e 500 -N