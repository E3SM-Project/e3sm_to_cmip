#!/bin/bash

# Running on pm-cpu

#SBATCH  --job-name=chemdyg_summary_table_native_0011-0100
#SBATCH  --account=e3sm
#SBATCH  --nodes=1
#SBATCH  --output=/global/cfs/cdirs/e3sm/xie7/ccmi_output/proc_med/output/v3.ccmi.PD_INT_custom30/o3ste/post/scripts/chemdyg_summary_table_native_0011-0100.o%j
#SBATCH  --exclusive
#SBATCH  --time=10:00:00
#SBATCH  --qos=regular
#SBATCH  --constraint=cpu

source /global/common/software/e3sm/anaconda_envs/load_latest_e3sm_unified_pm-cpu.sh

# To load custom E3SM Diags environment, comment out line above using 
# and uncomment lines below

#module load anaconda3/2019.03
#source /share/apps/anaconda3/2019.03/etc/profile.d/conda.sh
#conda activate e3sm_diags_env_dev

# Turn on debug output if needed
debug=False
if [[ "${debug,,}" == "true" ]]; then
  set -x
fi

# Make sure UVCDAT doesn't prompt us about anonymous logging
export UVCDAT_ANONYMOUS_LOG=False

# Script dir
cd /global/cfs/cdirs/e3sm/xie7/ccmi_output/proc_med/output/v3.ccmi.PD_INT_custom30/o3ste/post/scripts

# Get jobid
id=${SLURM_JOBID}

# Update status file
STARTTIME=$(date +%s)
echo "RUNNING ${id}" > chemdyg_summary_table_native_0011-0100.status

# Basic definitions
case="v3.ccmi.PD_INT_custom30"
short="v3.ccmi.PD_INT_custom30"
www="/global/cfs/cdirs/e3sm/xie7/ccmi_output/proc_med/output/v3.ccmi.PD_INT_custom30/o3ste/output"
y1=11
y2=12
#100
run_type=""
tag=""

# Create temporary workdir
workdir=`mktemp -d tmp.${id}.XXXX`
cd ${workdir}

# Create local links to input climo files
#tsDir=/global/cfs/cdirs/e3sm/xie7/ccmi_output/proc_med/output/v3.ccmi.PD_INT_custom30/o3ste/post/atm/native/ts/monthly/90yr
mkdir -p ts
#cd ts
#ln -s ${tsDir}/*.nc .
#cd ..
# Create symbolic links to input files
input=/global/cfs/cdirs/e3sm/xie7/ccmi_orig/v3.ccmi.PD_INT_custom30/archive/atm/hist//
eamfile=eam.h5
for (( year=${y1}; year<=${y2}; year++ ))
do
  YYYY=`printf "%04d" ${year}`
  for file in ${input}/${case}.eam.h0.${YYYY}-*.nc
  do
    ln -s ${file} ./ts
  done
  for file in ${input}/${case}.${eamfile}.${YYYY}-*.nc
  do
    ln -s ${file} ./ts
  done
done

#

#cd ..

# Run E3SM chem Diags
echo
echo ===== RUN E3SM CHEM DIAGS  =====
echo

# Prepare configuration file
cat > chem_summary_table.py << EOF
#!/usr/bin/env python
# coding: utf-8

from netCDF4 import Dataset
import matplotlib.pyplot as plt
import numpy as np
import xarray as xr
from calendar import monthrange
import pandas as pd

path = './ts/'
pathout = './'

short_name = '${short}'
startyear = '${y1}'
endyear = '${y2}'

filename = short_name+'.eam.h0.*.nc'
filenameh1 = short_name+'.${eamfile}.*.nc'

varname = ["O3"]
##,"CO","CH4LNZ","NO"]
layer = ['']

h0_in = xr.open_mfdataset(path+filename)

variablelist = list(h0_in.keys())

timeperiod = len(h0_in['time'])
startdate = str(np.array(h0_in['time'].dt.year[0]))+'-01-01'

time_range_month = pd.date_range(startdate,  periods=timeperiod, freq='ME')
h0_in['time'] = time_range_month

rearth = 6.37122e6 # Earth radius: m
unit_covet = 1.e-9*12 # kg/month -> Tg/year

area_rad = h0_in['area'][0]         # radian (ncol)
area = area_rad * rearth * rearth  # m2
lat = h0_in['lat'][0]
NH = area.where(lat >= 0)
SH = area.where(lat < 0)

lev = h0_in['lev']
time = h0_in['time']

year = np.array(time.dt.year)
month = np.array(time.dt.month)

dt = np.zeros(timeperiod)
for i in range(len(time)):
    dt[i] = monthrange(2001,month[i])[1]*3600*24

dt_array = xr.DataArray(dt, coords=[h0_in['time']], dims=["time"])

STE_time = np.zeros(len(time))
STE_time_NH = np.zeros(len(time))
STE_time_SH = np.zeros(len(time))
for var in range(len(varname)):
    total_layer = len(layer)

    for ll in range(total_layer):

        if varname[var] == 'O3':
            TDD = h0_in[varname[var]+'_2DTDD'+layer[ll]]
            CIP = h0_in[varname[var]+'_2DCIP'+layer[ll]] #kg/m2/sec
            CIL = h0_in[varname[var]+'_2DCIL'+layer[ll]] #kg/m2/sec
            total_net = CIP-CIL
	    #
            #TDD_total = (dt*(TDD*area).sum(axis=1)).mean() #kg
            #NET       = (dt*((CIP-CIL)*area).sum(axis=1)).mean()
            # NH
            #TDD_NH = (dt*(TDD*NH).sum(axis=1)).mean() #kg
            #NET_NH = (dt*((CIP-CIL)*NH).sum(axis=1)).mean()
            # SH
            #TDD_SH = (dt*(TDD*SH).sum(axis=1)).mean() #kg
            #NET_SH = (dt*((CIP-CIL)*SH).sum(axis=1)).mean()
            # calculate STE
	    #time
            TDD_time=dt*(TDD*area).sum(axis=1)
            NET_time=dt*((CIP-CIL)*area).sum(axis=1)
            STE_time=TDD_time-NET_time
            #
            TDD_NH_time=dt*(TDD*NH).sum(axis=1)
            NET_NH_time=dt*((CIP-CIL)*NH).sum(axis=1)
            STE_NH_time=TDD_time-NET_NH_time
            #
            TDD_SH_time=dt*(TDD*SH).sum(axis=1)
            NET_SH_time=dt*((CIP-CIL)*SH).sum(axis=1)
            STE_SH_time=TDD_SH_time-NET_SH_time
	    # calculate STE
            STE_time_xr =    xr.DataArray(STE_time, coords=[h0_in['time']], dims=["month"])
            STE_time_NH_xr = xr.DataArray(STE_NH_time, coords=[h0_in['time']], dims=["time"])
            STE_time_SH_xr = xr.DataArray(STE_SH_time, coords=[h0_in['time']], dims=["time"])
            ds1 = STE_time_xr.to_dataset(name='STE')
            ds2 = STE_time_NH_xr.to_dataset(name='STE_NH')
            ds3 = STE_time_SH_xr.to_dataset(name='STE_SH')
            ds = xr.merge([ds1, ds2, ds3])
            ds.to_netcdf(pathout+'E3SM_O3_STE_${y1}-${y2}.nc')
            quit()
EOF

# Run diagnostics
command="python -u chem_summary_table.py"
time ${command}
if [ $? != 0 ]; then
  cd ..
  echo 'ERROR (1)' > chemdyg_summary_table_native_0011-0100.status
  exit 1
fi

# Copy output to web server
echo
echo ===== COPY FILES TO WEB SERVER =====
echo

# Create top-level directory
tmp_Y1=`printf "%04d" ${y1}`
tmp_Y2=`printf "%04d" ${y2}`
f=${www}/${case}/e3sm_chem_diags_${tmp_Y1}_${tmp_Y2}/plots/
mkdir -p ${f}
if [ $? != 0 ]; then
  cd ..
  echo 'ERROR (2)' > chemdyg_summary_table_native_0011-0100.status
  exit 1
fi

# Copy files
mv *.html ${f}
mv *.nc   ${f}


# Change file permissions
chmod -R go+rX,go-w ${f}

if [ $? != 0 ]; then
  cd ..
  echo 'ERROR (3)' > chemdyg_summary_table_native_0011-0100.status
  exit 1
fi
cd ..
if [[ "${debug,,}" != "true" ]]; then
  rm -rf ${workdir}
fi

# Update status file and exit

ENDTIME=$(date +%s)
ELAPSEDTIME=$(($ENDTIME - $STARTTIME))

echo ==============================================
echo "Elapsed time: $ELAPSEDTIME seconds"
echo ==============================================
echo 'OK' > chemdyg_summary_table_native_0011-0100.status
exit 0
