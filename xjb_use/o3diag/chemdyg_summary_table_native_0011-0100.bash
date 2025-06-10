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
y2=100
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
h1_in = xr.open_mfdataset(path+filenameh1)

variablelist = list(h0_in.keys())

timeperiod = len(h0_in['time'])
startdate = str(np.array(h0_in['time'].dt.year[0]))+'-01-01'

time_range_month = pd.date_range(startdate,  periods=timeperiod, freq='ME')
h0_in['time'] = time_range_month
h1_in['time'] = time_range_month

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
linehead = '<h> E3SM chemistry high-level summary</h>'
linehead = linehead + '<pre>The * sign indicates that the calculation involves the stratosphere, while calculations without the sign primarily pertain to the troposhere. </pre>'
linehead = linehead + '<pre>'+short_name+'</pre>'
linehead = linehead + '<pre>Simulation period: '+ startyear +' - '+ endyear + '</pre>'
line_ann = linehead + '<p> Season: ANN </p>'

line_ann = line_ann + '<pre> Chemistry                                   Global        N. Hemisphere   S. Hemisphere </pre>'

fileout_ann = open(pathout+'chem_summary_table.html',"w")

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
            MSD = h1_in[varname[var]+'_2DMSD'] #kg/m2

            SCO = h0_in['SCO'] *2.1415e-14
            TCO = h0_in['TCO'] *2.1415e-14 #DU to Tg
            TDD = h0_in[varname[var]+'_2DTDD'+layer[ll]]
            CIP = h0_in[varname[var]+'_2DCIP'+layer[ll]] #kg/m2/sec
            CIL = h0_in[varname[var]+'_2DCIL'+layer[ll]] #kg/m2/sec
            total_net = CIP-CIL
            TOZ = SCO+TCO

            MSD_total = ((MSD*area).sum(axis=1)).mean() #kg
            TDD_total = (dt*(TDD*area).sum(axis=1)).mean() #kg
            CIP_total = (dt*(CIP*area).sum(axis=1)).mean()
            CIL_total = (dt*(-CIL*area).sum(axis=1)).mean()
            NET       = (dt*((CIP-CIL)*area).sum(axis=1)).mean()
            SCO_total = (SCO*area).sum(axis=1).mean() #kg
            TCO_total = (TCO*area).sum(axis=1).mean() #kg
            TOZ_total = (TOZ*area).sum(axis=1).mean() #kg
            # NH
            MSD_NH = ((MSD*NH).sum(axis=1)).mean() #kg
            TDD_NH = (dt*(TDD*NH).sum(axis=1)).mean() #kg
            CIP_NH = (dt*(CIP*NH).sum(axis=1)).mean()
            CIL_NH = (dt*(-CIL*NH).sum(axis=1)).mean()
            NET_NH = (dt*((CIP-CIL)*NH).sum(axis=1)).mean()
            SCO_NH = (SCO*NH).sum(axis=1).mean() #kg
            TCO_NH = (TCO*NH).sum(axis=1).mean() #kg
            TOZ_NH = (TOZ*NH).sum(axis=1).mean() #kg
            # SH
            MSD_SH = ((MSD*SH).sum(axis=1)).mean() #kg
            TDD_SH = (dt*(TDD*SH).sum(axis=1)).mean() #kg
            CIP_SH = (dt*(CIP*SH).sum(axis=1)).mean()
            CIL_SH = (dt*(-CIL*SH).sum(axis=1)).mean()
            NET_SH = (dt*((CIP-CIL)*SH).sum(axis=1)).mean()
            SCO_SH = (SCO*SH).sum(axis=1).mean() #kg
            TCO_SH = (TCO*SH).sum(axis=1).mean() #kg
            TOZ_SH = (TOZ*SH).sum(axis=1).mean() #kg
            # calculate STE
            for i in range(len(time)):
                MSDt = h1_in[varname[var]+'_2DMSD_trop'][i,:] #kg/m2

                TDBt = h0_in[varname[var]+'_2DTDB_trop'][i,:]
                TDDt = h0_in[varname[var]+'_2DTDD_trop'][i,:]
                TDEt = h0_in[varname[var]+'_2DTDE_trop'][i,:]
                TDIt = h0_in[varname[var]+'_2DTDI_trop'][i,:]
                TDAt = h0_in[varname[var]+'_2DTDA_trop'][i,:]
                TDLt = h0_in[varname[var]+'_2DTDL_trop'][i,:]
                TDNt = h0_in[varname[var]+'_2DTDN_trop'][i,:]
                TDOt = h0_in[varname[var]+'_2DTDO_trop'][i,:]
                TDSt = h0_in[varname[var]+'_2DTDS_trop'][i,:]
                TDUt = h0_in[varname[var]+'_2DTDU_trop'][i,:]

                total_td = (TDOt+TDEt+TDIt+TDAt+TDLt+TDNt+TDUt+TDBt+TDSt+TDDt)

                MSD_total = (MSDt*area).sum()
                td_temp = total_td*dt[i]
                TTD_total = (td_temp*area).sum()

                if i == 0:
                    STE = 'nan'
                    STE_NH = 'nan'
                    STE_SH = 'nan'
                else:
                    temp = MSD_old+td_temp
                    STE = ((MSDt-temp)*area).sum()
                    STE_NH = ((MSDt-temp)*NH).sum()
                    STE_SH = ((MSDt-temp)*SH).sum()

                STE_time[i] = STE
                STE_time_NH[i] = STE_NH
                STE_time_SH[i] = STE_SH

                MSD_old = MSDt

	STE_time_xr = xr.DataArray(STE_time, coords=[np.arange(1,13)], dims=["month"])
        STE_time_NH_xr = xr.DataArray(STE_time_NH, coords=[np.arange(1,13)], dims=["month"])
        STE_time_SH_xr = xr.DataArray(STE_time_SH, coords=[np.arange(1,13)], dims=["month"])
        ds1 = STE_time_xr.to_dataset(name='STE')
        ds2 = STE_time_NH_xr.to_dataset(name='STE_NH')
        ds3 = STE_time_SH_xr.to_dataset(name='STE_SH')
        ds = xr.merge([ds1, ds2, ds3])
        ds.to_netcdf(pathout+'E3SM_O3_STE_$case\_${y1}-${y2}.nc')
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
