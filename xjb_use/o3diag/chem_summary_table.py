#!/usr/bin/env python
# coding: utf-8

from netCDF4 import Dataset
import matplotlib.pyplot as plt
import numpy as np
import xarray as xr
from calendar import monthrange
import pandas as pd
#
import os as os
#
from datetime import datetime

path = './ts/'
#path = './'
pathout = './'

short_name = 'v3.ccmi.PD_INT_custom30'
startyear = '11'
endyear = '100'
filename = short_name+'.eam.h0.*.nc'
#
h0_files = sorted([f for f in os.listdir(path) if 'h0' in f])
files_count=len(h0_files)
#
varname = ["O3"]
layer = ['']
#
timeperiod = len(h0_files)#h0_in['time'])
dt = np.zeros(timeperiod)
STE_time = np.zeros(len(h0_files))
STE_time_NH = np.zeros(len(h0_files))
STE_time_SH = np.zeros(len(h0_files))
#output to data
STE_time_add=np.zeros(len(h0_files))
STE_time_NH_add=np.zeros(len(h0_files))
STE_time_SH_add=np.zeros(len(h0_files))
#
counts=0
#
for file in h0_files:
    counts=counts+1
    print(path+file)
    print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    h0_in = xr.open_mfdataset(path+file)
    print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    variablelist = list(h0_in.keys())
    #h0_in['time'] = time_range_month
    #
    rearth = 6.37122e6 # Earth radius: m
    unit_covet = 1.e-9*12 # kg/month -> Tg/year
    #
    area_rad = h0_in['area'][0]         # radian (ncol)
    area = area_rad * rearth * rearth  # m2
    lat = h0_in['lat'][0]
    NH = area.where(lat >= 0)
    SH = area.where(lat < 0)
    #
    time = h0_in['time']
    year = np.array(time.dt.year)
    month = np.array(time.dt.month)-1
    #
    dt=monthrange(2001,month[0])[1]*3600*24
    #
    for var in range(len(varname)):
        total_layer = len(layer)

        for ll in range(total_layer):
            TDD = h0_in[varname[var]+'_2DTDD'+layer[ll]]
            CIP = h0_in[varname[var]+'_2DCIP'+layer[ll]] #kg/m2/sec
            CIL = h0_in[varname[var]+'_2DCIL'+layer[ll]] #kg/m2/sec
            #time
            TDD_time=dt*(TDD*area).sum(axis=1)
            NET_time=dt*((CIP-CIL)*area).sum(axis=1)
            STE_time=TDD_time-NET_time
            #
            TDD_time_NH=dt*(TDD*NH).sum(axis=1)
            NET_time_NH=dt*((CIP-CIL)*NH).sum(axis=1)
            STE_time_NH=TDD_time-NET_time_NH
            #
            TDD_time_SH=dt*(TDD*SH).sum(axis=1)
            NET_time_SH=dt*((CIP-CIL)*SH).sum(axis=1)
            STE_time_SH=TDD_time_SH-NET_time_SH
            #
            STE_time_add[counts-1]=STE_time
            STE_time_NH_add[counts-1]=STE_time_NH
            STE_time_SH_add[counts-1]=STE_time_SH

print(STE_time_add)
# calculate STE
STE_time_xr =    xr.DataArray(STE_time_add,    coords=[np.arange(0,counts)], dims=["time"])
STE_time_NH_xr = xr.DataArray(STE_time_NH_add, coords=[np.arange(0,counts)], dims=["time"])
STE_time_SH_xr = xr.DataArray(STE_time_SH_add, coords=[np.arange(0,counts)], dims=["time"])
ds1 = STE_time_xr.to_dataset(name='STE')
ds2 = STE_time_NH_xr.to_dataset(name='STE_NH')
ds3 = STE_time_SH_xr.to_dataset(name='STE_SH')
ds = xr.merge([ds1, ds2, ds3])
#ds.to_netcdf(pathout+'E3SM_O3_STE_${y1}-${y2}.nc')
ds.to_netcdf(pathout+'E3SM_O3_STE_1-2.nc')
quit()
            

