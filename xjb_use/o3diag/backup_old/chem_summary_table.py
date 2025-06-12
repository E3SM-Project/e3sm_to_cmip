#!/usr/bin/env python
# coding: utf-8

from netCDF4 import Dataset
import matplotlib.pyplot as plt
import numpy as np
import xarray as xr
from calendar import monthrange
import pandas as pd

#path = './ts/'
path = './'
pathout = './'

short_name = 'v3.ccmi.PD_INT_custom30'
startyear = '11'
endyear = '100'

filename = short_name+'.eam.h0.*.nc'
filenameh1 = short_name+'.eam.h5.*.nc'

#varname = ["O3","CO","CH4LNZ","NO"]
varname = ["O3"]
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
#
dt = np.zeros(timeperiod)
for i in range(len(time)):
    dt[i] = monthrange(2001,month[i])[1]*3600*24

dt_array = xr.DataArray(dt, coords=[h0_in['time']], dims=["time"])
#
STE_time = np.zeros(len(time))
STE_time_NH = np.zeros(len(time))
STE_time_SH = np.zeros(len(time))
#
for var in range(len(varname)):
    total_layer = len(layer)
    print(total_layer)

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
                #print(STE_time)
                STE_time[i] = STE
                STE_time_NH[i] = STE_NH
                STE_time_SH[i] = STE_SH

                MSD_old = MSDt

            #STE_mean = STE_time[1::].mean()
            #STE_mean_NH = STE_time_NH[1::].mean()
            #STE_mean_SH = STE_time_SH[1::].mean()
            STE_time_xr =    xr.DataArray(STE_time, coords=[h0_in['time']], dims=["month"])
            STE_time_NH_xr = xr.DataArray(STE_time_NH, coords=[h0_in['time']], dims=["time"])
            STE_time_SH_xr = xr.DataArray(STE_time_SH, coords=[h0_in['time']], dims=["time"])
            ds1 = STE_time_xr.to_dataset(name='STE')
            ds2 = STE_time_NH_xr.to_dataset(name='STE_NH')
            ds3 = STE_time_SH_xr.to_dataset(name='STE_SH')
            ds = xr.merge([ds1, ds2, ds3])
            #, ds4, ds5, ds6, ds7, ds8, ds9])
            #ds.to_netcdf(pathout+'E3SM_O3_STE_${y1}-${y2}.nc')
            ds.to_netcdf(pathout+'E3SM_O3_STE_1-2.nc')


