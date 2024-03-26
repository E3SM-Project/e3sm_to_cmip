#!/bin/bash

# Conda Environment
# -----------------
# Either comment out the lines below to use the E3SM Unified environment
# or use a local environment.
#source /usr/local/e3sm_unified/envs/load_latest_e3sm_unified_acme1.sh
#conda activate e2c_nco511_13

# -----------------
# SETUP
# -----------------
#historical
exp=historical
#expshort=hist-nat
#exp=hist-all-xGHG-xaer
caseid=v2.LR.${exp}_0101
start=1850
end=1850
ypf=1

# -----------------
# PATHS
# -----------------
e2c_path=/p/user_pub/e3sm/zhang40/e3sm_to_cmip_data/
model_data=$e2c_path/model-output

# TODO: Update result_dir
result_dir=${e2c_path}/reference

rgr_dir=${result_dir}/rgr
rgr_dir_vert=${result_dir}/rgr_vert
rgr_dir_vert_plev=${result_dir}/rgr_vert_plev
native_dir=${result_dir}/native

map_file=${e2c_path}/maps/map_ne30pg2_to_cmip6_180x360_aave.20200201.nc
tables_path=${e2c_path}/cmor/cmip6-cmor-tables/Tables/
metadata_path=${e2c_path}/user_metadata.json

# NOTE: Space is not accepted in nco var list
## ------------------------------------------------------
## TEST CASE - atm monthly h0
## ------------------------------------------------------
input_path=${model_data}/v2.eam_input
flags='-7 --dfl_lvl=1 --no_cll_msr'
raw_var_list="ICEFRAC,OCNFRAC,LANDFRAC,PHIS,hyam,hybm,hyai,hybi,TREFHT,TS,PSL,PS,U10,QREFHT,PRECC,PRECL,PRECSC,PRECSL,QFLX,TAUX,TAUY,LHFLX,CLDTOT,FLDS,FLNS,FSDS,FSNS,SHFLX,CLOUD,CLDICE,TGCLDIWP,CLDLIQ,TGCLDCWP,TMQ,FLNSC,FSNTOA,FSNT,FLNT,FLUTC,FSDSC,SOLIN,FSNSC,FSUTOA,FSUTOAC,AODABS,AODVIS,AREL,TREFMNAV,TREFMXAV,FISCCP1_COSP,CLDTOT_ISCCP,MEANCLDALB_ISCCP,MEANPTOP_ISCCP,CLD_CAL,CLDTOT_CAL,CLDLOW_CAL,CLDMED_CAL,CLDHGH_CAL"

# 1. atm 2D variables
#--------------------------
cmip_var_list="pfull, phalf, tas, ts, psl, ps, sfcWind, huss, pr, prc, prsn, evspsbl, tauu, tauv, hfls, clt, rlds, rlus, rsds, rsus, hfss, cl, clw, cli, clivi, clwvi, prw, rldscs, rlut, rlutcs, rsdt, rsuscs, rsut, rsutcs, rtmt, abs550aer, od550aer, rsdscs, tasmax, tasmin, clisccp, cltisccp, albisccp, pctisccp, clcalipso, cltcalipso, cllcalipso, clmcalipso, clhcalipso"
ncclimo -P eam -j 1 --map=${map_file} --start=$start --end=$end --ypf=$ypf --split -c $caseid -o ${native_dir} -O ${rgr_dir} -v ${raw_var_list} -i ${input_path} ${flags}
# CMORIZE Atmosphere monthly variables: 2D and model level 3D variables (CLOUD,CLDICE,CLDLIQ)
e3sm_to_cmip -i ${rgr_dir} -o $result_dir  -v ${cmip_var_list} -t $tables_path -u ${metadata_path}

# 2. atm fixed variables
#--------------------------
raw_var_list="area,PHIS,LANDFRAC"
cmip_var_list="areacella, sftlf, orog"
ncremap --map=${map_file} -v area,PHIS,LANDFRAC -I ${input_path} -O ${rgr_dir}/fixed_vars

# CMORIZE Atmosphere fx variables
e3sm_to_cmip --realm fx -i ${rgr_dir}/fixed_vars -o $result_dir  -v areacella, sftlf, orog  -t ${tables_path} -u ${metadata_path}

## ------------------------------------------------------
## TEST CASE - atm 3D variables
## ------------------------------------------------------
flags='-7 --dfl_lvl=1 --no_cll_msr'
raw_var_list="Q,O3,T,U,V,Z3,RELHUM,OMEGA"
cmip_var_list="hur, hus, ta, ua, va, wap, zg, o3"
ncclimo -P eam -j 1 --map=${map_file} --start=$start --end=$end --ypf=$ypf --split -c $caseid -o ${native_dir} -O ${rgr_dir_vert} -v ${raw_var_list} -i ${input_path} ${flags}
for file in `ls ${rgr_dir_vert}`
do
  ncks --rgr xtr_mth=mss_val --vrt_fl=${e2c_path}/grids/vrt_remap_plev19.nc ${rgr_dir_vert}/$file ${rgr_dir}/$file
done

# Note --start=$start --end=$end would not work with ncremap
#ncremap -P eam -j 1 --xtr_mth=mss_val --vrt_fl=${e2c_path}/grids/vrt_remap_plev19.nc  -O ${rgr_dir} -v ${raw_var_list} -I ${rgr_dir_vert} ${flags}

# CMORIZE Atmosphere monthly plev variables
e3sm_to_cmip -i ${rgr_dir} -o $result_dir  -v ${cmip_var_list} -t ${tables_path} -u ${metadata_path}

## ------------------------------------------------------
## TEST CASE - atm high freq daily h1+
## ------------------------------------------------------
input_path=${model_data}/v2.eam.h1_input
flags='-7 --dfl_lvl=1 --no_cll_msr --clm_md=hfs'
raw_var_list="TREFHTMN,TREFHTMX,PRECT,TREFHT,FLUT,QREFHT"
cmip_var_list="tasmin, tasmax, tas, huss, rlut, pr"
rgr_dir=${result_dir}/rgr_day
native_dir=${result_dir}/native_day
ncclimo -P eam -j 1 --map=${map_file} --start=$start --end=$end --ypf=$ypf --split -c $caseid -o ${native_dir} -O ${rgr_dir} -v ${raw_var_list} -i ${input_path} ${flags}

# CMORIZE Atmosphere daily variables
e3sm_to_cmip --freq day -i ${rgr_dir} -o $result_dir  -v ${cmip_var_list} -t ${tables_path} -u ${metadata_path}

## ------------------------------------------------------
## TEST CASE - atm high freq 3hrly h1+
## ------------------------------------------------------
input_path=${model_data}/v2.eam.h4_input
flags='-7 --dfl_lvl=1 --no_cll_msr --clm_md=hfs'
raw_var_list="PRECT"
rgr_dir=${result_dir}/rgr_3hr
native_dir=${result_dir}/native_3hr
cmip_var_list="pr"
ncclimo -P eam -j 1 --map=${map_file} --start=$start --end=$end --ypf=$ypf --split -c $caseid -o ${native_dir} -O ${rgr_dir} -v ${raw_var_list} -i ${input_path} ${flags}

# CMORIZE Atmosphere 3hrly variables
e3sm_to_cmip --freq 3hr -i ${rgr_dir} -o $result_dir  -v ${cmip_var_list} -t ${tables_path} -u ${metadata_path}

## ------------------------------------------------------
## TEST CASE - land monthly h0
## ------------------------------------------------------
input_path=${model_data}/v2.elm_input/
flags='-7 --dfl_lvl=1 --no_cll_msr'
raw_var_list="LAISHA,LAISUN,QINTR,QOVER,QRUNOFF,QSOIL,QVEGE,QVEGT,SOILICE,SOILLIQ,SOILWATER_10CM,TSA,TSOI,H2OSNO"
cmip_var_list="mrsos, mrso, mrfso, mrros, mrro, prveg, evspsblveg, evspsblsoi, tran, tsl, lai"
rgr_dir=${result_dir}/rgr_lnd
native_dir=${result_dir}/native_lnd

# Note either include the extra variable landfrac or specify the file that has landfrac for subgrid scale mode to work.
ncclimo -P elm -j 1 --var_xtr=landfrac --map=${map_file} --start=$start --end=$end --ypf=$ypf --split -c $caseid -o ${native_dir} -O ${rgr_dir} -v ${raw_var_list} -i ${input_path} ${flags}
# Alternative ncclimo invocation
#ncclimo -P elm -j 1 --map=${map_file} --start=$start --end=$end --ypf=$ypf --split -c $caseid -o ${native_dir} -O ${rgr_dir} -v ${raw_var_list} -i ${input_path} ${flags} --sgs_frc=${input_path}/v2.LR.historical_0101.elm.h0.1850-01.nc/landfrac

#raw_var_list_elm_bgc="TOTLITC,CWDC,TOTPRODC,SOIL1C,SOIL2C,SOIL3C,^SOIL4C$,COL_FIRE_CLOSS,WOOD_HARVESTC,TOTVEGC,NBP,GPP,AR,HR"
# CMORIZE Land Monthly variables
e3sm_to_cmip -i ${rgr_dir} -o $result_dir  -v ${cmip_var_list} -t ${tables_path} -u ${metadata_path}

# CMORIZE Sea-ice Monthly variables
# Note the input folder for mpas sea ice files requires:
# 1. Monthly mean history files: e.g.,v2.LR.historical_0101.mpassi.hist.am.timeSeriesStatsMonthly.1850-01-01.nc
# 2. A restart file for meshes: can use the mpaso restart e.g., v2.LR.historical_0101.mpaso.rst.1855-01-01_00000.nc
e3sm_to_cmip -s --realm SImon --var-list siconc, sitemptop, sisnmass, sitimefrac, siu, siv, sithick, sisnthick, simass --map ${e2c_path}/maps/map_EC30to60E2r2_to_cmip6_180x360_aave.20220301.nc --input-path ${model_data}/v2.mpassi_input/ --output-path ${result_dir} --user-metadata ${metadata_path}  --tables-path ${e2c_path}/cmor/cmip6-cmor-tables/Tables

# CMORIZE Ocean Monthly variables
# Note the input folder for mpas ocean files requires:
# 1. History files: e.g.,v2.LR.historical_0101.mpaso.hist.am.timeSeriesStatsMonthly.1850-01-01.nc
# 2. The namelist file for constants: mpaso_in
#    masso, masscello, msftmz, pbo, and pso require 'config_density0'
#    hfsifrazil requires 'config_density0' and 'config_frazil_heat_of_fusion'
# 3. A restart file for mesh: e.g., v2.LR.historical_0101.mpaso.rst.1855-01-01_00000.nc
# 4. A region masks file for MOC regions: EC30to60E2r2_mocBasinsAndTransects20210623.nc (Needed for variable msftmz: Ocean Meridional Overturning Mass Streamfunction)
e3sm_to_cmip -s --realm Omon --var-list areacello, fsitherm, hfds, masso, mlotst, sfdsi, sob, soga, sos, sosga, tauuo, tauvo, thetaoga, tob, tos, tosga, volo, wfo, zos, thetaoga, hfsifrazil, masscello, so, thetao, thkcello, uo, vo, volcello, wo, zhalfo --map ${e2c_path}/maps/map_EC30to60E2r2_to_cmip6_180x360_aave.20220301.nc --input-path ${model_data}/v2.mpaso_input/ --output-path ${result_dir} --user-metadata ${metadata_path} --tables-path ${e2c_path}/cmor/cmip6-cmor-tables/Tables

exit
