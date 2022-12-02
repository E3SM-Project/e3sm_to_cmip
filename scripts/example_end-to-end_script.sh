#!/bin/bash

#source /usr/local/e3sm_unified/envs/load_latest_e3sm_unified_acme1.sh
#conda activate e2c_nco511_13

#historical
expshort=historical
exp=historical
#expshort=hist-nat
#exp=hist-all-xGHG-xaer
#en1
caseid=v2.LR.${exp}_0101
ens=r1i1p1f1
en=ens1

##en2
#caseid=v2.LR.${exp}_0151
#ens=r2i1p1f1
#en=ens2
###
###
##en3
#caseid=v2.LR.${exp}_0201
#ens=r3i1p1f1
#en=ens3
###
##en4
#caseid=v2.LR.${exp}_0251
#ens=r4i1p1f1
#en=ens4
###
##en5
#caseid=v2.LR.${exp}_0301
#ens=r5i1p1f1
#en=ens5
start=1850
end=1850
ypf=1


##piControl
#caseid=v2.LR.piControl
#exp=piControl
#ens=r1i1p1f1
#en=ens1
#start=1
#end=500
#ypf=500

#input_path=/p/user_pub/e3sm/warehouse/E3SM/2_0/$exp/LR/atmos/native/model-output/mon/$en/v0 
input_path=/p/user_pub/e3sm/zhang40/e2c_tony/e2c_test_data/v2.eam_input
#result_dir=/p/user_pub/e3sm/zhang40/cmorized_for_celine
result_dir=/p/user_pub/e3sm/zhang40/cmorized_for_testing
rgr_dir=${result_dir}/rgr
rgr_dir_vert=${result_dir}/rgr_vert
rgr_dir_vert_plev=${result_dir}/rgr_vert_plev
native_dir=${result_dir}/native

exp=$expshort

map_file=/home/zender1/data/maps/map_ne30pg2_to_cmip6_180x360_aave.20200201.nc
tables_path=/home/zhang40/cmip6-cmor-tables/Tables/
#metadata_path=/p/user_pub/e3sm/staging/resource/CMIP6-Metadata/E3SM-1-0/${exp}_${ens}.json
metadata_path=/home/zhang40/CMIP6-Metadata/E3SM-2-0_to_1/${exp}_${ens}.json
metadata_path=/home/zhang40/CMIP6-Metadata/E3SM-2-0/${exp}_${ens}.json

# space is not accepted
# atm monthly h0 
flags='-7 --dfl_lvl=1 --no_cll_msr'
raw_var_list="ICEFRAC,OCNFRAC,LANDFRAC,PHIS,hyam,hybm,hyai,hybi,TREFHT,TS,PSL,PS,U10,QREFHT,PRECC,PRECL,PRECSC,PRECSL,QFLX,TAUX,TAUY,LHFLX,CLDTOT,FLDS,FLNS,FSDS,FSNS,SHFLX,CLOUD,CLDICE,TGCLDIWP,CLDLIQ,TGCLDCWP,TMQ,FLNSC,FSNTOA,FSNT,FLNT,FLUTC,FSDSC,SOLIN,FSNSC,FSUTOA,FSUTOAC,AODABS,AODVIS,AREL,TREFMNAV,TREFMXAV,FISCCP1_COSP,CLDTOT_ISCCP,MEANCLDALB_ISCCP,MEANPTOP_ISCCP,CLD_CAL,CLDTOT_CAL,CLDLOW_CAL,CLDMED_CAL,CLDHGH_CAL"
# atm 2D variables
cmip_var_list="sftlf,orog,full, phalf, tas, ts, psl, ps, sfcWind, huss, pr, prc, prsn, evspsbl, tauu, tauv, hfls, clt, rlds, rlus, rsds, rsus, hfss, cl, clw, cli, clivi, clwvi, prw, rldscs, rlut, rlutcs, rsdt, rsuscs, rsut, rsutcs, rtmt, abs550aer, od550aer, rsdscs, tasmax, tasmin, clisccp, cltisccp, albisccp, pctisccp, clcalipso, cltcalipso, cllcalipso, clmcalipso, clhcalipso, hur, hus, ta, ua, va, wap, zg, o3"

#ncclimo -P eam -j 1 --map=${map_file} --start=$start --end=$end --ypf=$ypf --split -c $caseid -o ${native_dir}/${exp}_${ens} -O ${rgr_dir}/${exp}_${ens} -v ${raw_var_list} -i ${input_path} ${flags}
#
#area areacella
# atm 3D variables
flags='-7 --dfl_lvl=1 --no_cll_msr'
raw_var_list="Q,O3,T,U,V,Z3,RELHUM,OMEGA"
#cmip_var_list="hur, hus, ta, ua, va, wap, zg, o3"
#ncclimo -P eam -j 1 --map=${map_file} --start=$start --end=$end --ypf=$ypf --split -c $caseid -o ${native_dir}/${exp}_${ens} -O ${rgr_dir_vert}/${exp}_${ens} -v ${raw_var_list} -i ${input_path} ${flags}
# --start=$start --end=$end won't work with ncremap
ncremap -P eam -j 1 --map=${map_file} --xtr_mth=mss_val --vrt_fl=/p/user_pub/e3sm/staging/resource/grids/vrt_remap_plev19.nc  -O ${rgr_dir}/${exp}_${ens} -v ${raw_var_list} -I ${input_path} ${flags}


##echo ${rgr_dir_vert}/${exp}_${ens}
#for file in `ls ${rgr_dir_vert}/${exp}_${ens}`
#do
#  ncks --rgr xtr_mth=mss_val --vrt_fl=/p/user_pub/e3sm/staging/resource/grids/vrt_remap_plev19.nc ${rgr_dir_vert}/${exp}_${ens}/$file ${rgr_dir}/${exp}_${ens}/$file
#done
##

# CMORIZE Atmosphere monthly variables
e3sm_to_cmip -i ${rgr_dir}/${exp}_${ens} -o $result_dir  -v ${cmip_var_list} -t /home/zhang40/cmip6-cmor-tables/Tables/ -u ${metadata_path}

#
# atm high freq daily h1+
input_path=/p/user_pub/e3sm/zhang40/e2c_tony/e2c_test_data/v2.eam.h1_input
flags='-7 --dfl_lvl=1 --no_cll_msr --clm_md=hfs'
raw_var_list="TREFHTMN,TREFHTMX,PRECT,TREFHT,FLUT,QREFHT"
cmip_var_list="tasmin, tasmax, tas, huss, rlut, pr"
rgr_dir=${result_dir}/rgr_day
native_dir=${result_dir}/native_day
#ncclimo -P eam -j 1 --map=${map_file} --start=$start --end=$end --ypf=$ypf --split -c $caseid -o ${native_dir}/${exp}_${ens} -O ${rgr_dir}/${exp}_${ens} -v ${raw_var_list} -i ${input_path} ${flags}

# CMORIZE Atmosphere daily variables
e3sm_to_cmip --freq day -i ${rgr_dir}/${exp}_${ens} -o $result_dir  -v ${cmip_var_list} -t /home/zhang40/cmip6-cmor-tables/Tables/ -u ${metadata_path}
#
# atm high freq 3hrly h1+
input_path=/p/user_pub/e3sm/zhang40/e2c_tony/e2c_test_data/v2.eam.h4_input
flags='-7 --dfl_lvl=1 --no_cll_msr --clm_md=hfs'
raw_var_list="PRECT"
rgr_dir=${result_dir}/rgr_3hr
native_dir=${result_dir}/native_3hr
cmip_var_list="pr"
#ncclimo -P eam -j 1 --map=${map_file} --start=$start --end=$end --ypf=$ypf --split -c $caseid -o ${native_dir}/${exp}_${ens} -O ${rgr_dir}/${exp}_${ens} -v ${raw_var_list} -i ${input_path} ${flags}

# CMORIZE Atmosphere 3hrly variables
e3sm_to_cmip --freq 3hr -i ${rgr_dir}/${exp}_${ens} -o $result_dir  -v ${cmip_var_list} -t /home/zhang40/cmip6-cmor-tables/Tables/ -u ${metadata_path}

## land monthly h0
input_path=/p/user_pub/e3sm/zhang40/e2c_tony/e2c_test_data/v2.elm_input/
flags='-7 --dfl_lvl=1 --no_cll_msr'
raw_var_list="LAISHA,LAISUN,QINTR,QOVER,QRUNOFF,QSOIL,QVEGE,QVEGT,SOILICE,SOILLIQ,SOILWATER_10CM,TSA,TSOI,H2OSNO"
cmip_var_list="mrsos, mrso, mrfso, mrros, mrro, prveg, evspsblveg, evspsblsoi, tran, tsl, lai"
rgr_dir=${result_dir}/rgr_lnd
native_dir=${result_dir}/native_lnd
# Note either include the extra variable landfrac or specify the file that has landfrac for subgrid scale mode to work.
ncclimo -P elm -j 1 --var_xtr=landfrac --map=${map_file} --start=$start --end=$end --ypf=$ypf --split -c $caseid -o ${native_dir}/${exp}_${ens} -O ${rgr_dir}/${exp}_${ens} -v ${raw_var_list} -i ${input_path} ${flags}
# Alternative ncclimo invocation
#ncclimo -P elm -j 1 --map=${map_file} --start=$start --end=$end --ypf=$ypf --split -c $caseid -o ${native_dir}/${exp}_${ens} -O ${rgr_dir}/${exp}_${ens} -v ${raw_var_list} -i ${input_path} ${flags} --sgs_frc=${input_path}/v2.LR.historical_0101.elm.h0.1850-01.nc/landfrac

#e3sm_to_cmip --realm lnd -i ${rgr_dir}/${exp}_${ens} -o $result_dir  -v ${cmip_var_list} -t /home/zhang40/cmip6-cmor-tables/Tables/ -u ${metadata_path}
raw_var_list_elm_bgc="TOTLITC,CWDC,TOTPRODC,SOIL1C,SOIL2C,SOIL3C,^SOIL4C$,COL_FIRE_CLOSS,WOOD_HARVESTC,TOTVEGC,NBP,GPP,AR,HR"


# CMORIZE Land Monthly variables 
e3sm_to_cmip -i ${rgr_dir}/${exp}_${ens} -o $result_dir  -v ${cmip_var_list} -t /home/zhang40/cmip6-cmor-tables/Tables/ -u ${metadata_path}

## CMORIZE Sea-ice Monthly variables 
#e3sm_to_cmip -s --realm SImon --var-list siconc, sitemptop, sisnmass, sitimefrac, siu, siv, sithick, sisnthick, simass --map /p/user_pub/e3sm/staging/resource/maps/map_EC30to60E2r2_to_cmip6_180x360_aave.20220301.nc --input-path /p/user_pub/e3sm/zhang40/e2c_tony/e2c_test_data/v2.mpassi_input/ --output-path ${result_dir} --user-metadata /p/user_pub/e3sm/zhang40/e2c_tony/e2c_test_data/holodeck/input/historical_r1i1p1f1.json --tables-path /p/user_pub/e3sm/staging/resource/cmor/cmip6-cmor-tables/Tables
#
#
## CMORIZE Ocean Monthly variables 
#e3sm_to_cmip -s --realm Omon --var-list areacello, fsitherm, hfds, masso, mlotst, sfdsi, sob, soga, sos, sosga, tauuo, tauvo, thetaoga, tob, tos, tosga, volo, wfo, zos, thetaoga, hfsifrazil, masscello, so, thetao, thkcello, uo, vo, volcello, wo zhalfo --map /p/user_pub/e3sm/staging/resource/maps/map_EC30to60E2r2_to_cmip6_180x360_aave.20220301.nc --input-path /p/user_pub/e3sm/zhang40/e2c_tony/e2c_test_data/v2.mpaso_input/ --output-path ${result_dir} --user-metadata /p/user_pub/e3sm/zhang40/e2c_tony/e2c_test_data/holodeck/input/historical_r1i1p1f1.json --tables-path /p/user_pub/e3sm/staging/resource/cmor/cmip6-cmor-tables/Tables

exit

