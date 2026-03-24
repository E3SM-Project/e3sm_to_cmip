#!/bin/bash

# Conda Environment
# -----------------
#conda activate e3sm_to_cmip_dev

# -----------------
# PATHS
# -----------------
e2c_path=/lcrc/group/e3sm/public_html/diagnostics/e3sm_to_cmip_data
model_data=/gpfs/fs0/globalscratch/ac.zhang40/e2c_tests/model_data
scratch=/gpfs/fs0/globalscratch/ac.zhang40

# TODO: Update result_dir
result_dir=${scratch}/e2c_tests

rgr_dir=${result_dir}/rgr
rgr_dir_vert=${result_dir}/rgr_vert
rgr_dir_vert_plev=${result_dir}/rgr_vert_plev
native_dir=${result_dir}/native

map_file=${e2c_path}/maps/map_r05_to_cmip6_180x360_traave.20231110.nc
tables_path=${e2c_path}/cmip6-cmor-tables/Tables/
metadata_path=${e2c_path}/default_metadata.json

start=1850
end=1850
ypf=1
caseid=v3.LR.historical_0101

## ------------------------------------------------------
## TEST CASE - land monthly h0
## ------------------------------------------------------
#input_path=${model_data}/
input_path=/lcrc/group/e3sm2/ac.wlin/E3SMv3/v3.LR.historical_0101/archive/lnd/hist
flags='-7 --dfl_lvl=1 --no_cll_msr'
#raw_var_list="LAISHA,LAISUN,QINTR,QOVER,QRUNOFF,QSOIL,QVEGE,QVEGT,SOILICE,SOILLIQ,SOILWATER_10CM,TSA,TSOI,H2OSNO"
#cmip_var_list="mrsos, mrso, mrfso, mrros, mrro, prveg, evspsblveg, evspsblsoi, tran, tsl, lai"
raw_var_list="LEAFC,FROOTC,LIVECROOTC,DEADCROOTC,CWDC,AR,MR,QVEGT,LEAFC_ALLOC,FROOTC_ALLOC,WOODC_ALLOC,FAREA_BURNED,LAND_USE_FLUX,FSNO,SNOWDP,SNOWLIQ,H2OSNO_TOP,QSNOMELT,FGR,SNOBCMSL,SNODSTMSL,SNOOCMSL"
cmip_var_list="burntFractionAll, cCwd, cLeaf, cRoot, nppLeaf, nppRoot, nppWood, rGrowth, rMaint, tran, hfdsn, lwsnl, snc, snd, snm, snw, sootsn"

rgr_dir=${result_dir}/rgr_lnd
native_dir=${result_dir}/native_lnd
mkdir -p $rgr_dir
mkdir -p $native_dir

# Note either include the extra variable landfrac or specify the file that has landfrac for subgrid scale mode to work.
#ncclimo -P elm -j 1 --var_xtr=landfrac --map=${map_file} --start=$start --end=$end --ypf=$ypf --split -c $caseid -o ${native_dir} -O ${rgr_dir} -v ${raw_var_list} -i ${input_path} ${flags}
# Alternative ncclimo invocation
#ncclimo -P elm -j 1 --map=${map_file} --start=$start --end=$end --ypf=$ypf --split -c $caseid -o ${native_dir} -O ${rgr_dir} -v ${raw_var_list} -i ${input_path} ${flags} --sgs_frc=${input_path}/v2.LR.historical_0101.elm.h0.1850-01.nc/landfrac

#raw_var_list_elm_bgc="TOTLITC,CWDC,TOTPRODC,SOIL1C,SOIL2C,SOIL3C,^SOIL4C$,COL_FIRE_CLOSS,WOOD_HARVESTC,TOTVEGC,NBP,GPP,AR,HR"
# CMORIZE Land Monthly variables
e3sm_to_cmip -i ${rgr_dir} -o $result_dir  -v ${cmip_var_list} -t ${tables_path} -u ${metadata_path} --realm lnd --serial

