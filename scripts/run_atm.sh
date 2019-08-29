#!/bin/bash
NCO_PATH_OVERRIDE=Yes

vars=("RELHUM" "Q" "O3" "T" "U" "V" "Z3")
for i in "${vars[@]}"
do
    /export/zender1/bin/ncclimo --var=${i} -7 --dfl_lvl=1 --no_cll_msr --yr_srt=1 --yr_end=500 --ypf=25 --map=/export/zender1/data/maps/map_ne30np4_to_cmip6_180x360_aave.20181001.nc --drc_out=/p/user_pub/work/E3SM/cmip6_variables/piControl/atm/vrt_remapped_ne30 --drc_rgr=/p/user_pub/work/E3SM/cmip6_variables/piControl/atm/vrt_remapped_180x360 --drc_in=/p/user_pub/work/E3SM/cmip6_variables/piControl/atm/vrt_remapped/
done
