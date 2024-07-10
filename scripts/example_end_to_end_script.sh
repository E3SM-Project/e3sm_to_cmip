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


# CMORIZE Ocean Monthly variables
# Note the input folder for mpas ocean files requires:
# 1. History files: e.g.,v2.LR.historical_0101.mpaso.hist.am.timeSeriesStatsMonthly.1850-01-01.nc
# 2. The namelist file for constants: mpaso_in
#    masso, masscello, msftmz, pbo, and pso require 'config_density0'
#    hfsifrazil requires 'config_density0' and 'config_frazil_heat_of_fusion'
# 3. A restart file for mesh: e.g., v2.LR.historical_0101.mpaso.rst.1855-01-01_00000.nc
# 4. A region masks file for MOC regions: EC30to60E2r2_mocBasinsAndTransects20210623.nc (Needed for variable msftmz: Ocean Meridional Overturning Mass Streamfunction)

# BEYOND THIS POINT, We attempt to replace "${model_data}/v2.mpaso_input/" with a directory of symlinks ("native_data"), intended to support a
# user-specified "years-per-file" (YPF) value, and to call e3sm_to_cmip on those lonks in a loop that updates the links for each YPF segment.

ts=`date -u +%Y%m%d_%H%M%S_%6N`
runlog="e2e_e2c-${ts}.log"

Omon_var_list="areacello, fsitherm, hfds, masso, mlotst, sfdsi, sob, soga, sos, sosga, tauuo, tauvo, thetaoga, tob, tos, tosga, volo, wfo, zos, thetaoga, hfsifrazil, masscello, so, thetao, thkcello, uo, vo, volcello, wo, zhalfo"
mapfile=${e2c_path}/maps/map_EC30to60E2r2_to_cmip6_180x360_aave.20220301.nc

native_src=${model_data}/v2.mpaso_input/

in_count=`ls $native_src | wc -l`
echo "NATIVE_SOURCE_COUNT=$in_count files ($((in_count / 12)) years)" >> $runlog 2>&1


real_native_src=/p/user_pub/work/E3SM/2_0/historical/LR/ocean/native/model-output/mon/ens1/v20220806

# Determine range of years and number of segments from the available native input (model_data).
start_year=`ls $real_native_src | grep mpaso.hist.am.timeSeriesStatsMonthly | rev | cut -f2 -d. | rev | cut -f1 -d- | head -1`
final_year=`ls $real_native_src | grep mpaso.hist.am.timeSeriesStatsMonthly | rev | cut -f2 -d. | rev | cut -f1 -d- | tail -1`
range_years=$((10#$final_year - 10#$start_year + 1))
ypf=20
range_segs=$((range_years/ypf))
if [[ $((range_segs*ypf)) -lt $range_years ]]; then range_segs=$((range_segs + 1)); fi


native_data="native_links"
mkdir -p $native_data
rm $native_data/*

# WORK:  To begin, we need symlinks in native_data to ALL files in model_output, because the restart, namefile and region_mask files are there.

for afile in `ls $native_src`; do
    ln -s ${native_src}/$afile $native_data/$afile 2>/dev/null
done

# for this test, remove links to these datafiles, because we will use the full published years.
for afile in `ls ${native_data}/*mpaso.hist.am.timeSeriesStatsMonthly*.nc 2>/dev/null`; do
    rm -f $afile
done

for ((segdex=0;segdex<range_segs;segdex++)); do

    # wipe existing native_data datafile symlinks, create new range of same
    # then create the next segment of symlinks, and call the e3sm_to_cmip

    for afile in `ls ${native_data}/*mpaso.hist.am.timeSeriesStatsMonthly*.nc 2>/dev/null`; do
        rm -f $afile
    done

    for ((yrdex=0;yrdex<ypf;yrdex++)); do
        the_year=$((10#$start_year + segdex*ypf + yrdex))
        prt_year=`printf "%04d" "$the_year"`

        if [[ $the_year -gt $((10#$final_year)) ]]; then
            break;
        fi

        for afile in `ls $real_native_src/*.${prt_year}-*.nc`; do
            bfile=`basename $afile`
            ln -s $afile $native_data/$bfile 2>/dev/null
        done
    done

    year_init=$((10#$start_year + segdex*ypf))
    year_last=$((10#$start_year + segdex*ypf + yrdex - 1))
    ts=`date -u +%Y%m%d_%H%M%S_%6N`
    echo "$ts: Calling e3sm_to_cmip for segment years $year_init to $year_last" >> $runlog

    # e3sm_to_cmip -s --realm Omon --var-list $Omon_var_list --map ${mapfile} --input-path ${native_data}  --output-path ${result_dir} --user-metadata ${metadata_path} --tables-path ${tables_path} >> $runlog 2>&1

done


exit
