#!/bin/bash 

#SBATCH  --job-name=tem
#SBATCH  --account=e3sm
#SBATCH  --nodes=1
#SBATCH  --output=/global/cfs/cdirs/e3sm/xie7/ccmi/ccmi_cmor/e3sm_to_cmip/xjb_use/tem/batch/tem.o%j
#SBATCH  --exclusive
#SBATCH  --time=10:00:00
#SBATCH  --qos=regular
#SBATCH  --constraint=cpu

	source   /global/common/software/e3sm/anaconda_envs/load_latest_e3sm_unified_pm-cpu.sh
	module load idl
	case='v3.ccmi.PD_INT_custom30'
	year=11
	count=73
	#
	year1=$(printf "%04d\n" $year)
	year2=$(printf "%04d\n" $(($year+1)))
	time=$year1\01_$year2\12
	time2=$year1\_$year2
	#
	dir=/global/cfs/cdirs/e3sm/xie7/ccmi/
	#
	diriu=$dir/$case/output/U/post/atm/180x360_aave/ts/hourly/2yr/U_$time.nc
	diriv=$dir/$case/output/V/post/atm/180x360_aave/ts/hourly/2yr/V_$time.nc
	dirit=$dir/$case/output/T/post/atm/180x360_aave/ts/hourly/2yr/T_$time.nc
	diriw=$dir/$case/output/OMEGA/post/atm/180x360_aave/ts/hourly/2yr/OMEGA_$time.nc
	#
	diro=/global/cfs/cdirs/e3sm/xie7/ccmi_output/proc_med/tem/$case/
	mkdir -p $diro
	mkdir -p /global/cfs/cdirs/e3sm/xie7/ccmi_output/proc_med/output/$case/tem/
	mkdir -p /global/cfs/cdirs/e3sm/xie7/ccmi_output/proc_med/output/$case/tem/med/


#if [[ 1 -eq 2 ]]; then
	#
	for offset in {0..39..1}; do
	#for offset in {0..2..1}; do
	offsetf=$(printf "%02d\n" $offset)
	#
	tim1=$(($count*$offset))
	tim2=$(($count*$(($offset+1))-1))
	echo $tim1 $tim2
	#check if the data is already there
	echo /global/cfs/cdirs/e3sm/xie7/ccmi_output/proc_med/output/$case/tem/med/$case\_$time2\_$offsetf.nc
if [ ! -s  /global/cfs/cdirs/e3sm/xie7/ccmi_output/proc_med/output/$case/tem/med/$case\_$time2\_$offsetf.nc ]; then

	#exit
	ncks -O --fl_fmt=classic -d time,$tim1,$tim2   $diriu  $diro/U_$time\_$offsetf.nc
	ncks -O --fl_fmt=classic -d time,$tim1,$tim2   $diriv  $diro/V_$time\_$offsetf.nc
	ncks -O --fl_fmt=classic -d time,$tim1,$tim2   $dirit  $diro/T_$time\_$offsetf.nc
	ncks -O --fl_fmt=classic -d time,$tim1,$tim2   $diriw  $diro/OMEGA_$time\_$offsetf.nc
		#
		filename=3hr_epflux_$case\_$year\_$offsetf.pro
		cp -r 3hr_epflux.pro  $filename
		#
		echo $offsetf
		#
		sed -i "/runname=/c \ runname='$case'"  	$filename
		sed -i "/offset=/c   \ offset='$offsetf'"       $filename
		sed -i "/count=/c   \ count=$count" 	      	$filename
		sed -i "/year1=/c    \ year1='$year1'"   	$filename
		sed -i "/year2=/c    \ year2='$year2'"          $filename
		#
		idl  $filename  >& output_idl_$time\_$offsetf  
		wait
		sleep 10
		rm $diro/*_$time\_$offsetf.nc
fi
	done
#fi
	#sleep 60
	ls /global/cfs/cdirs/e3sm/xie7/ccmi_output/proc_med/output/$case/tem/med/$case\_$time2\_??.nc 
	ls /global/cfs/cdirs/e3sm/xie7/ccmi_output/proc_med/output/$case/tem/med/$case\_$time2\_??.nc | wc -l
	#ncrcat /global/cfs/cdirs/e3sm/xie7/ccmi_output/proc_med/output/$case/tem/med/$case\_$time2\_??.nc  /global/cfs/cdirs/e3sm/xie7/ccmi_output/proc_med/output/$case/tem/TEM_$case\_$time.nc
	ncrcat -v VADV /global/cfs/cdirs/e3sm/xie7/ccmi_output/proc_med/output/$case/tem/med/$case\_$time2\_??.nc  /global/cfs/cdirs/e3sm/xie7/ccmi_output/proc_med/output/$case/tem/TEM_$case\_$time.nc

	#
        dir_tem=/global/cfs/cdirs/e3sm/xie7/ccmi_output/proc_med/output/$case/tem/
        for ii in {0..7..1}; do
        TEM=(DELF  FPHI   FZ   VADV   WADV   VRES  WRES  KESIRES)
        dirr=$dir_tem/${TEM[ii]}
        mkdir -p $dirr
        ncrcat $dir_tem
        /global/cfs/cdirs/e3sm/xie7/ccmi_output/proc_med/output/$case/tem/med/
        *nc  /global/cfs/cdirs/e3sm/xie7/ccmi_output/proc_med/output/$case/tem/TEM_$case\_$time.nc
        done
	#remove used redundant
	#rm $diro/*_$time\_$offsetf.nc
	#
exit
