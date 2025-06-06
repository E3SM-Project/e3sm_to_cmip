#!/bin/bash 
	module load idl
	case='v3.ccmi.PD_INT_custom30'
	year=11
	offset=0
	count=146 #73
	#
	offsetf=$(printf "%02d\n" $offset)
	year1=$(printf "%04d\n" $year)
	year2=$(printf "%04d\n" $(($year+1)))
	time=$year1\01_$year2\12
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
	#
	for offset in {0..19..1}; do
	#for offset in {0..39..1}; do
	#for offset in {0..0..1}; do
	offsetf=$(printf "%02d\n" $offset)
	#
	tim1=$(($count*$offset))
	tim2=$(($count*$(($offset+1))-1))
	echo $tim1 $tim2
	#
	filename=new_proc_$offset.sh
	cp -r new_proc.sh  $filename
	sed -i "/case=/c \ case=$case" 	       	$filename
	sed -i "/year=/c \ year=$year"		$filename
	sed -i "/offset=0/c \  offset=$offset" 	$filename
	sed -i "/tim1=/c  \ tim1=$tim1" $filename
	sed -i "/tim2=/c  \ tim2=$tim2" $filename
	./$filename >& output_all_$offset &
		#exit
	done
	#sleep 800
	#
exit
