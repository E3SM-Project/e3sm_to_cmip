#!/bin/bash 
	module load idl
	case='v3.ccmi.PD_INT_custom30'
	year=11
	offset=0
	count=73
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
	for offset in {0..39..1}; do
	offsetf=$(printf "%02d\n" $offset)
	#
	#tim1=$((146*$offset))
	#tim2=$((146*$(($offset+1))-1))
	tim1=$(($count*$offset))
	tim2=$(($count*$(($offset+1))-1))
	echo $tim1 $tim2
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
		idl  $filename  >& output_idl_$time\_$offsetf  #&
		#exit
	done
	#sleep 800
	#
exit
