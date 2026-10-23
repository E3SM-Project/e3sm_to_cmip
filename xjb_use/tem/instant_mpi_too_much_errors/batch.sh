#!/bin/bash 

case1=(v3.ccmi.PD_INT_custom30     )
case2=(1 2 3)
case3=(90  30)

#for year in {1979..2014..1}; do
for i in 0; do
for j in 0; do
for k in 0; do
	#
	echo ${case1[i]}\_${case2[j]}\_${case3[k]}
	mkdir -p /global/cfs/cdirs/e3sm/xie7/ccmi_output/proc_med/tem/${case1[i]}\_${case2[j]}\_${case3[k]}
	exit
	#
	if [[ $k -eq 0 ]]; then
		nyear_s=11
		nyear_e=100
	else
		nyear_s=11
		nyear_e=100
	fi
	#
	for ((year=$nyear_s;year<=$nyear_e;year++))
	do
	#
	echo ${case1[i]}\_${case2[j]}\_${case3[k]} $nyear_s $nyear_e
	#for offset in {0..1}; do
	offset=0
	if [[ ! -f "submit_${case1[i]}\_${case2[j]}\_${case3[k]}\_$year\_$offset.sh" ]]; 
then
	
	cp -r submit.sh	submit_${case1[i]}\_${case2[j]}\_${case3[k]}\_$year\_$offset.sh
	sed -i  "/case=/c 	case=${case1[i]}\_${case2[j]}\_${case3[k]}"	submit_${case1[i]}\_${case2[j]}${case3[k]}\_$year\_$offset.sh
	sed -i  "/year=/c	year=$year" submit_${case1[i]}\_${case2[j]}\_${case3[k]}\_$year\_$offset.sh
	sed -i  "/offset2=/c	offset2=$(($offset*5))" submit_${case1[i]}\_${case2[j]}\_${case3[k]}\_$year\_$offset.sh
	#sbatch submit_${case1[i]}\_${case2[j]}\_${case3[k]}\_$year\_$offset.sh
fi

	#sleep 5
	#done
	#sleep 5
	done
#done
	#exit
done
done
done

