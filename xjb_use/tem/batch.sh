#!/bin/bash 

case1=(obsclim  obsqboi  obsamip)
case2=(01 02 03)
case3=(""  _SSP370)

#for year in {1979..2014..1}; do
for i in 0; do
#for i in 0 1 2; do
for j in 1; do
for k in 1; do
#for k in 0 1; do
#i=0
#j=0
#1
#k=1
	#
	echo ${case1[i]}\_${case2[j]}${case3[k]}
	mkdir -p /global/cscratch1/sd/xie7/qboi/TEM_calculate/idl/output/${case1[i]}\_${case2[j]}${case3[k]}
	#
	if [[ k -eq 0 ]]; 
	then
	nyear_s=1979
	nyear_e=2014
	else
	nyear_s=2015 #8 #2015
	nyear_e=2015 #8 #2020
	fi
	#
	for ((year=$nyear_s;year<=$nyear_e;year++))
	do
	#for year in 2014;
	#do
	#
	echo ${case1[i]}\_${case2[j]}${case3[k]} $year
	#for offset in {0..1}; do
	offset=0
	#1
	#0
if [[ ! -f "submit_${case1[i]}\_${case2[j]}${case3[k]}\_$year\_$offset.sh" ]]; 
then
	
	cp -r submit.sh	submit_${case1[i]}\_${case2[j]}${case3[k]}\_$year\_$offset.sh
	sed -i  "/case=/c 	case=${case1[i]}\_${case2[j]}${case3[k]}"	submit_${case1[i]}\_${case2[j]}${case3[k]}\_$year\_$offset.sh
	sed -i  "/year=/c	year=$year" submit_${case1[i]}\_${case2[j]}${case3[k]}\_$year\_$offset.sh
	sed -i  "/offset2=/c	offset2=$(($offset*5))" submit_${case1[i]}\_${case2[j]}${case3[k]}\_$year\_$offset.sh
	sbatch submit_${case1[i]}\_${case2[j]}${case3[k]}\_$year\_$offset.sh
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

