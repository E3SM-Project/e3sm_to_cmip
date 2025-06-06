#!/bin/bash 

case1=(v3.ccmi.PD_INT_custom30     )
case2=(1 2 3)
case3=(90  30)

#for year in {1979..2014..1}; do
for i in 0; do
for j in 0; do
for k in 0; do
	#
	if [[ $k -eq 0 ]]; then
		nyear_s=11
		nyear_e=100
	else
		nyear_s=11
		nyear_e=100
	fi
	#
	for ((year=$nyear_s;year<=$nyear_e;year+=2))
	do
	#
	echo ${case1[i]}\_${case2[j]}\_${case3[k]} $nyear_s $nyear_e
	if [[ ! -f "submit_${case1[i]}\_${case2[j]}\_$year.sh" ]]; 
then
	
	cp -r submit.sh	submit_${case1[i]}\_${case2[j]}\_$year.sh
		if [[ $i -eq 0 ]]; then
			sed -i  "/case=/c 	case='${case1[i]}'"	submit_${case1[i]}\_${case2[j]}_$year.sh
		else
			sed -i  "/case=/c       case='${case1[i]}\_${case2[j]}'"        submit_${case1[i]}\_${case2[j]}_$year.sh
		fi
	sed -i  "/year=/c	year=$year" submit_${case1[i]}\_${case2[j]}_$year.sh
	sbatch submit_${case1[i]}\_${case2[j]}\_${case3[k]}\_$year.sh
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

