#!/bin/bash 

case1=(	v3.ccmi.PD_INT_custom30  
       	v3.ccmi.FD_INT_4xCO2_custom30_mem1
       	v3.ccmi.FD_INT_4xCO2_custom30_mem2
	v3.ccmi.FD_INT_4xCO2_custom30_mem3
	v3.ccmi.PD_NINT_custom30_mem1
        v3.ccmi.PD_NINT_custom30_mem2
	v3.ccmi.PD_NINT_custom30_mem3
	v3.ccmi.FD_INT_4xCO2_PDSST_custom30_mem1
	v3.ccmi.FD_INT_4xCO2_PDSST_custom30_mem2
	v3.ccmi.FD_INT_4xCO2_PDSST_custom30_mem3
	v3.ccmi.FD_NINT_4xCO2_custom30_mem1
	v3.ccmi.FD_NINT_4xCO2_custom30_mem2
	v3.ccmi.FD_NINT_4xCO2_custom30_mem3
	v3.ccmi.FD_INT_1xCO2_4KSST_custom30_mem1
	v3.ccmi.FD_INT_1xCO2_4KSST_custom30_mem2
	v3.ccmi.FD_INT_1xCO2_4KSST_custom30_mem3)
case2=(1 2 3)
case3=(90  30  30  30  30  30  30  30  30  30  30  30  30  30  30  30)
#for year in {1979..2014..1}; do
for i in 14; do
for j in 0; do
for k in 3; do
	#
	if [[ $k -eq 0 ]]; then
		nyear_s=11  #11  
		nyear_e=40  #100  
	else
		nyear_s=57  #41  #87  #27  #11  #87  #71  #57  #41  #57  #41  #27  #11  #26  #11 #71  #85  #71  #57  #41 #57 #41 #27
		nyear_e=70  #56  #100 #40  #26  #100 #86  #70  #56  #70  #56  #40  #26  #40  #25 #84  #100 #84  #70  #56 #70 #56 #40
		#40
	fi
	#
	for ((year=$nyear_s;year<=$nyear_e;year+=2))
	do
	#
	echo ${case1[i]}\_${case2[j]}\_${case3[k]} $nyear_s $nyear_e $year
if [[ ! -f "submit_${case1[i]}\_${case2[j]}\_$year.sh" ]]; 
then
	
	cp -r submit.sh	submit_${case1[i]}\_${case2[j]}\_$year.sh
	sed -i  "/case=/c 	case='${case1[i]}'"	submit_${case1[i]}\_${case2[j]}_$year.sh
	sed -i  "/year=/c	year=$year" submit_${case1[i]}\_${case2[j]}_$year.sh
	#sbatch submit_${case1[i]}\_${case2[j]}\_$year.sh
	./submit_${case1[i]}\_${case2[j]}\_$year.sh  &
fi

	done
done
done
done

