;;for monthly
vars=(/"U","V","T","OMEGA"/)
case="v3.ccmi.PD_INT_custom30"
scale="hourly"
year="2yr"
time="001101_010012"
;;
dir2="/global/cfs/cdirs/e3sm/xie7/ccmi_output/proc_med/"
;;
n6hours=292
;;
do i=0,3
	dir="/global/cfs/cdirs/e3sm/xie7/ccmi/"+case+"/output/"+vars(i)+"/post/atm/180x360_aave/ts/"+scale+"/"+year+"/"
	fils=systemfunc("ls "+dir+"*nc")
	;;
	times=str_get_cols(fils,104,116)
	;;
	print(fils)
	;;
	do j=0,450-1;;for 90yr simulation only
		;;
		kk=mod(j,10)
		k2=j/10
		;;
		f1=addfile(fils(k2),"r")
		;;
	aa=f1->$vars(i)$(0+kk*n6hours:n6hours-1+kk*n6hours,:,:,:)
	aa!1="plevTEM"
	;;
	fil2=dir2+vars(i)+"_"+case+"_"+scale+"_"+year+"_"+times(k2)+"_3D_"+kk+".nc"
	system("rm -rf "+fil2)
	setfileoption("nc","Format","LargeFile")
	f3=addfile(fil2,"c")
	filedimdef( f3, "time", n6hours, True)
	;;
	aa_vint=aa
	f3->$vars(i)$=aa_vint(:,{0.4:150},{-15:15},:)
	f3->lat_bnds=f1->lat_bnds({-15:15},:)
	f3->lon_bnds=f1->lon_bnds
	f3->time_bnds=f1->time_bnds(0+kk*n6hours:n6hours-1+kk*n6hours,:)
	;;
	aa_vint=aa
	aa_vint_zonal=dim_avg_n_Wrap(aa_vint,3)
	;;
	fil3=dir2+vars(i)+"_"+case+"_"+scale+"_"+year+"_"+times(k2)+"_2D_"+kk+".nc"
        system("rm -rf "+fil3)
        setfileoption("nc","Format","LargeFile")
        f4=addfile(fil3,"c")
	filedimdef( f4, "time", n6hours, True)
        f4->$vars(i)$=aa_vint_zonal
	f4->lat_bnds=f1->lat_bnds
	f4->time_bnds=f1->time_bnds(0+kk*n6hours:n6hours-1+kk*n6hours,:)
	;;
	delete(aa)
	delete(aa_vint)
	delete(aa_vint_zonal)
	;;
	if (kk.eq.9) then
	;;connect the data together
	system("rm -rf "+dir2+"output/"+case+"/"+scale+"/3D/"+vars(i)+"_"+times(k2)+".nc")
	system("rm -rf "+dir2+"output/"+case+"/"+scale+"/2D/"+vars(i)+"_"+times(k2)+".nc")
	;;
	system("mkdir -p "+dir2+"output/"+case+"/"+scale+"//3D/")
	system("mkdir -p "+dir2+"output/"+case+"/"+scale+"/2D/")
	;;
	system("ncrcat  "+dir2+vars(i)+"*"+times(k2)+"*3D*nc "+dir2+"output/"+case+"/"+scale+"/3D/"+vars(i)+"_"+times(k2)+".nc")
	system("ncrcat  "+dir2+vars(i)+"*"+times(k2)+"*2D*nc "+dir2+"output/"+case+"/"+scale+"/2D/"+vars(i)+"_"+times(k2)+".nc")
	;;
	system("rm -rf "+dir2+vars(i)+"*"+times(k2)+"*3D*nc ")
	system("rm -rf "+dir2+vars(i)+"*"+times(k2)+"*2D*nc ")
	end if
	;;
	end do
        delete(fils)
	;;
end do
;;
exit
