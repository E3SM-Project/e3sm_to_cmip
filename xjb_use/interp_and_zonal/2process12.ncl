;;for monthly
vars=(/"O3","H2OLNZ"/)
case="v3.ccmi.FD_NINT_4xCO2_custom30_mem3"
scale="daily"
year="10yr"
;time="001101_004012"
;time="004101_007012"
time="007101_010012"
nstart=71;41;11;71;11;41
;;
plevo=new(42,double)
plevo=(/100000.,92500.,85000.,70000.,60000.,50000.,40000.,30000.,25000.,20000.,17000.,15000.,\
	13000.,11500.,10000.,9000.,8000.,7000.,6000.,5000.,4000.,3500.,3000.,2500.,2000.,\
	1700.,1500.,1300.,1100.,1000.,900.,800.,600.,500.,400.,300.,200.,150.,100.,70.,50.,40./)
plevo2=plevo
plevo2@units="Pa"
delete(plevo2@_FillValue)
plevo=plevo/100.
plevo@units="hPa"
plevo1=plevo
;;
dir2="/global/cfs/cdirs/e3sm/xie7/ccmi_output/proc_med/"
;;
;rawdata="/global/cfs/cdirs/e3sm/xie7/ccmi_process/"+case+"/output/U/post/atm/180x360_aave/ts/monthly/30yr/U_"+time+".nc"
rawdata="/global/cfs/cdirs/e3sm/xie7/ccmi/"+case+"/output/U/post/atm/180x360_aave/ts/monthly/30yr/U_"+time+".nc"
fraw=addfile(rawdata,"r")
;;
ndays=365
numb=strlen(case)
numb=numb+2
;;
do i=0,1
	;dir="/global/cfs/cdirs/e3sm/xie7/ccmi_process/"+case+"/output/"+vars(i)+"/post/atm/180x360_aave/ts/"+scale+"/"+year+"/"
	dir="/global/cfs/cdirs/e3sm/xie7/ccmi/"+case+"/output/"+vars(i)+"/post/atm/180x360_aave/ts/"+scale+"/"+year+"/"
	fils=systemfunc("ls "+dir+"*nc")
	print(fils)
	;exit
	;;
	times=str_get_cols(fils,104+(numb-24+1),116+(numb-24+1))
	;print(times)
	;exit
	;;
	do j=0,29;1;0,21;0,1;89;;for 90yr simulation only
		;;
		kk=mod(j,10)
		k2=j/10
		;;
		f1=addfile(fils(k2),"r")
		;;
	aa=f1->$vars(i)$(0+kk*ndays:ndays-1+kk*ndays,:,:,:)
	;;convert to 1e+6
	aa=aa*1e+6
	;;
	;fil1="/global/cfs/cdirs/e3sm/xie7/ccmi_process/"+case+"/output/PS/post/atm/180x360_aave/ts/daily/30yr/PS_"+time+".nc"
	fil1="/global/cfs/cdirs/e3sm/xie7/ccmi/"+case+"/output/PS/post/atm/180x360_aave/ts/daily/30yr/PS_"+time+".nc"
	f2=addfile(fil1,"r")
	ps=f2->PS(0+j*ndays:ndays+j*ndays-1,:,:)
	;;
	aa_vint=vinth2p(aa,fraw->hyai,fraw->hybi,plevo1,ps,1,(fraw->P0)/todouble(100.),1,False)
	;;
	aa_vint!1="plev42"
	;;
	aa_vint&plev42=plevo
	aa_vint&plev42@standard_name="air_pressure"
	aa_vint&plev42@axis="Z"
	aa_vint&plev42@long_name="pressure"
	aa_vint&plev42@positive="down"
	delete(aa_vint&plev42@_FillValue)
	aa_vint_zonal=dim_avg_n_Wrap(aa_vint,3)
	;;
	system("mkdir -p "+dir2+"output/"+case+"/daily/2D/")
	;;
	if ((nstart+j).lt.100) then
                fil3=dir2+"output/"+case+"/daily/2D/"+vars(i)+"_00"+(nstart+j)+"01_00"+(nstart+j)+"12"+".nc"
        else
                fil3=dir2+"output/"+case+"/daily/2D/"+vars(i)+"_0"+(nstart+j)+"01_0"+(nstart+j)+"12"+".nc"
        end if
	;;
        system("rm -rf "+fil3)
        setfileoption("nc","Format","LargeFile")
        f4=addfile(fil3,"c")
	filedimdef( f4, "time", ndays, True)
        f4->$vars(i)$=aa_vint_zonal
	f4->lat_bnds=f1->lat_bnds
	f4->time_bnds=f1->time_bnds(0+kk*ndays:ndays-1+kk*ndays,:)
	;;
	delete(aa)
	delete(ps)
	;;
	delete(aa_vint)
	delete(aa_vint_zonal)
	;;
if (False) then
	if (kk.eq.9) then
	;;connect the data together
	system("rm -rf "+dir2+"output/"+case+"/daily/3D/"+vars(i)+"_"+times(k2)+".nc")
	system("rm -rf "+dir2+"output/"+case+"/daily/2D/"+vars(i)+"_"+times(k2)+".nc")
	;;
	system("mkdir -p "+dir2+"output/"+case+"/daily/3D/")
	system("mkdir -p "+dir2+"output/"+case+"/daily/2D/")
	;;
	;system("ncrcat  "+dir2+vars(i)+"*"+times(k2)+"*3D*nc "+dir2+"output/"+case+"/day/3D/"+vars(i)+"_"+times(k2)+".nc")
	;system("ncrcat  "+dir2+vars(i)+"*"+times(k2)+"*2D*nc "+dir2+"output/"+case+"/day/2D/"+vars(i)+"_"+times(k2)+".nc")
	;;
	;system("rm -rf "+dir2+vars(i)+"*"+times(k2)+"*3D*nc ")
	;system("rm -rf "+dir2+vars(i)+"*"+times(k2)+"*2D*nc ")
	end if
end if
	;;
	end do
        delete(fils)
end do
;;







exit
