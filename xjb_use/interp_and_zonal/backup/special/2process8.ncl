load "/pscratch/sd/x/xie7/from_cori/Analysis/NCLep/self.ncl"
begin
skip=7;11;10;9;8;6;7;11;10;9;8;7;6;1;2;1;6;5;4;3;2;1;0
draw_name="ccmi_"
dir_draw="/pscratch/sd/x/xie7/ccmi/Analysis/PDF/"

;;for daily
vars=(/"U","V","T","OMEGA","Z3","UTEND_CLUBB","UTGWORO","UTGWSPEC","VTEND_CLUBB","VTGWORO","VTGWSPEC"/)
;case="v3.ccmi.PD_INT_custom30"
case="v3.ccmi.FD_INT_4xCO2_custom30_mem2";1"
scale="daily";monthly"
year="10yr"
;time="001101_004012"
time="004101_007012"
nstart=41
;;
plevo=new(42,double)
plevo=(/100000.,92500.,85000.,70000.,60000.,50000.,40000.,30000.,25000.,20000.,17000.,15000.,\
	13000.,11500.,10000.,9000.,8000.,7000.,6000.,5000.,4000.,3500.,3000.,2500.,2000.,\
	1700.,1500.,1300.,1100.,1000.,900.,800.,600.,500.,400.,300.,200.,150.,100.,70.,50.,40./)
plevo2=plevo
plevo=plevo/100.
plevo@units="hPa"
;;
dir2="/global/cfs/cdirs/e3sm/xie7/ccmi_output/proc_med/";+"day/"
;;
rawdata="/global/cfs/cdirs/e3sm/xie7/ccmi_orig/"+case+"/archive/atm/hist/"+case+".eam.h0.0065-09.nc"
fraw=addfile(rawdata,"r")
;;
ndays=365
;;
do i=0,10;6,10;4,4;0,4
	dir="/global/cfs/cdirs/e3sm/xie7/ccmi/"+case+"/output/"+vars(i)+"/post/atm/180x360_aave/ts/"+scale+"/"+year+"/"
	fils=systemfunc("ls "+dir+"*nc")
	;;
	times=str_get_cols(fils,104,116)
	;;
	do j=0,29;89;1;0,21;0,1;89;;for 30yr simulation only
		;;
		kk=mod(j,10)
		k2=j/10
		;;
		f1=addfile(fils(k2),"r")
		;;
	aa=f1->$vars(i)$(0+kk*ndays:ndays-1+kk*ndays,:,:,:)
	printVarSummary(aa)
	;;
	fil1="/global/cfs/cdirs/e3sm/xie7/ccmi/"+case+"/output/PS/post/atm/180x360_aave/ts/daily/30yr/PS_"+time+".nc"
	f2=addfile(fil1,"r")
	ps=f2->PS(0+j*ndays:ndays+j*ndays-1,:,:)
	;;
	aa_vint=vinth2p(aa,fraw->hyai,fraw->hybi,plevo,ps,1,(fraw->P0)/todouble(100.),1,False)
	;;
	aa_vint!1="plev42"
	;;
	plevo@units="Pa"
	plevo=plevo2
	delete(plevo@_FillValue)
	;;
	aa_vint&plev42=plevo
	aa_vint&plev42@standard_name="air_pressure"
	aa_vint&plev42@axis="Z"
	aa_vint&plev42@long_name="pressure"
	aa_vint&plev42@positive="down"
	delete(aa_vint&plev42@_FillValue)
	;delete(aa_vint@_FillValue)
	;;
	;fil2=dir2+vars(i)+"_"+case+"_"+scale+"_"+year+"_"+times(k2)+"_3D_"+j+".nc"
	;fil2=dir2+"output/"+case+"/day/3D/"+vars(i)+"_001101_001112"+".nc"
	system("mkdir -p "+dir2+"output/"+case+"/daily/3D/")
	system("mkdir -p "+dir2+"output/"+case+"/daily/2D/")
	;;
if (i.lt.5) then
	if ((nstart+j).lt.100) then
		fil2=dir2+"output/"+case+"/daily/3D/"+vars(i)+"_00"+(nstart+j)+"01_00"+(nstart+j)+"12"+".nc"
	else
		fil2=dir2+"output/"+case+"/daily/3D/"+vars(i)+"_0"+(nstart+j)+"01_0"+(nstart+j)+"12"+".nc"
	end if
	;;
	system("rm -rf "+fil2)
	setfileoption("nc","Format","LargeFile")
	f3=addfile(fil2,"c")
	filedimdef( f3, "time", ndays, True)
	f3->$vars(i)$=aa_vint
	f3->lat_bnds=f1->lat_bnds
	f3->lon_bnds=f1->lon_bnds
	f3->time_bnds=f1->time_bnds(0+kk*ndays:ndays-1+kk*ndays,:)
end if
	;;
;if (False) then
;if (i.ge.4) then;;only zg needs daily and zonal mean
	aa_vint_zonal=dim_avg_n_Wrap(aa_vint,3)
	;;
	if ((nstart+j).lt.100) then
                fil3=dir2+"output/"+case+"/daily/2D/"+vars(i)+"_00"+(nstart+j)+"01_00"+(nstart+j)+"12"+".nc"
        else
                fil3=dir2+"output/"+case+"/daily/2D/"+vars(i)+"_0"+(nstart+j)+"01_0"+(nstart+j)+"12"+".nc"
        end if
	print(fil3)
	;;
        system("rm -rf "+fil3)
        setfileoption("nc","Format","LargeFile")
        f4=addfile(fil3,"c")
	filedimdef( f4, "time", ndays, True)
        f4->$vars(i)$=aa_vint_zonal
	f4->lat_bnds=f1->lat_bnds
	f4->time_bnds=f1->time_bnds(0+kk*ndays:ndays-1+kk*ndays,:)
;end if
	;;
	delete(aa)
	delete(ps)
	;;
	delete(aa_vint)
	;delete(aa_vint_zonal)
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
end
