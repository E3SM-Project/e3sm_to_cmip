;;for monthly
vars=(/"O3","H2OLNZ","E90","NO","NO2","N2OLNZ","N2O5","HNO3","OH","HO2","CH4LNZ","SO2","jo1dU","NOYLNZ"/);;AODABS is 2D and just use is enough
case="v3.ccmi.PD_INT_custom30"
scale="monthly"
year="90yr"
time="001101_010012"
;;
plevo=new(42,double)
plevo=(/100000.,92500.,85000.,70000.,60000.,50000.,40000.,30000.,25000.,20000.,17000.,15000.,\
	13000.,11500.,10000.,9000.,8000.,7000.,6000.,5000.,4000.,3500.,3000.,2500.,2000.,\
	1700.,1500.,1300.,1100.,1000.,900.,800.,600.,500.,400.,300.,200.,150.,100.,70.,50.,40./)
plevo2=plevo
plevo=plevo/100.
plevo@units="hPa"
;;
dir2="/global/cfs/cdirs/e3sm/xie7/ccmi_output/proc_med/"
;;
rawdata="/global/cfs/cdirs/e3sm/xie7/ccmi/v3.ccmi.PD_INT_custom30/archive/atm/hist/v3.ccmi.PD_INT_custom30.eam.h0.0024-09.nc"
fraw=addfile(rawdata,"r")
;;
do i=7,13
	dir="/global/cfs/cdirs/e3sm/xie7/ccmi/"+case+"/output/"+vars(i)+"/post/atm/180x360_aave/ts/"+scale+"/"+year+"/"
	fil=dir+vars(i)+"_"+time+".nc"
	print(fil)
	;;
	f1=addfile(fil,"r")
	do j=0,9;;for 90yr simulation only
	aa=f1->$vars(i)$(0+j*108:108+j*108-1,:,:,:)
	;;convert to ppm for those tracers 
	if (vars(i).ne."AODABS".and.\
	    vars(i).ne."jo1dU") then
	    aa=aa*1e+6
    	end if
	;;
	fil1="/global/cfs/cdirs/e3sm/xie7/ccmi/v3.ccmi.PD_INT_custom30/output/PS/post/atm/180x360_aave/ts/monthly/90yr/PS_001101_010012.nc"
	f2=addfile(fil1,"r")
	ps=f2->PS(0+j*108:108+j*108-1,:,:)
	;;
	aa_vint=vinth2p(aa,fraw->hyai,fraw->hybi,plevo,ps,1,(fraw->P0)/todouble(100.),1,False)
	
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
	;;
	aa_vint_zonal=dim_avg_n_Wrap(aa_vint,3)
	fil3=dir2+vars(i)+"_"+case+"_"+scale+"_"+year+"_"+time+"_2D_"+j+".nc"
        system("rm -rf "+fil3)
        setfileoption("nc","Format","LargeFile")
        f4=addfile(fil3,"c")
	filedimdef( f4, "time", 108, True)
        f4->$vars(i)$=aa_vint_zonal
	f4->lat_bnds=f1->lat_bnds
	f4->time_bnds=f1->time_bnds(0+j*108:108+j*108-1,:)
	;;
	delete(aa)
	delete(ps)
	;;
	delete(aa_vint)
	delete(aa_vint_zonal)
	end do
	;;connect the data together
	system("rm -rf "+dir2+"output/"+case+"/"+scale+"/2D/"+vars(i)+"_"+time+".nc")
	system("mkdir -p "+dir2+"output/"+case+"/"+scale+"/2D/")
	system("ncrcat  "+dir2+vars(i)+"*2D*nc "+dir2+"output/"+case+"/"+scale+"/2D/"+vars(i)+"_"+time+".nc")
	system("rm -rf "+dir2+vars(i)+"*2D*nc ")
end do
;;

exit
