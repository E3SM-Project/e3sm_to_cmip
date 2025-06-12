load "/pscratch/sd/x/xie7/from_cori/Analysis/NCLep/self.ncl"
begin
;;
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
var=(/"DELF","FPHI","FZ","KESIRES","VADV","VRES","WADV","WRES","VT","UV","UW"/)
dir="/global/cfs/cdirs/e3sm/xie7/ccmi_output/proc_med/output/v3.ccmi.PD_INT_custom30/tem/hourly/"
dir_day="/global/cfs/cdirs/e3sm/xie7/ccmi_output/proc_med/output/v3.ccmi.PD_INT_custom30/tem/daily/"
dir_mon="/global/cfs/cdirs/e3sm/xie7/ccmi_output/proc_med/output/v3.ccmi.PD_INT_custom30/tem/monthly/"
;;
rawdata="/global/cfs/cdirs/e3sm/xie7/ccmi_orig/v3.ccmi.PD_INT_custom30/archive/atm/hist/v3.ccmi.PD_INT_custom30.eam.h0.0024-09.nc"
fraw=addfile(rawdata,"r")
;;
fps_day="/global/cfs/cdirs/e3sm/xie7/ccmi/v3.ccmi.PD_INT_custom30/output/PS/post/atm/180x360_aave/ts/daily/90yr/PS_001101_010012.nc"
fps_mon="/global/cfs/cdirs/e3sm/xie7/ccmi/v3.ccmi.PD_INT_custom30/output/PS/post/atm/180x360_aave/ts/monthly/90yr/PS_001101_010012.nc"
f1=addfile(fps_day,"r")
f2=addfile(fps_mon,"r")
;;
do i=0,10;8,10;0,7
fils=systemfunc("ls "+dir+var(i)+"/*nc")
fils_name=systemfunc("ls "+dir+var(i))
;;
	do j=0,dimsizes(fils)-1
		ff=addfile(fils(j),"r")
		aa=ff->$var(i)$
		;;
		;;form day and mon
		aa_day=calculate_daily_values(aa,"avg",0,False)
		aa_mon=calculate_monthly_values(aa,"avg",0,False)
		;;take 2 years leave last day/mon out due to 3rd year
		bb_day=aa_day(0:729,:,:)
		bb_mon=aa_mon(0:23,:,:)
		dimday=dimsizes(bb_day)
		dimmon=dimsizes(bb_mon)
		;;vert interp
		;;
		bb_day_lon=new((/dimday(0),dimmon(1),dimday(2),1/),float)
		bb_mon_lon=new((/dimmon(0),dimmon(1),dimmon(2),1/),float)
		bb_day_lon(:,:,:,0)=bb_day
		bb_mon_lon(:,:,:,0)=bb_mon
		;;
		ps_day=new((/dimday(0),dimday(2),1/),float)
		ps_mon=new((/dimmon(0),dimmon(2),1/),float)
		ps_day(:,:,0)=dim_avg_n_Wrap(f1->PS(0+j*730:729+j*730,:,:),2)
		ps_mon(:,:,0)=dim_avg_n_Wrap(f2->PS(0+j*24:23+j*24,:,:),2)
		;;
		bb_day_vint=vinth2p(bb_day_lon,fraw->hyai,fraw->hybi,plevo,ps_day,1,(fraw->P0)/todouble(100.),1,False)
		bb_mon_vint=vinth2p(bb_mon_lon,fraw->hyai,fraw->hybi,plevo,ps_mon,1,(fraw->P0)/todouble(100.),1,False)
		;;
		bb_day_vint!1="plev42"
		bb_mon_vint!1="plev42"
		bb_day_vint&plev42=bb_day_vint&plev42*100.
		bb_mon_vint&plev42=bb_mon_vint&plev42*100.
		bb_day_vint&plev42@units="Pa"
		bb_mon_vint&plev42@units="Pa"
		;;output data
		system("mkdir -p "+dir_day+var(i)+"/med/")
		system("mkdir -p "+dir_mon+var(i)+"/med/")
		system("rm -f "+dir_day+var(i)+"/med/"+fils_name(j))
		system("rm -f "+dir_mon+var(i)+"/med/"+fils_name(j))
		fday=addfile(dir_day+var(i)+"/med/"+fils_name(j),"c")
		fmon=addfile(dir_mon+var(i)+"/med/"+fils_name(j),"c")
		filedimdef(fday,"time",-1,True)
		filedimdef(fmon,"time",-1,True)
		;;
		fday->$var(i)$=bb_day_vint(:,:,:,0)
		;fday->lat_bnds=f1->lat_bnds
		;fday->time_bnds=f1->time_bnds(0+j*730:729+j*730,:)
		fmon->$var(i)$=bb_mon_vint(:,:,:,0)
		;fmon->lat_bnds=f2->lat_bnds
		;fmon->time_bnds=f2->time_bnds(0+j*24:23+j*24,:)
		;;
		delete(aa)
		delete(aa_mon)
		delete(aa_day)
		delete(bb_day)
		delete(bb_mon)
		;;
		delete(ps_day)
		delete(ps_mon)
		;;
		delete(bb_day_lon)
		delete(bb_mon_lon)
		delete(bb_day_vint)
		delete(bb_mon_vint)
	end do
	;;connect data into one
	system("ncrcat -O "+dir_day+var(i)+"/med/*nc "+dir_day+var(i)+"_"+time+".nc")  
	system("ncrcat -O "+dir_mon+var(i)+"/med/*nc "+dir_mon+var(i)+"_"+time+".nc") 
	fday_comb=addfile(dir_day+var(i)+"_"+time+".nc","w")
	fmon_comb=addfile(dir_mon+var(i)+"_"+time+".nc","w")
	fday_comb->time_bnds=f1->time_bnds
	fday_comb->lat_bnds=f1->lat_bnds
	fmon_comb->time_bnds=f2->time_bnds
	fmon_comb->lat_bnds=f2->lat_bnds
	;;
delete(fils)
delete(fils_name)
end do
exit
















end
