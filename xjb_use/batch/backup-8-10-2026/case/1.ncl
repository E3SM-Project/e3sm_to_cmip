load "/pscratch/sd/x/xie7/from_cori/Analysis/NCLep/self.ncl"
begin


;fil1="/global/cfs/cdirs/e3sm/xie7/ccmi_output/QUOCA/QUOCA/LLNL/E3SM/FTINT-PDCO2/r3i1p1f1/monZ/zg/gr/v20251001/zg_monZ_E3SM_FTINT-PDCO2_r3i1p1f1_gr_007101-010012.nc"
;fil2="/global/cfs/cdirs/e3sm/xie7/ccmi_output/proc_med/output/v3.ccmi.FD_INT_1xCO2_4KSST_custom30_mem3/monthly/2D/Z3_007101_010012.nc"
fil1="/global/cfs/cdirs/e3sm/xie7/ccmi_output/QUOCA/QUOCA/LLNL/E3SM/FTINT/r2i1p1f1/monZ/zg/gr/v20251001/zg_monZ_E3SM_FTINT_r2i1p1f1_gr_004101-007012.nc"
fil2="/global/cfs/cdirs/e3sm/xie7/ccmi_output/proc_med/output/v3.ccmi.FD_INT_4xCO2_custom30_mem2/monthly/2D/Z3_004101_007012.nc"
f1=addfile(fil1,"r")
f2=addfile(fil2,"r")
printMinMax(f1->zg-f2->Z3,0)








end
