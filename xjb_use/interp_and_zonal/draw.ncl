load "/pscratch/sd/x/xie7/from_cori/Analysis/NCLep/self.ncl"
begin
skip=1;0
draw_name="ccmi_"
dir_draw="/pscratch/sd/x/xie7/ccmi/Analysis/PDF/"
;;
	if (skip.eq.0) then
		load "/global/cfs/cdirs/e3sm/xie7/ccmi/ccmi_cmor/e3sm_to_cmip/xjb_use/interp_and_zonal/2process.ncl"
	else if (skip.eq.1) then 
		load "/global/cfs/cdirs/e3sm/xie7/ccmi/ccmi_cmor/e3sm_to_cmip/xjb_use/interp_and_zonal/2process2.ncl"
	end if
	end if
;;
end 
