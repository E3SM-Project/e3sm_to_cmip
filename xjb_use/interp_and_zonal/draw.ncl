load "/pscratch/sd/x/xie7/from_cori/Analysis/NCLep/self.ncl"
begin
skip=5;4;3;2;1;0
draw_name="ccmi_"
dir_draw="/pscratch/sd/x/xie7/ccmi/Analysis/PDF/"
;;
	if (skip.eq.0) then
		load "/global/cfs/cdirs/e3sm/xie7/ccmi/ccmi_cmor/e3sm_to_cmip/xjb_use/interp_and_zonal/2process.ncl"
	else if (skip.eq.1) then 
		load "/global/cfs/cdirs/e3sm/xie7/ccmi/ccmi_cmor/e3sm_to_cmip/xjb_use/interp_and_zonal/2process2.ncl"
	else if (skip.eq.2) then
		load "/global/cfs/cdirs/e3sm/xie7/ccmi/ccmi_cmor/e3sm_to_cmip/xjb_use/interp_and_zonal/2process3.ncl"
	else if (skip.eq.3) then
		load "/global/cfs/cdirs/e3sm/xie7/ccmi/ccmi_cmor/e3sm_to_cmip/xjb_use/interp_and_zonal/2process4.ncl"
	else if (skip.eq.4) then
		load "/global/cfs/cdirs/e3sm/xie7/ccmi/ccmi_cmor/e3sm_to_cmip/xjb_use/interp_and_zonal/2process5.ncl"
	else if (skip.eq.5) then
		load "/global/cfs/cdirs/e3sm/xie7/ccmi/ccmi_cmor/e3sm_to_cmip/xjb_use/interp_and_zonal/2process6.ncl"
	end if
	end if
	end if
	end if
	end if
	end if
;;
end 
