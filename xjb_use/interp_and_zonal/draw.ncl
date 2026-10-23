load "/pscratch/sd/x/xie7/from_cori/Analysis/NCLep/self.ncl"
begin
skip=9;7;11;10;9;8;7;6;11;10;8;6;11;10;9;8;7;6;8;7;6;7;11;7;9;11;10;9;8;7;6;10;9;6;8;6;9;11;9;7;11;10;9;8;7;6;10;8;11;9;10;7;10;9;8;6;7;6;9;11;7;9;7;11;10;9;8;7;9;8;7;6;11;7;11;10;8;7;6;11;7;10;8;6;11;10;9;8;7;6;11;10;8;7;6;11;10;9;8;7;6;11;10;9;8;6;11;10;8;7;6;11;0;9;8;7;6;1;10;1;5;2;5;1;4;2;1;0;11;7;11;7;6;11;8;7;6;11;10;9;8;6;8;7;6;7;11;0;9;8;7;6;10;8;6;10;8;6;8;10;8;6;10;8;10;8;10;6;10;6;11;8;6;7;11;7;11;10;8;7;6;10;8;7;6;11;10;7;8;7;6;7;11;10;9;8;6;7;11;10;9;8;7;6;1;2;1;6;5;4;3;2;1;0
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
	else if (skip.eq.6) then
		;;other 30 yr sims start from here
		load "/global/cfs/cdirs/e3sm/xie7/ccmi/ccmi_cmor/e3sm_to_cmip/xjb_use/interp_and_zonal/2process7.ncl"
	else if (skip.eq.7) then
		load "/global/cfs/cdirs/e3sm/xie7/ccmi/ccmi_cmor/e3sm_to_cmip/xjb_use/interp_and_zonal/2process8.ncl"
	else if (skip.eq.8) then
		load "/global/cfs/cdirs/e3sm/xie7/ccmi/ccmi_cmor/e3sm_to_cmip/xjb_use/interp_and_zonal/2process9.ncl"
	else if (skip.eq.9) then
		load "/global/cfs/cdirs/e3sm/xie7/ccmi/ccmi_cmor/e3sm_to_cmip/xjb_use/interp_and_zonal/2process10.ncl"
	else if (skip.eq.10) then
		load "/global/cfs/cdirs/e3sm/xie7/ccmi/ccmi_cmor/e3sm_to_cmip/xjb_use/interp_and_zonal/2process11.ncl"
	else if (skip.eq.11) then
		load "/global/cfs/cdirs/e3sm/xie7/ccmi/ccmi_cmor/e3sm_to_cmip/xjb_use/interp_and_zonal/2process12.ncl"
	end if
	end if
	end if	
	end if
	end if
	end if
	end if
	end if
	end if
	end if
	end if
	end if
;;
end 
