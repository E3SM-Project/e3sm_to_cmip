pro netcdf_global,flag,nfile,nvar,dat

if flag eq -1 then begin
print,'pro netcdf_global,flag,nfile,nvar,dat'
print,'Input: flag=0. Reads a global attribute from netcdf file = nfile.'
print,'Input strings: nfile=name of file, nvar=name of variable.'
return
endif

file_id=ncdf_open(nfile)
;var_id=ncdf_varid(file_id,nvar)
;ncdf_varget,file_id,var_id,dat

ncdf_attget,file_id,/GLOBAL,nvar,dat
ncdf_close,file_id

return
end