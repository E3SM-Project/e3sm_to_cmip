pro netcdf,flag,nfile,nvar,dat

if flag eq -1 then begin
print,'pro netcdf,flag,nfile,nvar,dat'
print,'Input: flag=0. Reads data from netcdf file = nfile.'
print,'Input strings: nfile=name of file, nvar=name of variable.'
return
endif

file_id=ncdf_open(nfile)
var_id=ncdf_varid(file_id,nvar)
ncdf_varget,file_id,var_id,dat
ncdf_close,file_id
return
end
