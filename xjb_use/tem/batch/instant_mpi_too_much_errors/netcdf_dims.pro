pro netcdf_dims,flag,nfile,nvar,dat

if flag eq -1 then begin
print,'pro netcdf_dims,flag,nfile,nvar,dat'
print,'Input: flag=0. Reads a dimension size from netcdf file = nfile.'
print,'Input strings: nfile=name of file, nvar=name of dimension.'
return
endif
;
file_id=ncdf_open(nfile)
dim_id=ncdf_dimid(file_id,nvar)
ncdf_diminq,file_id,dim_id,nvar,dat
ncdf_close,file_id
return
end
